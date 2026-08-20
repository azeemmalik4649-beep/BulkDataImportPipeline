using BulkDataImportPipeline.Data;
using BulkDataImportPipeline.Models;
using BulkDataImportPipeline.Utilities;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System.Data;
using System.Diagnostics;
using System.Threading.Channels;

namespace BulkDataImportPipeline.Services
{
    public class ValidatedBulkCopyImportService
    {
        private readonly AppDbContext _context;

        public ValidatedBulkCopyImportService(AppDbContext context)
        {
            _context = context;
        }

        public async Task<ImportResult> ImportCsvValidatedAsync(string filePath, string errorFolderPath)
        {
            var stopwatch = Stopwatch.StartNew();
            string? connectionString = _context.Database.GetConnectionString();

            // Staging table saaf karo
            await using (var clearConn = new SqlConnection(connectionString))
            {
                await clearConn.OpenAsync();
                await using var clearCmd = new SqlCommand("TRUNCATE TABLE CustomersStaging", clearConn);
                await clearCmd.ExecuteNonQueryAsync();
            }

            var channel = Channel.CreateBounded<Customer>(new BoundedChannelOptions(2000)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

            // Error rows collect karne ke liye thread-safe list
            var errorMessages = new System.Collections.Concurrent.ConcurrentBag<string>();

            // ---- PRODUCER: parhta hai, validate karta hai, sirf VALID rows channel mein daalta hai ----
            var producerTask = Task.Run(async () =>
            {
                using var reader = new StreamReader(filePath);
                string? headerLine = await reader.ReadLineAsync();

                string? line;
                int lineNumber = 1; // header line 1 thi
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    lineNumber++;

                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    var validationResult = CustomerRowValidator.ValidateAndParse(line, lineNumber);

                    if (!validationResult.IsValid)
                    {
                        errorMessages.Add(validationResult.ErrorReason!);
                        continue; // is row ko skip karo, channel mein mat bhejo
                    }

                    await channel.Writer.WriteAsync(validationResult.Customer!);
                }

                channel.Writer.Complete();
            });

            // ---- CONSUMER: SqlBulkCopy se staging table mein daalta hai ----
            int rowsInserted = 0;
            const int batchSize = 5000;
            var table = CreateStagingDataTable();

            await using var bulkConn = new SqlConnection(connectionString);
            await bulkConn.OpenAsync();

            await foreach (var customer in channel.Reader.ReadAllAsync())
            {
                AddRowToTable(table, customer);

                if (table.Rows.Count >= batchSize)
                {
                    await BulkCopyBatch(bulkConn, table);
                    rowsInserted += table.Rows.Count;
                    table.Clear();
                }
            }

            if (table.Rows.Count > 0)
            {
                await BulkCopyBatch(bulkConn, table);
                rowsInserted += table.Rows.Count;
            }

            await producerTask;

            // MERGE staging → final table
            await using (var mergeCmd = new SqlCommand(MergeSql, bulkConn))
            {
                mergeCmd.CommandTimeout = 300;
                await mergeCmd.ExecuteNonQueryAsync();
            }

            // Error report CSV likhna (agar koi errors huyi)
            string? errorFilePath = null;
            if (!errorMessages.IsEmpty)
            {
                Directory.CreateDirectory(errorFolderPath);
                errorFilePath = Path.Combine(errorFolderPath, $"import_errors_{DateTime.UtcNow:yyyyMMdd_HHmmss}.csv");

                await using var errorWriter = new StreamWriter(errorFilePath);
                await errorWriter.WriteLineAsync("ErrorDetail");
                foreach (var errorMsg in errorMessages)
                {
                    // Comma ko safe karne ke liye quotes mein wrap kar dete hain
                    await errorWriter.WriteLineAsync($"\"{errorMsg.Replace("\"", "\"\"")}\"");
                }
            }

            stopwatch.Stop();

            return new ImportResult
            {
                RowsInserted = rowsInserted,
                RowsFailed = errorMessages.Count,
                ElapsedMilliseconds = stopwatch.ElapsedMilliseconds,
                ErrorFilePath = errorFilePath
            };
        }

        private static DataTable CreateStagingDataTable()
        {
            var table = new DataTable();
            table.Columns.Add("FullName", typeof(string));
            table.Columns.Add("Email", typeof(string));
            table.Columns.Add("City", typeof(string));
            table.Columns.Add("Country", typeof(string));
            table.Columns.Add("SignupDate", typeof(DateTime));
            table.Columns.Add("IsActive", typeof(bool));
            return table;
        }

        private static void AddRowToTable(DataTable table, Customer customer)
        {
            var row = table.NewRow();
            row["FullName"] = customer.FullName;
            row["Email"] = customer.Email;
            row["City"] = customer.City;
            row["Country"] = customer.Country;
            row["SignupDate"] = customer.SignupDate;
            row["IsActive"] = customer.IsActive;
            table.Rows.Add(row);
        }

        private static async Task BulkCopyBatch(SqlConnection connection, DataTable table)
        {
            using var bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = "CustomersStaging",
                BatchSize = 5000
            };

            bulkCopy.ColumnMappings.Add("FullName", "FullName");
            bulkCopy.ColumnMappings.Add("Email", "Email");
            bulkCopy.ColumnMappings.Add("City", "City");
            bulkCopy.ColumnMappings.Add("Country", "Country");
            bulkCopy.ColumnMappings.Add("SignupDate", "SignupDate");
            bulkCopy.ColumnMappings.Add("IsActive", "IsActive");

            await bulkCopy.WriteToServerAsync(table);
        }

        private const string MergeSql = @"
MERGE INTO Customers AS Target
USING CustomersStaging AS Source
ON Target.Email = Source.Email
WHEN MATCHED THEN
    UPDATE SET
        Target.FullName = Source.FullName,
        Target.City = Source.City,
        Target.Country = Source.Country,
        Target.SignupDate = Source.SignupDate,
        Target.IsActive = Source.IsActive
WHEN NOT MATCHED THEN
    INSERT (FullName, Email, City, Country, SignupDate, IsActive)
    VALUES (Source.FullName, Source.Email, Source.City, Source.Country, Source.SignupDate, Source.IsActive);
";
    }
}