using BulkDataImportPipeline.Data;
using BulkDataImportPipeline.Models;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Threading.Channels;

namespace BulkDataImportPipeline.Services
{
    public class BulkCopyImportService
    {
        private readonly AppDbContext _context;

        public BulkCopyImportService(AppDbContext context)
        {
            _context = context;
        }

        public async Task<ImportResult> ImportCsvWithBulkCopyAsync(string filePath)
        {
            var stopwatch = Stopwatch.StartNew();
            string? connectionString = _context.Database.GetConnectionString();

            // Step A: staging table saaf karo (purana leftover data na ho, resumability ke liye zaroori)
            await using (var clearConn = new SqlConnection(connectionString))
            {
                await clearConn.OpenAsync();
                await using var clearCmd = new SqlCommand("TRUNCATE TABLE CustomersStaging", clearConn);
                await clearCmd.ExecuteNonQueryAsync();
            }

            // Producer/Consumer channel - Step 3 wala hi pattern
            var channel = Channel.CreateBounded<Customer>(new BoundedChannelOptions(2000)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

            var producerTask = Task.Run(async () =>
            {
                try
                {
                    using var reader = new StreamReader(filePath);
                    string? headerLine = await reader.ReadLineAsync();

                    string? line;
                    while ((line = await reader.ReadLineAsync()) != null)
                    {
                        if (string.IsNullOrWhiteSpace(line)) continue;
                        var parts = line.Split(',');

                        var customer = new Customer
                        {
                            FullName = parts[0],
                            Email = parts[1],
                            City = parts[2],
                            Country = parts[3],
                            SignupDate = DateTime.Parse(parts[4], CultureInfo.InvariantCulture),
                            IsActive = bool.Parse(parts[5])
                        };

                        await channel.Writer.WriteAsync(customer);
                    }

                    channel.Writer.Complete();
                }
                catch (Exception ex)
                {
                    channel.Writer.Complete(ex);
                }
            });

            // Consumer: rows ko DataTable mein jama karta hai, batch-wise SqlBulkCopy karta hai
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

            // Leftover rows
            if (table.Rows.Count > 0)
            {
                await BulkCopyBatch(bulkConn, table);
                rowsInserted += table.Rows.Count;
            }

            await producerTask;

            // Step B: Staging table se final Customers table mein MERGE
            await using (var mergeCmd = new SqlCommand(MergeSql, bulkConn))
            {
                mergeCmd.CommandTimeout = 300; // bade dataset ke liye extra time
                await mergeCmd.ExecuteNonQueryAsync();
            }

            stopwatch.Stop();

            return new ImportResult
            {
                RowsInserted = rowsInserted,
                ElapsedMilliseconds = stopwatch.ElapsedMilliseconds
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