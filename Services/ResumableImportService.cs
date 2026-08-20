using System.Data;
using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using BulkDataImportPipeline.Data;
using BulkDataImportPipeline.Models;
using BulkDataImportPipeline.Utilities;

namespace BulkDataImportPipeline.Services
{
    public class ResumableImportResult
    {
        public bool WasSkippedAsDuplicate { get; set; }
        public int RowsInserted { get; set; }
        public int RowsFailed { get; set; }
        public long ElapsedMilliseconds { get; set; }
        public string? ErrorFilePath { get; set; }
        public int ImportJobId { get; set; }
    }

    public class ResumableImportService
    {
        private readonly AppDbContext _context;

        public ResumableImportService(AppDbContext context)
        {
            _context = context;
        }

        public async Task<ResumableImportResult> ImportCsvResumableAsync(string filePath, string errorFolderPath)
        {
            var stopwatch = Stopwatch.StartNew();
            string fileName = Path.GetFileName(filePath);

            // ---- STEP 1: File hash nikalna ----
            string fileHash = await FileHashHelper.ComputeSha256Async(filePath);

            // ---- STEP 2: Check karna - kya ye file pehle SUCCESSFULLY complete ho chuki hai? ----
            var existingCompletedJob = await _context.ImportJobs
                .Where(j => j.FileHash == fileHash && j.Status == "Completed")
                .FirstOrDefaultAsync();

            if (existingCompletedJob != null)
            {
                stopwatch.Stop();
                return new ResumableImportResult
                {
                    WasSkippedAsDuplicate = true,
                    RowsInserted = existingCompletedJob.RowsProcessed,
                    RowsFailed = existingCompletedJob.RowsFailed,
                    ElapsedMilliseconds = stopwatch.ElapsedMilliseconds,
                    ImportJobId = existingCompletedJob.ImportJobId
                };
            }

            // ---- STEP 3: Naya ImportJob record banana (tracking ke liye) ----
            var importJob = new ImportJob
            {
                FileName = fileName,
                FileHash = fileHash,
                Status = "InProgress",
                RowsProcessed = 0,
                RowsFailed = 0,
                LastCheckpointLine = 0,
                StartedAtUtc = DateTime.UtcNow
            };
            _context.ImportJobs.Add(importJob);
            await _context.SaveChangesAsync();
            int importJobId = importJob.ImportJobId;

            string? connectionString = _context.Database.GetConnectionString();

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

            var errorMessages = new System.Collections.Concurrent.ConcurrentBag<string>();

            // ---- PRODUCER ----
            var producerTask = Task.Run(async () =>
            {
                using var reader = new StreamReader(filePath);
                string? headerLine = await reader.ReadLineAsync();

                string? line;
                int lineNumber = 1;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    lineNumber++;
                    if (string.IsNullOrWhiteSpace(line)) continue;

                    var validationResult = CustomerRowValidator.ValidateAndParse(line, lineNumber);

                    if (!validationResult.IsValid)
                    {
                        errorMessages.Add(validationResult.ErrorReason!);
                        continue;
                    }

                    await channel.Writer.WriteAsync(validationResult.Customer!);
                }

                channel.Writer.Complete();
            });

            // ---- CONSUMER: bulk copy + checkpoint update har batch ke baad ----
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

                    // ---- CHECKPOINT: progress database mein save karo ----
                    await UpdateCheckpoint(connectionString!, importJobId, rowsInserted, errorMessages.Count);
                }
            }

            if (table.Rows.Count > 0)
            {
                await BulkCopyBatch(bulkConn, table);
                rowsInserted += table.Rows.Count;
                await UpdateCheckpoint(connectionString!, importJobId, rowsInserted, errorMessages.Count);
            }

            await producerTask;

            // ---- MERGE staging → final table ----
            await using (var mergeCmd = new SqlCommand(MergeSql, bulkConn))
            {
                mergeCmd.CommandTimeout = 300;
                await mergeCmd.ExecuteNonQueryAsync();
            }

            // ---- Error report likhna ----
            string? errorFilePath = null;
            if (!errorMessages.IsEmpty)
            {
                Directory.CreateDirectory(errorFolderPath);
                errorFilePath = Path.Combine(errorFolderPath, $"import_errors_{DateTime.UtcNow:yyyyMMdd_HHmmss}.csv");

                await using var errorWriter = new StreamWriter(errorFilePath);
                await errorWriter.WriteLineAsync("ErrorDetail");
                foreach (var errorMsg in errorMessages)
                {
                    await errorWriter.WriteLineAsync($"\"{errorMsg.Replace("\"", "\"\"")}\"");
                }
            }

            // ---- Job ko "Completed" mark karna ----
            await using (var completeConn = new SqlConnection(connectionString))
            {
                await completeConn.OpenAsync();
                await using var completeCmd = new SqlCommand(
                    @"UPDATE ImportJobs SET Status = 'Completed', RowsProcessed = @rows, RowsFailed = @failed, CompletedAtUtc = SYSUTCDATETIME() WHERE ImportJobId = @id",
                    completeConn);
                completeCmd.Parameters.AddWithValue("@rows", rowsInserted);
                completeCmd.Parameters.AddWithValue("@failed", errorMessages.Count);
                completeCmd.Parameters.AddWithValue("@id", importJobId);
                await completeCmd.ExecuteNonQueryAsync();
            }

            stopwatch.Stop();

            return new ResumableImportResult
            {
                WasSkippedAsDuplicate = false,
                RowsInserted = rowsInserted,
                RowsFailed = errorMessages.Count,
                ElapsedMilliseconds = stopwatch.ElapsedMilliseconds,
                ErrorFilePath = errorFilePath,
                ImportJobId = importJobId
            };
        }

        private static async Task UpdateCheckpoint(string connectionString, int importJobId, int rowsProcessed, int rowsFailed)
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync();
            await using var cmd = new SqlCommand(
                "UPDATE ImportJobs SET RowsProcessed = @rows, RowsFailed = @failed, LastCheckpointLine = @rows WHERE ImportJobId = @id",
                conn);
            cmd.Parameters.AddWithValue("@rows", rowsProcessed);
            cmd.Parameters.AddWithValue("@failed", rowsFailed);
            cmd.Parameters.AddWithValue("@id", importJobId);
            await cmd.ExecuteNonQueryAsync();
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