using System.Diagnostics;
using System.Globalization;
using BulkDataImportPipeline.Data;
using BulkDataImportPipeline.Models;

namespace BulkDataImportPipeline.Services
{
    public class SlowImportService
    {
        private readonly AppDbContext _context;

        public SlowImportService(AppDbContext context)
        {
            _context = context;
        }

        public async Task<ImportResult> ImportCsvSlowAsync(string filePath)
        {
            var stopwatch = Stopwatch.StartNew();
            int rowsInserted = 0;

            using var reader = new StreamReader(filePath);

            // Pehli line header hai, usay skip karo
            string? headerLine = await reader.ReadLineAsync();

            string? line;
            while ((line = await reader.ReadLineAsync()) != null)
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

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

                // BAD PRACTICE (jaan-boojh kar): har row ke baad SaveChanges call kar rahe hain
                _context.Customers.Add(customer);
                await _context.SaveChangesAsync();

                rowsInserted++;
            }

            stopwatch.Stop();

            return new ImportResult
            {
                RowsInserted = rowsInserted,
                ElapsedMilliseconds = stopwatch.ElapsedMilliseconds
            };
        }
    }

    public class ImportResult
    {
        public int RowsInserted { get; set; }
        public int RowsFailed { get; set; }
        public long ElapsedMilliseconds { get; set; }
        public string? ErrorFilePath { get; set; }
    }
}