using System.Diagnostics;
using System.Globalization;
using System.Threading.Channels;
using BulkDataImportPipeline.Data;
using BulkDataImportPipeline.Models;

namespace BulkDataImportPipeline.Services
{
    public class ChannelImportService
    {
        private readonly AppDbContext _context;

        public ChannelImportService(AppDbContext context)
        {
            _context = context;
        }

        public async Task<ImportResult> ImportCsvWithChannelAsync(string filePath)
        {
            var stopwatch = Stopwatch.StartNew();

            // Bounded channel: max 1000 rows queue mein reh sakti hain ek waqt mein
            var channel = Channel.CreateBounded<Customer>(new BoundedChannelOptions(1000)
            {
                FullMode = BoundedChannelFullMode.Wait // channel full ho to producer ruk jayega
            });

            // ---- PRODUCER: CSV parhta hai, channel mein daalta hai ----
            var producerTask = Task.Run(async () =>
            {
                using var reader = new StreamReader(filePath);
                string? headerLine = await reader.ReadLineAsync(); // header skip

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

                    // Agar channel full hai (1000 items pehle se pending), to yahan wait karega
                    await channel.Writer.WriteAsync(customer);
                }

                // Producer ka kaam khatam - consumer ko signal do
                channel.Writer.Complete();
            });

            // ---- CONSUMER: channel se rows uthata hai, batch mein insert karta hai ----
            int rowsInserted = 0;
            const int batchSize = 500;
            var batch = new List<Customer>(batchSize);

            await foreach (var customer in channel.Reader.ReadAllAsync())
            {
                batch.Add(customer);

                if (batch.Count >= batchSize)
                {
                    _context.Customers.AddRange(batch);
                    await _context.SaveChangesAsync();
                    _context.ChangeTracker.Clear(); // tracked objects clear, memory/slowness control
                    rowsInserted += batch.Count;
                    batch.Clear();
                }
            }

            // Leftover rows jo batchSize se kam thin
            if (batch.Count > 0)
            {
                _context.Customers.AddRange(batch);
                await _context.SaveChangesAsync();
                rowsInserted += batch.Count;
            }

            await producerTask; // confirm producer bina error khatam hua

            stopwatch.Stop();

            return new ImportResult
            {
                RowsInserted = rowsInserted,
                ElapsedMilliseconds = stopwatch.ElapsedMilliseconds
            };
        }
    }
}