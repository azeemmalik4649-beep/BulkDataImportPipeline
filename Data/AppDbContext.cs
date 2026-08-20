using Microsoft.EntityFrameworkCore;
using BulkDataImportPipeline.Models;

namespace BulkDataImportPipeline.Data
{
    public class AppDbContext : DbContext
    {
        public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
        {
        }

        public DbSet<Customer> Customers { get; set; }
        public DbSet<ImportJob> ImportJobs { get; set; }
    }
}