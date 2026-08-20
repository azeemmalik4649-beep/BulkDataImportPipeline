namespace BulkDataImportPipeline.Models
{
    public class ImportJob
    {
        public int ImportJobId { get; set; }
        public string FileName { get; set; } = string.Empty;
        public string FileHash { get; set; } = string.Empty;
        public string Status { get; set; } = string.Empty;
        public int? TotalRowsInFile { get; set; }
        public int RowsProcessed { get; set; }
        public int RowsFailed { get; set; }
        public int LastCheckpointLine { get; set; }
        public DateTime StartedAtUtc { get; set; }
        public DateTime? CompletedAtUtc { get; set; }
    }
}