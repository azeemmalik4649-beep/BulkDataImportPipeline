using Microsoft.AspNetCore.Mvc;
using BulkDataImportPipeline.Services;

namespace BulkDataImportPipeline.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ImportController : ControllerBase
    {
        private readonly SlowImportService _slowImportService;
        private readonly ChannelImportService _channelImportService;
        private readonly BulkCopyImportService _bulkCopyImportService;
        private readonly ValidatedBulkCopyImportService _validatedImportService;

        public ImportController(
            SlowImportService slowImportService,
            ChannelImportService channelImportService,
            BulkCopyImportService bulkCopyImportService,
            ValidatedBulkCopyImportService validatedImportService)
        {
            _slowImportService = slowImportService;
            _channelImportService = channelImportService;
            _bulkCopyImportService = bulkCopyImportService;
            _validatedImportService = validatedImportService;
        }

        [HttpPost("slow")]
        public async Task<IActionResult> ImportSlow([FromQuery] string fileName)
        {
            string filePath = Path.Combine(Directory.GetCurrentDirectory(), "GeneratedFiles", fileName);
            if (!System.IO.File.Exists(filePath))
                return NotFound(new { Message = $"File not found: {filePath}" });

            var result = await _slowImportService.ImportCsvSlowAsync(filePath);
            return Ok(new { Message = "Slow import completed", result.RowsInserted, result.ElapsedMilliseconds, ElapsedSeconds = result.ElapsedMilliseconds / 1000.0 });
        }

        [HttpPost("channel")]
        public async Task<IActionResult> ImportChannel([FromQuery] string fileName)
        {
            string filePath = Path.Combine(Directory.GetCurrentDirectory(), "GeneratedFiles", fileName);
            if (!System.IO.File.Exists(filePath))
                return NotFound(new { Message = $"File not found: {filePath}" });

            var result = await _channelImportService.ImportCsvWithChannelAsync(filePath);
            return Ok(new { Message = "Channel-based import completed", result.RowsInserted, result.ElapsedMilliseconds, ElapsedSeconds = result.ElapsedMilliseconds / 1000.0 });
        }

        [HttpPost("bulkcopy")]
        public async Task<IActionResult> ImportBulkCopy([FromQuery] string fileName)
        {
            string filePath = Path.Combine(Directory.GetCurrentDirectory(), "GeneratedFiles", fileName);
            if (!System.IO.File.Exists(filePath))
                return NotFound(new { Message = $"File not found: {filePath}" });

            var result = await _bulkCopyImportService.ImportCsvWithBulkCopyAsync(filePath);
            return Ok(new { Message = "SqlBulkCopy + MERGE import completed", result.RowsInserted, result.ElapsedMilliseconds, ElapsedSeconds = result.ElapsedMilliseconds / 1000.0 });
        }


        [HttpPost("validated")]
        public async Task<IActionResult> ImportValidated([FromQuery] string fileName)
        {
            string filePath = Path.Combine(Directory.GetCurrentDirectory(), "GeneratedFiles", fileName);
            if (!System.IO.File.Exists(filePath))
                return NotFound(new { Message = $"File not found: {filePath}" });

            string errorFolderPath = Path.Combine(Directory.GetCurrentDirectory(), "ErrorReports");

            var result = await _validatedImportService.ImportCsvValidatedAsync(filePath, errorFolderPath);

            return Ok(new
            {
                Message = "Validated import completed",
                result.RowsInserted,
                result.RowsFailed,
                result.ElapsedMilliseconds,
                ElapsedSeconds = result.ElapsedMilliseconds / 1000.0,
                result.ErrorFilePath
            });
        }

        [HttpGet("download-errors")]
        public IActionResult DownloadErrorFile([FromQuery] string filePath)
        {
            if (!System.IO.File.Exists(filePath))
                return NotFound(new { Message = "Error file not found" });

            byte[] fileBytes = System.IO.File.ReadAllBytes(filePath);
            string fileName = Path.GetFileName(filePath);

            return File(fileBytes, "text/csv", fileName);
        }

    }
}