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

        public ImportController(
            SlowImportService slowImportService,
            ChannelImportService channelImportService,
            BulkCopyImportService bulkCopyImportService)
        {
            _slowImportService = slowImportService;
            _channelImportService = channelImportService;
            _bulkCopyImportService = bulkCopyImportService;
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
    }
}