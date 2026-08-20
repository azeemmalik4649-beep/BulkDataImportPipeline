using Microsoft.AspNetCore.Mvc;
using BulkDataImportPipeline.Services;

namespace BulkDataImportPipeline.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ImportController : ControllerBase
    {
        private readonly SlowImportService _slowImportService;

        public ImportController(SlowImportService slowImportService)
        {
            _slowImportService = slowImportService;
        }

        [HttpPost("slow")]
        public async Task<IActionResult> ImportSlow([FromQuery] string fileName)
        {
            string filePath = Path.Combine(Directory.GetCurrentDirectory(), "GeneratedFiles", fileName);

            if (!System.IO.File.Exists(filePath))
            {
                return NotFound(new { Message = $"File not found: {filePath}" });
            }

            var result = await _slowImportService.ImportCsvSlowAsync(filePath);

            return Ok(new
            {
                Message = "Slow import completed",
                result.RowsInserted,
                result.ElapsedMilliseconds,
                ElapsedSeconds = result.ElapsedMilliseconds / 1000.0
            });
        }
    }
}