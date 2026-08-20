using Microsoft.AspNetCore.Mvc;
using BulkDataImportPipeline.Utilities;

namespace BulkDataImportPipeline.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class DataGeneratorController : ControllerBase
    {
        [HttpPost("generate")]
        public IActionResult GenerateCsv([FromQuery] int rowCount = 1000)
        {
            string folderPath = Path.Combine(Directory.GetCurrentDirectory(), "GeneratedFiles");
            Directory.CreateDirectory(folderPath); // agar folder nahi hai to bana do

            string filePath = Path.Combine(folderPath, $"customers_{rowCount}.csv");

            CsvDataGenerator.GenerateCsvFile(filePath, rowCount);

            return Ok(new
            {
                Message = "CSV generated successfully",
                FilePath = filePath,
                RowCount = rowCount
            });
        }
    }
}