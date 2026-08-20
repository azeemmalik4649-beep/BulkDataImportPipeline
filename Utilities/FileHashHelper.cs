using System.Security.Cryptography;

namespace BulkDataImportPipeline.Utilities
{
    public static class FileHashHelper
    {
        public static async Task<string> ComputeSha256Async(string filePath)
        {
            using var sha256 = SHA256.Create();
            using var stream = File.OpenRead(filePath);

            byte[] hashBytes = await sha256.ComputeHashAsync(stream);

            // Byte array ko readable hex string mein convert karna (e.g. "a3f5b8...")
            return Convert.ToHexString(hashBytes).ToLowerInvariant();
        }
    }
}