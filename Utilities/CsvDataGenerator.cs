using System.Text;

namespace BulkDataImportPipeline.Utilities
{
    public static class CsvDataGenerator
    {
        private static readonly string[] FirstNames = { "Ali", "Ahmed", "Sara", "Fatima", "Bilal", "Zainab", "Hassan", "Ayesha", "Usman", "Hira" };
        private static readonly string[] LastNames = { "Khan", "Malik", "Shah", "Butt", "Iqbal", "Raza", "Sheikh", "Chaudhry", "Farooq", "Aslam" };
        private static readonly string[] Cities = { "Lahore", "Karachi", "Islamabad", "Faisalabad", "Multan", "Peshawar", "Quetta", "Rawalpindi" };
        private static readonly string[] Countries = { "Pakistan" };

        public static void GenerateCsvFile(string filePath, int rowCount)
        {
            var random = new Random(42); // fixed seed = same data har baar (testing ke liye consistent)

            using var writer = new StreamWriter(filePath, false, Encoding.UTF8);

            // Header row
            writer.WriteLine("FullName,Email,City,Country,SignupDate,IsActive");

            for (int i = 1; i <= rowCount; i++)
            {
                string firstName = FirstNames[random.Next(FirstNames.Length)];
                string lastName = LastNames[random.Next(LastNames.Length)];
                string fullName = $"{firstName} {lastName}";
                string email = $"{firstName.ToLower()}.{lastName.ToLower()}{i}@example.com";
                string city = Cities[random.Next(Cities.Length)];
                string country = Countries[random.Next(Countries.Length)];
                DateTime signupDate = DateTime.UtcNow.AddDays(-random.Next(0, 1000));
                bool isActive = random.Next(0, 10) > 1; // ~80% active

                writer.WriteLine($"{fullName},{email},{city},{country},{signupDate:yyyy-MM-dd},{isActive}");
            }
        }
    }
}