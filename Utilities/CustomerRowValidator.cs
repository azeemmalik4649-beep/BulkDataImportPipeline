using System.Globalization;
using BulkDataImportPipeline.Models;

namespace BulkDataImportPipeline.Utilities
{
    public class RowValidationResult
    {
        public bool IsValid { get; set; }
        public Customer? Customer { get; set; }
        public string? ErrorReason { get; set; }
    }

    public static class CustomerRowValidator
    {
        public static RowValidationResult ValidateAndParse(string csvLine, int lineNumber)
        {
            var parts = csvLine.Split(',');

            // Check 1: Kya expected column count hai?
            if (parts.Length != 6)
            {
                return Invalid(lineNumber, csvLine, $"Expected 6 columns, found {parts.Length}");
            }

            string fullName = parts[0].Trim();
            string email = parts[1].Trim();
            string city = parts[2].Trim();
            string country = parts[3].Trim();
            string signupDateRaw = parts[4].Trim();
            string isActiveRaw = parts[5].Trim();

            // Check 2: FullName khaali to nahi
            if (string.IsNullOrWhiteSpace(fullName))
            {
                return Invalid(lineNumber, csvLine, "FullName is empty");
            }

            // Check 3: Email khaali ya basic format ghalat to nahi
            if (string.IsNullOrWhiteSpace(email) || !email.Contains('@'))
            {
                return Invalid(lineNumber, csvLine, "Email is missing or invalid format");
            }

            // Check 4: SignupDate parse ho raha hai ya nahi
            if (!DateTime.TryParse(signupDateRaw, CultureInfo.InvariantCulture, DateTimeStyles.None, out var signupDate))
            {
                return Invalid(lineNumber, csvLine, $"Invalid date format: '{signupDateRaw}'");
            }

            // Check 5: IsActive parse ho raha hai ya nahi
            if (!bool.TryParse(isActiveRaw, out var isActive))
            {
                return Invalid(lineNumber, csvLine, $"Invalid boolean value: '{isActiveRaw}'");
            }

            // Sab checks pass - valid row
            return new RowValidationResult
            {
                IsValid = true,
                Customer = new Customer
                {
                    FullName = fullName,
                    Email = email,
                    City = city,
                    Country = country,
                    SignupDate = signupDate,
                    IsActive = isActive
                }
            };
        }

        private static RowValidationResult Invalid(int lineNumber, string rawLine, string reason)
        {
            return new RowValidationResult
            {
                IsValid = false,
                ErrorReason = $"Line {lineNumber}: {reason} | Raw data: {rawLine}"
            };
        }
    }
}