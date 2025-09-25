using CsvHelper;
using System.Globalization;
using System.IO;
using System.Collections.Generic;
using System.Linq;
namespace ETL.Infrastructure.Services
{
    public class CsvReaderService<T>
    {
        private readonly string _filePath;
        public CsvReaderService(string filePath)
        {
            _filePath = filePath;
        }
        public List<T> ReadRecords()
        {
            if (!File.Exists(_filePath))
            {
                Console.WriteLine($"File not found: {_filePath}");
                return new List<T>();
            }
            using var reader = new StreamReader(_filePath);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

            var records = csv.GetRecords<T>().ToList();
            return records;
        }
    }
}