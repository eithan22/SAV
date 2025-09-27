using CsvHelper;
using System.Globalization;

namespace ETL.Infraestructura.Services.Readers
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

            try
            {
                if (File.Exists(_filePath))
                {
                    using (var reader = new StreamReader(_filePath))

                    using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
                    {
                        var records = csv.GetRecords<T>().ToList();
                        return records;
                    }
                }
                else
                {
                    Console.WriteLine($"File not found: {_filePath}");
                    return new List<T>();
                }
            }

            catch (Exception ex)
            {
                Console.WriteLine($"Error reading CSV file {_filePath}: {ex.Message}");
                return new List<T>();

            }
        }
              
        }
    }









