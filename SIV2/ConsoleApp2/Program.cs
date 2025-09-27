using ETL.Domain.Entities;

using System.Diagnostics;
using ETL.Infraestructura.Services.Readers;
using ETL.Infraestructura.Services.Loader;
using Order = ETL.Domain.Entities.Order;
class Program
{
    static void Main()
    {
        string basePath = @"C:\Users\Lenovo\Documents\Proyecto Big Data\Archivo CSV Análisis de Ventas-20250923";
        string connectionString = "Server=DESKTOP-NKLD521;Database=sav;Trusted_Connection=True;TrustServerCertificate=True;";


        var stopwatch = Stopwatch.StartNew();
        Console.WriteLine("--- Extract CSVs ---");
        var customers = new CsvReaderService<Customer>(Path.Combine(basePath, "customers.csv")).ReadRecords();
        var products = new CsvReaderService<Product>(Path.Combine(basePath, "products.csv")).ReadRecords();
        var orders = new CsvReaderService<Order>(Path.Combine(basePath, "orders.csv")).ReadRecords();
        var orderDetails = new CsvReaderService<OrderDetail>(Path.Combine(basePath, "order_details.csv")).ReadRecords();

        
        Console.WriteLine($"Customers: {customers.Count}, \nProducts: {products.Count}, \nOrders: {orders.Count},\nOrderDetails: {orderDetails.Count}");

        
      

        Console.WriteLine("------------ Load to Database -------------");
        var loader = new DatabaseLoaderService(connectionString);

        loader.BulkLoadData(customers, products, orders, orderDetails);
        stopwatch.Stop();
        Console.WriteLine($"ETL completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds.");
    }
}