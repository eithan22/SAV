using ETL.Domain.Entities;
using ETL.Infrastructure.Services;
using System;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;
class Program
{
    static void Main()
    {
        string basePath = @"C:\Users\Lenovo\Documents\Proyecto Big Data\Archivo CSV Análisis de Ventas-20250923";

        string connectionString = "Server=DESKTOP-NKLD521;Trusted_Connection=True;TrustServerCertificate=True;";

        var stopwatch = Stopwatch.StartNew();
        Console.WriteLine("--- Extract CSVs ---");
        var customers = new CsvReaderService<Customer>(Path.Combine(basePath, "customers.csv")).ReadRecords();
        var products = new CsvReaderService<Product>(Path.Combine(basePath, "products.csv")).ReadRecords();
        var orders = new CsvReaderService<Order>(Path.Combine(basePath, "orders.csv")).ReadRecords();
        var orderDetails = new CsvReaderService<OrderDetail>(Path.Combine(basePath, "order_details.csv")).ReadRecords();


        Console.WriteLine($"Customers: {customers.Count}, Products: {products.Count}, Orders: {orders.Count}, OrderDetails: {orderDetails.Count}");
        Console.WriteLine("--- Transform Data ---");

        // Optional: transformations like calculating TotalPrice if needed

        Console.WriteLine("--- Load to Database ---");
        var loader = new DatabaseLoaderService(connectionString);
        loader.BulkLoadData(customers, products, orders, orderDetails);
        stopwatch.Stop();
        Console.WriteLine($"ETL completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds.");
    }
}