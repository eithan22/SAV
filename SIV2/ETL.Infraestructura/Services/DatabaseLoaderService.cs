using ETL.Domain.Entities;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;

namespace ETL.Infrastructure.Services
{
    public class DatabaseLoaderService
    {
        private readonly string _connectionString;
        public DatabaseLoaderService(string connectionString) => _connectionString = connectionString;

        public void BulkLoadData(
            List<Customer> customers,
            List<Product> products,
            List<Order> orders,
            List<OrderDetail> orderDetails)
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            Console.WriteLine("Database connection established.");

            BulkInsertCustomers(customers, connection);
            BulkInsertProducts(products, connection);
            BulkInsertOrders(orders, connection);
            BulkInsertOrderDetails(orderDetails, connection);

            Console.WriteLine("Data loaded successfully.");
        }

        // --- Helper mejorado ---
        private object GetSafeValue(string value, int maxLength = int.MaxValue)
        {
            if (string.IsNullOrEmpty(value))
                return DBNull.Value;

            if (value.Length > maxLength)
                return value.Substring(0, maxLength);

            return value;
        }

        // --- Bulk Inserts ---
        private void BulkInsertCustomers(List<Customer> customers, SqlConnection connection)
        {
            var dt = new DataTable();
            dt.Columns.Add("CustomerID", typeof(int));
            dt.Columns.Add("FirstName", typeof(string));
            dt.Columns.Add("LastName", typeof(string));
            dt.Columns.Add("Email", typeof(string));
            dt.Columns.Add("Phone", typeof(string));
            dt.Columns.Add("City", typeof(string));
            dt.Columns.Add("Country", typeof(string));

            foreach (var c in customers)
            {
                dt.Rows.Add(
                    c.CustomerID,
                    GetSafeValue(c.FirstName, 100),
                    GetSafeValue(c.LastName, 100),
                    GetSafeValue(c.Email, 150),
                    GetSafeValue(c.Phone, 50),
                    GetSafeValue(c.City, 100),
                    GetSafeValue(c.Country) // Sin límite para NVARCHAR(MAX)
                );
            }

            using var bulk = new SqlBulkCopy(connection)
            {
                DestinationTableName = "Customers",
                BulkCopyTimeout = 300 // 5 minutos timeout
            };

            // Mapeo explícito de columnas
            bulk.ColumnMappings.Add("CustomerID", "CustomerID");
            bulk.ColumnMappings.Add("FirstName", "FirstName");
            bulk.ColumnMappings.Add("LastName", "LastName");
            bulk.ColumnMappings.Add("Email", "Email");
            bulk.ColumnMappings.Add("Phone", "Phone");
            bulk.ColumnMappings.Add("City", "City");
            bulk.ColumnMappings.Add("Country", "Country");

            bulk.WriteToServer(dt);
            Console.WriteLine($"Inserted {customers.Count} customers.");
        }

        private void BulkInsertProducts(List<Product> products, SqlConnection connection)
        {
            var dt = new DataTable();
            dt.Columns.Add("ProductID", typeof(int));
            dt.Columns.Add("ProductName", typeof(string));
            dt.Columns.Add("Category", typeof(string));
            dt.Columns.Add("Price", typeof(decimal));
            dt.Columns.Add("Stock", typeof(int));

            foreach (var p in products)
            {
                dt.Rows.Add(
                    p.ProductID,
                    GetSafeValue(p.ProductName, 200),
                    GetSafeValue(p.Category, 100),
                    p.Price,
                    p.Stock
                );
            }

            using var bulk = new SqlBulkCopy(connection)
            {
                DestinationTableName = "Products",
                BulkCopyTimeout = 300
            };

            bulk.ColumnMappings.Add("ProductID", "ProductID");
            bulk.ColumnMappings.Add("ProductName", "ProductName");
            bulk.ColumnMappings.Add("Category", "Category");
            bulk.ColumnMappings.Add("Price", "Price");
            bulk.ColumnMappings.Add("Stock", "Stock");

            bulk.WriteToServer(dt);
            Console.WriteLine($"Inserted {products.Count} products.");
        }

        private void BulkInsertOrders(List<Order> orders, SqlConnection connection)
        {
            var dt = new DataTable();
            dt.Columns.Add("OrderID", typeof(int));
            dt.Columns.Add("CustomerID", typeof(int));
            dt.Columns.Add("OrderDate", typeof(DateTime));
            dt.Columns.Add("Status", typeof(string));

            foreach (var o in orders)
            {
                dt.Rows.Add(
                    o.OrderID,
                    o.CustomerID,
                    o.OrderDate,
                    GetSafeValue(o.Status, 50)
                );
            }

            using var bulk = new SqlBulkCopy(connection)
            {
                DestinationTableName = "Orders",
                BulkCopyTimeout = 300
            };

            bulk.ColumnMappings.Add("OrderID", "OrderID");
            bulk.ColumnMappings.Add("CustomerID", "CustomerID");
            bulk.ColumnMappings.Add("OrderDate", "OrderDate");
            bulk.ColumnMappings.Add("Status", "Status");

            bulk.WriteToServer(dt);
            Console.WriteLine($"Inserted {orders.Count} orders.");
        }

        private void BulkInsertOrderDetails(List<OrderDetail> details, SqlConnection connection)
        {
            var dt = new DataTable();
            dt.Columns.Add("OrderID", typeof(int));
            dt.Columns.Add("ProductID", typeof(int));
            dt.Columns.Add("Quantity", typeof(int));
            dt.Columns.Add("TotalPrice", typeof(decimal));

            foreach (var d in details)
            {
                dt.Rows.Add(
                    d.OrderID,
                    d.ProductID,
                    d.Quantity,
                    d.TotalPrice
                );
            }

            using var bulk = new SqlBulkCopy(connection)
            {
                DestinationTableName = "OrderDetails",
                BulkCopyTimeout = 300
            };

            bulk.ColumnMappings.Add("OrderID", "OrderID");
            bulk.ColumnMappings.Add("ProductID", "ProductID");
            bulk.ColumnMappings.Add("Quantity", "Quantity");
            bulk.ColumnMappings.Add("TotalPrice", "TotalPrice");

            bulk.WriteToServer(dt);
            Console.WriteLine($"Inserted {details.Count} order details.");
        }
    }
}