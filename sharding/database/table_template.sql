-- Enable foreign key support
PRAGMA foreign_keys = ON;

-- Drop tables if they already exist
DROP TABLE IF EXISTS Orders;
DROP TABLE IF EXISTS Customers;

-- Create Customers table (No AUTOINCREMENT)
CREATE TABLE Customers (
    customer_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL
);

-- Create Orders table with a foreign key (No AUTOINCREMENT)
CREATE TABLE Orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date TEXT NOT NULL,
    amount REAL NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id) ON DELETE CASCADE
);

-- Insert sample data into Customers table
INSERT INTO Customers (customer_id, name, email) VALUES (1, 'Alice Johnson', 'alice@example.com');
INSERT INTO Customers (customer_id, name, email) VALUES (2, 'Bob Smith', 'bob@example.com');

-- Insert sample data into Orders table
INSERT INTO Orders (order_id, customer_id, order_date, amount) VALUES (1, 1, '2025-02-03', 100.50);
INSERT INTO Orders (order_id, customer_id, order_date, amount) VALUES (2, 2, '2025-02-04', 200.75);
INSERT INTO Orders (order_id, customer_id, order_date, amount) VALUES (3, 1, '2025-02-05', 50.00);
