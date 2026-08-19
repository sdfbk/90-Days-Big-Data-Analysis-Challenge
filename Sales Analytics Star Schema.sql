CREATE DATABASE Sales_Analytics;
USE Sales_Analytics;

CREATE TABLE DimDate(
Date_id INT PRIMARY KEY,
Full_date DATE NOT NULL,
Day_Of_Week VARCHAR(9),
Day_number INT,
Month_number INT,
Month_name VARCHAR(9),
Quarter VARCHAR(2),
Year INT,
Is_Weekend TINYINT
);

CREATE TABLE DimTime(
Timestamp_id INT PRIMARY KEY,
Hour TINYINT,
Minute TINYINT,
Second TINYINT,
Time_Of_Day VARCHAR(20)
);

CREATE TABLE DimProduct(
Product_id INT PRIMARY KEY,
Product_sku VARCHAR(10) UNIQUE,
Product_name VARCHAR(50),
Category VARCHAR(50),
SubCategory VARCHAR(50),
Brand VARCHAR(50),
Unit_cost FLOAT
);

CREATE TABLE DimStore(
Store_id INT,
Store_name VARCHAR(50),
City VARCHAR(50),
State VARCHAR(50),
Country VARCHAR(50),
Store_type VARCHAR(50)
);

CREATE TABLE DimCustomer(
CustomerID INT,
Customer_name VARCHAR(50),
Gender VARCHAR(10),
Age_group INT CHECK(Age_group >=18),
City VARCHAR(20),
Loyalty_tier VARCHAR(20)
);

CREATE TABLE FactSales(
SalesID INT PRIMARY KEY,
CustomerID INT,
Product_id INT,
Store_id INT,
Date_id INT,
Timestamp_id INT,
Promotion_id INT,
Quantity_sold INT,
Unit_price FLOAT,
Discount_Amount FLOAT,
Total_Sales_Amount FLOAT,
CONSTRAINT ProdFK FOREIGN KEY(Product_id) REFERENCES DimProduct(Product_id),
CONSTRAINT DateFK FOREIGN KEY(Date_id) REFERENCES DimDate(Date_id),
CONSTRAINT TimestampFK FOREIGN KEY(Timestamp_id) REFERENCES DimTime(Timestamp_id)
);