CREATE DATABASE Weather_Data_Project
GO

USE Weather_Data_Project
GO

CREATE TABLE Dim_Location (
    LocationID INT IDENTITY(1,1) PRIMARY KEY,
    CityName VARCHAR(100) NOT NULL,
    Country VARCHAR(100) NOT NULL,
    Latitude FLOAT NOT NULL,
    Longitude FLOAT NOT NULL
);
GO

-- 3. Create Dimension Table: Date & Time
CREATE TABLE Dim_Date (
    DateID INT IDENTITY(1,1) PRIMARY KEY,
    FullDateTime DATETIME UNIQUE NOT NULL,
    Year INT,
    Month INT,
    Day INT,
    Hour INT,
    DayName VARCHAR(20)
);
GO
-- 4. Create Fact Table: Weather Metrics
CREATE TABLE Fact_Weather_Metrics (
    WeatherID INT IDENTITY(1,1) PRIMARY KEY,
    LocationID INT FOREIGN KEY REFERENCES Dim_Location(LocationID),
    DateID INT FOREIGN KEY REFERENCES Dim_Date(DateID),
    Temperature FLOAT,
    Humidity FLOAT,
    WindSpeed FLOAT,
    Precipitation FLOAT,
    Data_Type VARCHAR(50) -- 'Actual / Live' vs 'Forecast'
);
GO

USE Weather_Data_Project;

-- Check if your cities (including Pune & Mumbai) loaded successfully
SELECT * FROM Dim_Location;

-- Check if dates loaded successfully
SELECT * FROM Dim_Date;

-- Check if weather records loaded successfully
SELECT TOP 10 * FROM Fact_Weather_Metrics;