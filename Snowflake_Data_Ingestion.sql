-- Create a new database for the project
CREATE DATABASE DE_PROJECT;

-- Switch to the newly created database
USE DATABASE DE_PROJECT;

-- Create a table to store weather data from CSV files
CREATE OR REPLACE TABLE weather_data(
    temp           NUMBER(20,0),         -- Temperature in Celsius
    CITY          VARCHAR(128),          -- City name
    humidity       NUMBER(20,5),         -- Humidity percentage
    wind_speed     NUMBER(20,5),         -- Wind speed in mph
    time           VARCHAR(128),         -- Timestamp of the recorded data
    wind_dir       VARCHAR(128),         -- Wind direction
    pressure_mb    NUMBER(20,5)          -- Atmospheric pressure in millibars
);

-- Create an integration object for external storage access
CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = external_stage
  STORAGE_PROVIDER = s3
  ENABLED = true
  STORAGE_AWS_ROLE_ARN = '<AWS_ROLE_ARN>'  -- Replace with the actual AWS IAM Role ARN
  STORAGE_ALLOWED_LOCATIONS = ('<S3_BUCKET_PATH>');  -- Replace with the actual S3 bucket path

-- Describe the integration object to retrieve external ID for AWS role setup
DESC INTEGRATION s3_int;

-- Create a file format for CSV ingestion
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = csv
  FIELD_DELIMITER = ','       -- Use a comma as the delimiter
  SKIP_HEADER = 1             -- Skip the header row in CSV files
  NULL_IF = ('NULL', 'null')  -- Treat NULL and null as NULL values
  EMPTY_FIELD_AS_NULL = true; -- Convert empty fields to NULL

-- Create an external stage to access CSV files stored in S3
CREATE OR REPLACE STAGE ext_csv_stage
  URL = '<S3_BUCKET_PATH>'  -- Replace with the actual S3 bucket path
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = csv_format;

-- Create a Snowflake pipe to automate data ingestion from S3 to the weather_data table
CREATE OR REPLACE PIPE mypipe AUTO_INGEST = TRUE AS
COPY INTO weather_data
FROM @ext_csv_stage
ON_ERROR = CONTINUE; -- Continue processing even if some records fail

-- Show existing pipes to verify the setup
SHOW PIPES;

-- Query the table to verify the data ingestion
SELECT * FROM weather_data;
