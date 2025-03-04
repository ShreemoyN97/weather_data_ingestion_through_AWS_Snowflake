# Weather Data Pipeline

## Overview
This project automates the process of fetching, storing, and analyzing weather data. The workflow involves multiple AWS services, Snowflake, and an external weather API.

## Workflow
1. **Fetching Weather Data**
   - An AWS Lambda function (**Fetch_Weather_Data**) is triggered every hour by an **EventBridge** rule.
   - It fetches real-time weather data from [WeatherAPI](https://www.weatherapi.com) for multiple cities.
   - The retrieved data is stored in **DynamoDB**.

2. **Streaming Data to S3**
   - A second AWS Lambda function (**DDBStream2S3**) is triggered by **DynamoDB Streams** when new records are inserted.
   - It processes the new records and converts them into a CSV format.
   - The CSV file is uploaded to **Amazon S3**.

3. **Triggering Snowflake Ingestion**
   - Once the data is uploaded to S3, an **SQS notification** is generated.
   - Snowflake listens to this SQS notification and triggers **Snowpipe** to ingest data into a Snowflake table.
   - The data is stored and can be queried from Snowflake for further analysis.

---

## Required AWS Permissions
### **Lambda Function: Fetch_Weather_Data**
This function requires the following AWS IAM permissions:
- `dynamodb:PutItem` – To insert records into DynamoDB
- `dynamodb:DescribeTable` – To verify DynamoDB table existence
- `logs:CreateLogGroup` – To create CloudWatch log groups
- `logs:CreateLogStream` – To create CloudWatch log streams
- `logs:PutLogEvents` – To log execution details

### **Lambda Function: DDBStream2S3**
This function requires:
- `dynamodb:DescribeStream` – To access the DynamoDB stream
- `dynamodb:GetRecords` – To read records from the DynamoDB stream
- `s3:PutObject` – To upload CSV files to S3
- `s3:GetObject` – To read files (for debugging)
- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents` – For CloudWatch logging

### **S3 Bucket Permissions**
The S3 bucket storing CSV files requires:
- `s3:PutObject` – To allow Lambda to upload files
- `s3:GetObject` – To allow Snowflake to read files

### **SQS Notification Permissions**
The SQS queue for Snowflake ingestion requires:
- `sqs:SendMessage` – To allow S3 to send notifications
- `sqs:ReceiveMessage` – To allow Snowflake to process messages

### **Snowflake Permissions**
The Snowflake integration requires:
- An **external stage** to read files from S3
- A **Snowpipe** configured to auto-ingest data upon receiving an SQS notification
- `STORAGE_INTEGRATION` permissions set up with the IAM role for Snowflake to access S3

---

## Setup Instructions
### Step 1: Deploy AWS Lambda Functions
- Upload the Python scripts (`Fetch_Weather_Data.py` and `DDBStream2S3.py`) to AWS Lambda.
- Set appropriate environment variables (DynamoDB table name, S3 bucket name, etc.).
- Attach the required IAM roles to Lambda functions.

### Step 2: Configure DynamoDB
- Create a DynamoDB table named `weather_data` with a primary key `city` and a sort key `time`.
- Enable **DynamoDB Streams** and set it to trigger `DDBStream2S3`.

### Step 3: Set Up S3 and SQS
- Create an **S3 bucket** to store weather data CSV files.
- Configure **S3 Event Notifications** to send messages to an **SQS queue**.

### Step 4: Set Up Snowflake Integration
- Create a **STORAGE INTEGRATION** in Snowflake to access S3.
- Create an **external stage** pointing to the S3 bucket.
- Configure **Snowpipe** to listen for SQS messages and automatically ingest data into the Snowflake table.

---

## Testing
- Manually trigger `Fetch_Weather_Data` to populate DynamoDB.
- Verify that DynamoDB Streams trigger `DDBStream2S3` and that CSV files appear in S3.
- Check if Snowflake ingests data automatically when new CSVs are uploaded to S3.

## Monitoring & Logging
- Use **AWS CloudWatch** for Lambda logs and execution insights.
- Check **S3 logs** to confirm successful file uploads.
- Monitor **Snowflake query logs** for ingestion issues.

---

## Contributors
- **Shreemoy Nanda**

For any questions or contributions, feel free to open an issue or submit a pull request!

