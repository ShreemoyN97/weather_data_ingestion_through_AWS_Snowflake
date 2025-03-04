from datetime import datetime
import pandas as pd
import boto3
from io import StringIO

def handle_insert(record):
    """
    Processes an INSERT event from DynamoDB Stream and converts it into a DataFrame.

    Args:
        record (dict): The DynamoDB event record containing 'NewImage' data.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the inserted record.
    """
    print("Handling Insert: ", record)
    record_dict = {}  # Dictionary to store column names and values

    # Extract data from the 'NewImage' field
    for key, value in record['dynamodb']['NewImage'].items():
        for data_type, col_value in value.items():
            record_dict.update({key: col_value})

    # Convert dictionary to DataFrame
    df_record = pd.DataFrame([record_dict])
    return df_record


def lambda_handler(event, context):
    """
    AWS Lambda function to process DynamoDB Stream records and store them as CSV in S3.

    Args:
        event (dict): The event data passed to the function by DynamoDB Stream.
        context (object): AWS Lambda context object.

    Returns:
        None
    """
    print(event)
    df = pd.DataFrame()  # Initialize an empty DataFrame

    # Process each record in the event
    for record in event['Records']:
        table = record['eventSourceARN'].split("/")[1]  # Extract table name from ARN

        if record['eventName'] == "INSERT": 
            dff = handle_insert(record)  # Process INSERT events
            df = dff  # Assign processed data to DataFrame

    # If DataFrame is not empty, save to S3
    if not df.empty:
        all_columns = list(df)
        df[all_columns] = df[all_columns].astype(str)  # Convert all columns to string type

        # Generate a unique file name using timestamp
        file_name = f"{table}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"

        print(event)  # Print event for debugging

        # Convert DataFrame to CSV format
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Initialize S3 client
        s3 = boto3.client('s3')
        bucket_name = "<S3_Bucket_Name>"  # Replace with actual S3 bucket name
        key = f"<S3_Bucket_Folder_Name>/{file_name}"  # S3 folder structure. Replace <S3_Bucket_Folder_Name> with actual folder name

        print(key)  # Print S3 key for debugging

        # Upload the CSV file to S3
        s3.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())

    print(f'Successfully processed {len(event["Records"])} records.')
