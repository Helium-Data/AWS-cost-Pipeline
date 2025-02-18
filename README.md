# AWS Cost Data Pipeline


### Overview
This project automates the extraction, processing, and loading of AWS cost and usage reports from Amazon S3 into Google BigQuery. The pipeline:

- Extracts AWS billing data from a compressed file in an S3 bucket.
- Processes & aggregates the cost data.
- Loads the transformed data into Google BigQuery for further analysis.


### Project Components
1. Data Extraction from AWS S3
- Uses boto3 to connect to AWS S3.
- Identifies and extracts the latest billing report (in .zip format).
- Reads CSV files inside the ZIP and selects key billing-related columns.

2. Data Transformation & Aggregation
- Renames columns for consistency.
- Aggregates cost data by product and billing period.

3. Data Loading into Google BigQuery
- Deletes existing records for the report date to prevent duplication.
- Loads the processed data into BigQuery using google.cloud.bigquery.


## File Structure
```
|-- config.py                   # Stores AWS & GCP credentials
|-- aws_cost.py                     # Main Python script
|-- heliumhealth-1ce77f433fc7.json  # GCP BigQuery service account key
|-- README.md                       # Project documentation
```

## Setup & Configuration
1. Install Required Packages
Ensure you have the required Python libraries installed:
```
pip install boto3 pandas google-cloud-bigquery
```

2. Configure AWS & GCP Credentials

AWS Credentials (config.py)
Store AWS credentials securely in a config.py file:
```
aws_access_key_id     = "YOUR_AWS_ACCESS_KEY"
aws_secret_access_key = "YOUR_AWS_SECRET_KEY"
bucket_name           = "your-s3-bucket-name"
```

## Function Details
get_date_range()
- Calculates the first day of the current month and the first day of the next month.
- Generates a date range string for AWS cost report retrieval.

read_data_from_s3()
- Connects to AWS S3 using boto3.
- Searches for compressed (.zip) files matching the billing period.
- Extracts and loads CSV data into a Pandas DataFrame.

aggregate_data()
- Groups data by product and billing period.
- Summarizes blended cost per product.

load_to_bigquery()
- Deletes existing records for the report date.
- Loads new data into BigQuery.
