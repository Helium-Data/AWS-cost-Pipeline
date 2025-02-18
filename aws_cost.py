# %%
import boto3
import pandas as pd
import zipfile
import io
import os
# from datetime import datetime, timedelta
import datetime
from google.cloud import bigquery
import config


def get_date_range():
    current_date = datetime.datetime.now()
    first_day_of_month = current_date.replace(day=1)
    first_day_of_next_month = (first_day_of_month.replace(day=1) + datetime.timedelta(days=32)).replace(day=1)
    date_range = f"{first_day_of_month.strftime('%Y%m%d')}-{first_day_of_next_month.strftime('%Y%m%d')}"
    return date_range


def read_data_from_s3(aws_access_key_id, aws_secret_access_key, bucket_name, date_range, report_date):
    s3 = boto3.client('s3', 
                      aws_access_key_id=aws_access_key_id, 
                      aws_secret_access_key=aws_secret_access_key)
    folder_prefix = f'CUR/helium-aws-CUR/{date_range}/'
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
    file_names = [obj['Key'] for obj in response.get('Contents', [])]
    zip_file_names = [file_name for file_name in file_names if file_name.endswith('.zip')]

    def extract_timestamp(file_name):
        timestamp_str = file_name.split('/')[-2]
        return datetime.datetime.strptime(timestamp_str, "%Y%m%dT%H%M%SZ")

    zip_files_for_date = [zip_file for zip_file in zip_file_names if extract_timestamp(zip_file).date() == report_date]
    if not zip_files_for_date:
        raise ValueError(f"No zip files found for the date {report_date}")
    
    latest_zip_file = max(zip_files_for_date, key=extract_timestamp)
    
    obj = s3.get_object(Bucket=bucket_name, Key=latest_zip_file)
    zip_data = io.BytesIO(obj['Body'].read())
    
    with zipfile.ZipFile(zip_data, 'r') as z:
        file_in_zip = z.namelist()[0]
        with z.open(file_in_zip) as f:
            columns_to_select = ['bill/BillingPeriodStartDate', 'bill/BillingPeriodEndDate', 'lineItem/ProductCode', 'product/ProductName', 'lineItem/BlendedCost', 'lineItem/UnblendedCost']
            df = pd.read_csv(f, usecols=columns_to_select)
            df = df.rename(columns=lambda x: x.strip().lower().replace("/", '_'))
    
    return df

def aggregate_data(df, report_date):
    result = df.groupby(['bill_billingperiodstartdate', 'bill_billingperiodenddate', 'lineitem_productcode', 'product_productname']).agg({'lineitem_blendedcost': 'sum'}).reset_index()
    result['billingdate'] = report_date
    return result


def load_to_bigquery(result, report_date, gcp_keyfile_path, table_id):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_keyfile_path
    storage_client = bigquery.Client.from_service_account_json(gcp_keyfile_path)
    sql = f"DELETE FROM {table_id} WHERE billingdate = '{report_date}'"
    storage_client.query(sql).result()
    
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("product_productname", "STRING"),
    ])
    
    job = storage_client.load_table_from_dataframe(result, table_id, job_config=job_config)
    job.result()
    print(f"Loaded data for {report_date} into BigQuery table {table_id}")


def main(aws_access_key_id, aws_secret_access_key, bucket_name, report_date, gcp_keyfile_path):
    report_date = datetime.datetime.strptime(report_date, "%Y%m%d").date()
    
    # Get the date range
    date_range = get_date_range()
    # date_range = '20250201-20250301'     #--Use this to log backlogs

    
    # Read data from S3
    df = read_data_from_s3(aws_access_key_id, aws_secret_access_key, bucket_name, date_range, report_date)
    
    # Aggregate data
    result = aggregate_data(df, report_date)
    
    # Load to BigQuery
    load_to_bigquery(result, report_date, gcp_keyfile_path, 'aws_cost.aws_cost_report')

# Example usage
# dates = ['02','03','04','05','06','07','08',
    # '09','10','11','12','13','14','15','16']
# dates = ["01"]
# for days in dates:
main(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID") # config.aws_access_key_id,
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY") #config.aws_secret_access_key,
    bucket_name=os.getenv("BUCKET_NAME")  #config.bucket_name,
    report_date=(datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d"),
    # report_date='202502' + days, #-- Use this to log backlogs
    gcp_keyfile_path='heliumhealth-1ce77f433fc7.json'
)
