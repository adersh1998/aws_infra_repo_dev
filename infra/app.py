import json
import os
import boto3
import pyarrow.parquet as pq
import io
import time
 
s3 = boto3.client("s3")
redshift = boto3.client("redshift-data")
 
CLUSTER_ID = os.environ["CLUSTER_ID"]
DATABASE = os.environ["DATABASE"]
DB_USER = os.environ["DB_USER"]
S3_BUCKET = os.environ["S3_BUCKET"]
REDSHIFT_IAM_ROLE = os.environ["REDSHIFT_IAM_ROLE"]
 
SCHEMA_NAME = "public"
TABLE_NAME = "auto_table"
 
 
def map_type(parquet_type):
    mapping = {
        "int64": "BIGINT",
        "int32": "INTEGER",
        "double": "DOUBLE PRECISION",
        "float": "REAL",
        "bool": "BOOLEAN",
        "string": "VARCHAR(65535)",
        "timestamp[ms]": "TIMESTAMP",
        "date32[day]": "DATE"
    }
    return mapping.get(str(parquet_type), "VARCHAR(65535)")
 
 
def wait_for_query(statement_id):
    while True:
        desc = redshift.describe_statement(Id=statement_id)
        if desc["Status"] in ["FINISHED", "FAILED", "ABORTED"]:
            return desc
        time.sleep(2)
 
 
def lambda_handler(event, context):
 
    prefix = event.get("prefix", "")
 
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    parquet_files = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]
 
    if not parquet_files:
        return {"message": "No parquet files found"}
 
    # Detect schema from first file
    obj = s3.get_object(Bucket=S3_BUCKET, Key=parquet_files[0])
    table = pq.read_table(io.BytesIO(obj["Body"].read()))
    schema = table.schema
 
    columns = []
    for field in schema:
        columns.append(f'"{field.name}" {map_type(field.type)}')
 
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
        {', '.join(columns)}
    );
    """
 
    create_resp = redshift.execute_statement(
        ClusterIdentifier=CLUSTER_ID,
        Database=DATABASE,
        DbUser=DB_USER,
        Sql=create_sql
    )
 
    wait_for_query(create_resp["Id"])
 
    # Create manifest
    manifest_entries = [
        {"url": f"s3://{S3_BUCKET}/{key}", "mandatory": True}
        for key in parquet_files
    ]
 
    manifest_key = f"{prefix}/manifest.json"
 
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=manifest_key,
        Body=json.dumps({"entries": manifest_entries})
    )
 
    copy_sql = f"""
    COPY {SCHEMA_NAME}.{TABLE_NAME}
    FROM 's3://{S3_BUCKET}/{manifest_key}'
    IAM_ROLE '{REDSHIFT_IAM_ROLE}'
    FORMAT AS PARQUET
    MANIFEST;
    """
 
    copy_resp = redshift.execute_statement(
        ClusterIdentifier=CLUSTER_ID,
        Database=DATABASE,
        DbUser=DB_USER,
        Sql=copy_sql
    )
 
    result = wait_for_query(copy_resp["Id"])
 
    if result["Status"] != "FINISHED":
        raise Exception("COPY failed")
 
    return {"message": "Data loaded successfully"}
 