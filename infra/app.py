import boto3
import json
import time
import uuid
 
s3 = boto3.client("s3")
rsd = boto3.client("redshift-data")
 
CLUSTER_ID = os.environ["CLUSTER_ID"]
DATABASE = os.environ["DATABASE"]
DB_USER = os.environ["DB_USER"]
REDSHIFT_IAM_ROLE = os.environ["REDSHIFT_IAM_ROLE"]
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "data/splitted/")
SCHEMA_NAME = "public"
TABLE_NAME = "auto_table"
 
 
def execute_sql(sql):
    response = rsd.execute_statement(
        ClusterIdentifier=CLUSTER_ID,
        Database=DATABASE,
        DbUser=DB_USER,
        Sql=sql
    )
    statement_id = response["Id"]
 
    while True:
        desc = rsd.describe_statement(Id=statement_id)
        if desc["Status"] in ["FINISHED", "FAILED", "ABORTED"]:
            break
        time.sleep(1)
 
    if desc["Status"] != "FINISHED":
        raise Exception(desc)
 
    return rsd.get_statement_result(Id=statement_id)
 
 
def lambda_handler(event, context):
 
    # ----------------------------------
    # 1️⃣ Get first 2 parquet files
    # ----------------------------------
 
    response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=S3_PREFIX
    )
 
    parquet_files = [
        obj["Key"] for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]
 
    first_two = parquet_files[:2]
 
    if not first_two:
        raise Exception("No parquet files found")
 
    # ----------------------------------
    # 2️⃣ Create Manifest File
    # ----------------------------------
 
    manifest = {
        "entries": [
            {
                "url": f"s3://{S3_BUCKET}/{key}",
                "mandatory": True
            }
            for key in first_two
        ]
    }
 
    manifest_key = f"{S3_PREFIX}manifest.json"
 
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=manifest_key,
        Body=json.dumps(manifest)
    )
 
    manifest_path = f"s3://{S3_BUCKET}/{manifest_key}"
 
    # ----------------------------------
    # 3️⃣ Create Temporary External Table
    # ----------------------------------
 
    temp_table = f"temp_ext_{uuid.uuid4().hex[:8]}"
 
    create_external_sql = f"""
    CREATE EXTERNAL TABLE spectrum_schema.{temp_table}
    STORED AS PARQUET
    LOCATION 's3://{S3_BUCKET}/{S3_PREFIX}';
    """
 
    execute_sql(create_external_sql)
 
    # ----------------------------------
    # 4️⃣ Get Schema from SVV_EXTERNAL_COLUMNS
    # ----------------------------------
 
    schema_query = f"""
    SELECT columnname, external_type
    FROM SVV_EXTERNAL_COLUMNS
    WHERE schemaname = 'spectrum_schema'
      AND tablename = '{temp_table}';
    """
 
    result = execute_sql(schema_query)
 
    columns = result["Records"]
 
    column_defs = []
 
    for col in columns:
        col_name = col[0]["stringValue"]
        col_type = col[1]["stringValue"]
        column_defs.append(f"{col_name} {col_type}")
 
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
        {', '.join(column_defs)}
    );
    """
 
    execute_sql(create_table_sql)
 
    # ----------------------------------
    # 5️⃣ Drop External Table
    # ----------------------------------
 
    drop_sql = f"DROP TABLE spectrum_schema.{temp_table};"
    execute_sql(drop_sql)
 
    # ----------------------------------
    # 6️⃣ COPY using Manifest
    # ----------------------------------
 
    copy_sql = f"""
    COPY {SCHEMA_NAME}.{TABLE_NAME}
    FROM '{manifest_path}'
    IAM_ROLE '{IAM_ROLE}'
    FORMAT AS PARQUET
    MANIFEST;
    """
 
    execute_sql(copy_sql)
 
    return {
        "status": "Success",
        "table": TABLE_NAME
    }
 