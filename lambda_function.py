import psycopg2
import boto3
import io
import datetime

def lambda_handler(event, context):
    # PostgreSQL connection details
    db_host = 'ec2-3-15-148-94.us-east-2.compute.amazonaws.com'
    db_name = 'eportal_db_prod'
    db_user = 'postgres'
    db_password = 'Decklaration'
    db_port = 5432  # Replace with your PostgreSQL port if different

    # Get the current date in the format 'YYYY-MM-DD'
    current_date = datetime.datetime.now().strftime('%Y-%m-%d')

    # S3 bucket and object details
    s3_bucket_name = 'dump-psql'
    s3_object_key = f'backups/db_backup_{current_date}.sql'

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        cursor = conn.cursor()

        # Perform the database dump and store it in a file-like object
        with io.StringIO() as dump_file:
            cursor.copy_expert(f"COPY (SELECT * FROM your_table) TO STDOUT WITH CSV", dump_file)
            dump_file.seek(0)

            # Upload the dump file to S3
            s3_client = boto3.client('s3')
            s3_client.put_object(Bucket=s3_bucket_name, Key=s3_object_key, Body=dump_file.read())

        # Close the database connection
        cursor.close()
        conn.close()

        return {
            'statusCode': 200,
            'body': 'Database dump successfully stored in S3.'
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
