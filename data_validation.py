import boto3
import csv
import json
import os

# AWS Services
s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    Validates CSV data from an S3 bucket and writes results to another S3 bucket.
    """
    try:
        # 1. Get S3 Bucket and File Information
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
        output_bucket = os.environ['OUTPUT_BUCKET'] # Destination bucket for results
        # 2. Download the CSV file from S3
        csv_file = s3.get_object(Bucket=bucket_name, Key=file_key)
        csv_content = csv_file['Body'].read().decode('utf-8')

        # 3. Define Validation Rules (Example - adapt to your needs)
        validation_rules = {
            'column1': {'type': 'string', 'required': True},
            'column2': {'type': 'integer', 'min': 0},
            'column3': {'type': 'email'} # requires separate email validation function
        }

        # 4. Validate the Data
        validation_results = validate_csv_data(csv_content, validation_rules)
        #5 Create the validation file
        validation_file = create_validation_file(file_key, validation_results)
        # 6. Upload Validation Results to S3
        output_key = f"validation_results/{file_key.replace('.csv', '.json')}" # Different directory
        s3.put_object(Bucket=output_bucket, Key=output_key, Body=json.dumps(validation_file))

        return {
            'statusCode': 200,
            'body': f"Validation complete. Results uploaded to s3://{output_bucket}/{output_key}"
        }

    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': f"Error: {e}"
        }

def validate_csv_data(csv_content, validation_rules):
    """
    Validates CSV data against predefined rules.
    """
    results = []
    reader = csv.DictReader(csv_content.splitlines())
    for row_num, row in enumerate(reader, start=1): #Start at line 1 for human readability
        row_results = {'row': row_num, 'errors': []} # Capture row number for error identification
        for column, rules in validation_rules.items():
            value = row.get(column) #Safely handle missing column
            if rules.get('required') and not value:
                row_results['errors'].append(f"Column '{column}' is required.")
            if rules.get('type') == 'integer':
                try:
                    int(value)
                except ValueError:
                    row_results['errors'].append(f"Column '{column}' must be an integer.")

            if rules.get('type') == 'email' and value:
                if not is_valid_email(value):
                    row_results['errors'].append(f"Column '{column}' is not a valid email address.")
        results.append(row_results)

    return results

def create_validation_file(filename, validation_results):

    validation_file = {
        "filename" : filename,
        "validationResults" : validation_results
    }
    return validation_file

def is_valid_email(email):
    import re

    email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

    return bool(re.match(email_pattern, email))
