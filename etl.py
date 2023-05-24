import boto3
from botocore import UNSIGNED
from botocore.client import Config



def extract():
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    bucket_name = "tenalytics-internship-bucket"
    prefix = "orders_data"

    # List objects in the s3 bucket
    response = s3.list_objects(Bucket=bucket_name, Prefix=prefix)

    # Check if objects are found
    if 'Contents' in response:
        for obj in response['Contents']:
            # Print boject
            print(obj['Key'])
    else:
        print("No objects found in the specified bucket and prefix.")

    # Check if objects are found
    if 'Contents' in response:
        for obj in response['Contents']:
            file_name = obj['Key'].split('/')[-1]
            if file_name == '': continue
            # Download the file
            s3.download_file(bucket_name, obj['Key'], file_name)
    else:
        print('No object found in the specified bucket and prefix.')
        
