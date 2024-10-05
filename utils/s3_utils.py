import boto3
from botocore.exceptions import NoCredentialsError
import os


def save_data_on_s3(file_name, data):

    session = boto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    s3 = session.resource("s3")

    if not data:
        print("No data to save")
        return False

    try:
        object = s3.Object(os.environ["S3_BUCKET"], file_name)
        res = object.put(Body=str(data))
        print(f"Data saved to {os.environ['S3_BUCKET']}/{file_name}")
        return True
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

def get_data_from_s3(file_name):

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    objects_list = s3.list_objects_v2(Bucket=os.environ["S3_BUCKET"]).get("Contents")

    for result in objects_list:
        if file_name in result["Key"]:
            data = s3.get_object(Bucket=os.environ["S3_BUCKET"], Key=file_name)
            return data["Body"].read()
