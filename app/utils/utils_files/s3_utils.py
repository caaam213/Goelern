from typing import Any
import boto3
from botocore.exceptions import NoCredentialsError
import os


def save_data_on_s3(bucket:str, file_name:str, data:Any)->bool:
    """Save data on S3

    Args:
        bucket (str): Bucket name to save the data
        file_name (str): File name to save the data
        data (Any): Data to be saved

    Returns:
        bool: True if the data is saved, else False
    """

    session = boto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    s3 = session.resource("s3")

    if not data:
        print("No data to save")
        return False

    try:
        object = s3.Object(bucket, file_name)
        res = object.put(Body=str(data))
        print(f"Data saved to {bucket}/{file_name}")
        return True
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

import os
import boto3
import logging
from typing import Any, Optional

def get_data_from_s3(bucket: str, file_name: str) -> Optional[Any]:
    """Get data from a file in an S3 bucket.

    Args:
        bucket (str): The name of the S3 bucket.
        file_name (str): The name of the file to retrieve.

    Returns:
        Optional[Any]: Content of the file if found, otherwise None.
    """
    try:
        # Verify AWS credentials are set
        if "AWS_ACCESS_KEY_ID" not in os.environ or "AWS_SECRET_ACCESS_KEY" not in os.environ:
            logging.error("AWS credentials are not set in environment variables.")
            return None

        # Initialize the S3 client
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )

        # Attempt to retrieve the file directly
        try:
            response = s3.get_object(Bucket=bucket, Key=file_name)
            logging.info(f"File '{file_name}' successfully retrieved from '{bucket}' bucket.")
            return response["Body"].read()
        
        except s3.exceptions.NoSuchKey:
            logging.error(f"File '{file_name}' not found in bucket '{bucket}'.")
            return None

    except Exception as e:
        logging.error(f"An error occurred while retrieving data from S3: {e}")
        return None

