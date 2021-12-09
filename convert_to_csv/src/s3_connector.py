"""Module controlling the connection to the S3 storage.

This module contains the methods to configure the connections to S3 via Boto, as well as helper methods for code stubs
that will be reused across a number of methods and modules.

This file can be imported as a module and contains the following functions:

    * get_conn_details - gets the information from the Kubernetes secret named "s3-secret" which is mounted to the
        application pod as part of the file store.
    * make_s3_client - configures a boto3 client connection using either connection details passed to it, or credentials
        retrieved by get_conn_details if no connection credentials are passed as an argument.
    * put_item - places a dict object to given path and bucket on S3 as a serialized JSON file. Creates the bucket if it
        doesn't already exist.


"""
import json
import os

import boto3
from botocore.client import Config, BaseClient
from botocore.exceptions import ParamValidationError, ClientError


def get_conn_details() -> dict:
    """Gets S3 connection info from Kubernetes secret "s3-secret".

    This gets the s3 connection configuration info from the Kubernetes secret "s3-secret". The secret is mounted to the
    pod as file system objects and is read from there. Each object is transformed to a dictionary string for ease of
    reuse.

    :return: The credential information for the S3 connection that is needed to use Boto3 to connect.
    :rtype: dict
    """
    s3_conn = {
        "endpoint": os.getenv("S3_ENDPOINT"),
        "access_key": os.getenv("S3_ACCESS_KEY"),
        "access_secret": os.getenv("S3_ACCESS_SECRET")
    }
    return s3_conn


def make_s3_client(s3_conn=get_conn_details()) -> BaseClient:
    """Makes an S3 client object that can be used to interact with the S3 storage.

    This makes a boto3 client object that is configured to talk to the defined S3 storage. The parameter of s3_conn is
    OPTIONAL, and if nothing is passed it defaults to the values contained in the Airflow connection with connection id
    "s3_conn".

    :param s3_conn: A dict of strings with the keys [access_key, access_secret, endpoint], defaults to the values
        contained in the Kubernetes secret with the name "s3-secret".
    :type s3_conn: dict
    :return: The configured Boto3 client object that is ready to connect to the given S3 service.
    :rtype: BaseClient
    """
    try:
        s3_client = boto3.client('s3', aws_access_key_id=s3_conn["access_key"],
                                 aws_secret_access_key=s3_conn["access_secret"], endpoint_url=s3_conn["endpoint"],
                                 config=Config(signature_version='s3v4'))
    except ValueError as e:
        raise ValueError(e)

    return s3_client


def put_item(object_to_put, bucket_name, target_path, s3_client) -> bool:
    """Writes a given dict object to S3 in json format.

    This method writes a given Python dictionary to JSON format and places it in the defined S3 bucket at the chosen
    S3 path.

    :param object_to_put: The object to put in S3, encoded as a dictionary
    :type object_to_put: dict
    :param bucket_name: The name of the bucket to put the item in. If the bucket doesn't exist this method will create
        it.
    :type bucket_name: str
    :param target_path: The path on S3 (excl bucket name) to put the object to be stored.
    :type target_path: str
    :param s3_client: The pre-configured s3_client.
    :type s3_client: BaseClient
    :return: True if operation succeeded, raises errors otherwise.
    :rtype: bool
    """
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
            raise ClientError(e)
    try:
        s3_client.put_object(
            Body=json.dumps(object_to_put),
            Bucket=bucket_name,
            Key=target_path
        )
        return True
    except ParamValidationError:
        raise ParamValidationError()
    except ClientError as e:
        raise ClientError(e)
