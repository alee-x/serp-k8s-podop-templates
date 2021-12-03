"""Module controlling the connection to the S3 storage.

This module contains the methods to configure the connections to S3 via Boto, as well as helper methods for code stubs
that will be reused across a number of methods and modules.

This file can be imported as a module and contains the following functions:

    * get_conn_details - gets the information from the Kubernetes secret named "s3-secret" which is mounted to the
        application pod as part of the file store.
    * make_s3_client - configures a boto3 client connection using either connection details passed to it, or credentials
        retrieved by get_conn_details if no connection credentials are passed as an argument.


"""
import boto3
from botocore.client import Config, BaseClient
import os


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
    s3_client = boto3.client('s3', aws_access_key_id=s3_conn["access_key"],
                             aws_secret_access_key=s3_conn["access_secret"], endpoint_url=s3_conn["endpoint"],
                             config=Config(signature_version='s3v4'))
    return s3_client
