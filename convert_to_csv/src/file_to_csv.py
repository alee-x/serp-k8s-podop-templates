import struct

import pandas as pd
import logging
import sys
import json
import csv
import pyreadstat
import os
from botocore.exceptions import ClientError
from pyreadstat._readstat_parser import ReadstatError

from . import s3_connector

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(module)s %(levelname)s: %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)


def get_ledger(ledger_s3, bucket_name):
    s3_client = s3_connector.make_s3_client(s3_connector.get_conn_details())
    ledger_obj = s3_client.get_object(Bucket=bucket_name, Key=ledger_s3)
    ledger = json.loads(ledger_obj["Body"].read())
    target_file_location = ledger["location_details"]
    logger.info("Ledger loaded from bucket {0}, path {1}".format(bucket_name, ledger_s3))
    logger.info("The location of the file to be converted is: {0}".format(target_file_location))
    return ledger, target_file_location


def get_target_file(target_file_path):
    target_file_loc_type = target_file_path.split("/")[0]
    logger.info("Location type where the target file is kept : {0}".format(target_file_loc_type))
    if target_file_loc_type == "s3a:":
        s3_client = s3_connector.make_s3_client(s3_connector.get_conn_details())
        # derive bucket for the file to be converted from the full path given
        target_file_bucket = target_file_path.split("/")[2]
        target_file_route = "/".join(target_file_path.split("/")[3:])
        logger.info("The target file will be loaded from bucket {0} at path {1}".format(target_file_bucket,
                                                                                        target_file_route))
        try:
            target_file = s3_client.get_object(Bucket=target_file_bucket, Key=target_file_route)["Body"]
            logger.info("TARGET FILE LOADED SUCCESSFULLY")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.critical("The file at path {0} does not exist! DAG failing.".format(target_file_path))
            if e.response["Error"]["Code"] == "NoSuchBucket":
                logger.critical("The bucket {0} does not exist! DAG failing.".format(target_file_bucket))
            raise e
    else:
        raise ValueError("This can currently only get files from S3")
    return target_file


def convert_spss(local_target_file, target_file_path):
    # We have to save the file to disk temporarily because pyreadstat can't handle streaming data
    temp_filename = "tmp-spss.sav"
    with open(temp_filename, 'wb') as f:
        f.write(local_target_file.read())
    try:
        df, meta = pyreadstat.read_sav("tmp-spss.sav")
    except ReadstatError as error:
        logger.critical("PyReadStat couldn't parse the file. It is either corrupted or empty. The file it tried to read"
                        "is {0}".format(target_file_path))
        raise error

    values_list = []
    for key1, value1 in meta.variable_value_labels.items():
        for key2, value2 in value1.items():
            values_list.append([key1, key2, value2])

    val_df = pd.DataFrame(data=values_list, columns=["Field", "Code", "Meaning"])

    desc_list = []
    for k1, v1 in meta.column_names_to_labels.items():
        desc_list.append([k1, v1])

    desc_df = pd.DataFrame(data=desc_list, columns=["Field", "Meaning"])

    logger.info("SPSS file {0} has been converted to CSV".format(target_file_path))

    # now we delete the temp file
    os.remove(temp_filename)
    return df, {"-values": val_df, "-description": desc_df}


def convert_stata(local_target_file, target_file_path):
    # We have to save the file to disk temporarily because we can't use .read() twice on StreamingBddy objects
    temp_filename = "tmp-stata.dta"
    with open(temp_filename, 'wb') as f:
        f.write(local_target_file.read())
    logger.info("Converting the file from Stata to csv")

    try:
        df = pd.read_stata(temp_filename)
        sr = pd.io.stata.StataReader(temp_filename)
        vl = sr.value_labels()
        sr.close()
    except struct.error as err:
        logger.critical("The stata file at location {0} is either empty or corrupted. DAG failing."
                        .format(target_file_path))
        raise err
    except ValueError as err:
        logger.critical("Either the version of the given Stata file is not supported by pandas, or it's not a Stata "
                        "file at all. File given: {0}. DAG failing.".format(target_file_path))
        raise err

    values_list = []
    for key1, value1 in vl.items():
        for key2, value2 in value1.items():
            values_list.append([key1, key2, value2])

    val_df = pd.DataFrame(data=values_list, columns=["Field", "Code", "Meaning"])
    logger.info("STATA file {0} has been converted to CSV".format(target_file_path))

    # now we delete the temp file
    os.remove(temp_filename)
    return df, {"-values": val_df}


def put_converted_file_s3(converted_df, extra_dfs, bucket_name, project_code, dag_run_id, target_file_path):
    converted_csv_name = target_file_path.split("/")[-1].split(".")[0]
    converted_csv_path = "{0}/jobs/{1}/converted_files/{2}.csv".format(project_code, dag_run_id, converted_csv_name)

    s3_conn = s3_connector.get_conn_details()
    s3_client = s3_connector.make_s3_client(s3_conn)
    # check that the bucket we're writing to still exists
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as err:
        if err.response["Error"]["Code"] == "404":
            logger.critical("The bucket {0} does not exist! DAG failing.".format(bucket_name))
        raise err

    logger.info("The converted file will be saved in bucket {0} at path {1}".format(bucket_name, converted_csv_path))

    converted_df.to_csv(
        f"s3://{bucket_name}/{converted_csv_path}",
        index=False,
        storage_options={
            "key": s3_conn["access_key"],
            "secret": s3_conn["access_secret"],
            "client_kwargs": {
                "endpoint_url": s3_conn["endpoint"]
            }
        },
    )

    logger.info("The converted file has been saved in bucket {0} at path {1}".format(bucket_name, converted_csv_path))

    for name, extra_df in extra_dfs.items():
        extra_df_path = "{0}/jobs/{1}/converted_files/{2}{3}.csv".format(project_code, dag_run_id, converted_csv_name,
                                                                         name)
        extra_df.to_csv(
            f"s3://{bucket_name}/{extra_df_path}",
            index=False,
            header=True,
            quoting=csv.QUOTE_NONNUMERIC,
            storage_options={
                "key": s3_conn["access_key"],
                "secret": s3_conn["access_secret"],
                "client_kwargs": {
                    'endpoint_url': s3_conn['endpoint']
                }
            },
        )

        logger.info("The extra file has been saved in bucket {0} at path {1}".format(bucket_name, extra_df_path))

    return converted_csv_path


def check_converted_file(bucket_name, converted_file_path):
    s3_client = s3_connector.make_s3_client(s3_connector.get_conn_details())
    try:
        s3_client.get_object(Bucket=bucket_name, Key=converted_file_path)["Body"].read()
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.critical("The csv file in bucket {0} at path {1} does not seem to exist. This DAG FAILED!"
                            .format(bucket_name, converted_file_path))
        if e.response["Error"]["Code"] == "NoSuchBucket":
            logger.critical("The bucket {0} does not exist! DAG failing.".format(bucket_name))
        raise e
    return True


def main(bucket_name, project_code, ledger_s3, dag_run_id):
    task_ledger, target_file_loc = get_ledger(ledger_s3, bucket_name)
    targ_file_type = task_ledger["attributes"]["file_type"].lower()
    if targ_file_type not in ["spss", "stata"]:
        raise ValueError("The target file type in the ledger is not 'spss' or 'stata' -- are you this is correct?")

    loaded_target_file = get_target_file(target_file_loc)

    if targ_file_type == "spss":
        file_as_df, other_dfs = convert_spss(loaded_target_file, target_file_loc)
    elif targ_file_type == "stata":
        file_as_df, other_dfs = convert_stata(loaded_target_file, target_file_loc)
    else:
        raise ValueError("The target file type in the ledger is not 'spss' or 'stata' -- are you this is correct?")

    converted_file_loc = put_converted_file_s3(file_as_df, other_dfs, bucket_name, project_code, dag_run_id,
                                               target_file_loc)
    result = check_converted_file(bucket_name, converted_file_loc)
    return result


if __name__ == "__main__":
    bucket = sys.argv[1]
    project = sys.argv[2]
    ledger_path = sys.argv[3]
    run_id = sys.argv[4]

    main(bucket, project, ledger_path, run_id)
