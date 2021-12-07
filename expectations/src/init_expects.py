""" Module controlling steps in the Great Expectations pipeline

This module contains all methods run as KubernetesPodOperators in the main Great Expectations pipeline DAG. It requires
that the K8s pod is configured to have the Great Expectations library installed.

This file can be imported as a module and contains the following functions:

    * expectations_init - takes any existing expectation suite for the defined dataset and saves it to a temporary
        location in the S3 storage.
    * get_expectations - connects to the NRDAv2 API to retrieve existing expectation suites for the dataset defined in
        the DAG run configuration.
    * get_empty_expect_suite - creates an empty Great Expectation suite using the config defined in the
        expectations_config module. The suite is configured to run on a dataset loaded into a Spark DataFrame and
        save all outputs from the suite to pre-defined paths in the S3 storage.
    * add_expectations_to_suite - reads existing expectation suite from S3 storage, adds them to an empty expectation
        suite, and saves the populated expectation suite back to S3.
"""
import json
import logging
import sys
from typing import List, Dict

import requests
from great_expectations.data_context import BaseDataContext

from .conn_secrets import get_airflow_auth_conn, get_nrda_conn
from . import expectations_config as expect_config
from . import s3_connector

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(module)s %(levelname)s: %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)


def expectations_init(bucket_name, project_code, ledger_s3, dag_run_id) -> str:
    """Initialise the expectation pipeline, configure existing files.

    This method gets any existing expectation suites via the get_expectations() method, and then saves them to a defined
    path on the S3 storage.

    :param bucket_name: The name of the bucket on S3 where the expectations will be stored. Passed to Airflow as part of
    the ledger configuration.
    :type bucket_name: str
    :param project_code: The project code of the dataset that the expectation suite will be run against. Passed to
    Airflow as part of the ledger configuration
    :type project_code: str
    :param ledger_s3: The path on S3 to the saved ledger file for this run.
    :type ledger_s3: str
    :param dag_run_id: The unique run_id for the particular run of this DAG. Generated by Airflow.
    :type dag_run_id: str
    :return: The path on S3 where the existing expectation file will be saved for use later.
    :rtype: str
    """

    s3_client = s3_connector.make_s3_client(s3_connector.get_conn_details())
    ledger_obj = s3_client.get_object(Bucket=bucket_name, Key=ledger_s3)
    ledger = json.loads(ledger_obj["Body"].read())
    nrda_expect_suite = get_expectations(ledger)
    exist_expect_suite = {"expectations": nrda_expect_suite}

    # Store existing expectations to file on s3 to avoid issue of oversized arg for spark job
    s3_expect_json_path = "{0}/jobs/{1}/expects.json".format(project_code, dag_run_id)

    s3_connector.put_item(exist_expect_suite, bucket_name, s3_expect_json_path, s3_client)

    return s3_expect_json_path


def get_expectations(ledger) -> List[Dict]:
    """ Get an existing expectation suite for the dataset defined in the DAG run configuration.

    This uses the authentication information for the Airflow account to query the NRDAv2 API for the existing
    expectation suite for the dataset defined as part of the ledger parameter.
    If there is no existing expectation suite for this dataset then this function, and the whole pipeline, will fail.

    :param ledger: The ledger passed to the DAG as part of the run configuration. See nrda_expectation.py for an
        example of the expected format.
    :type ledger: dict
    :return: The existing expectation suite for the dataset defined in the ledger object.
    :rtype: (list of dict) or None
    """
    airflow_oauth2_conn = get_airflow_auth_conn()
    nrda_backend_api_conn = get_nrda_conn()

    token_url = airflow_oauth2_conn["host"]
    # client (application) credentials on keycloak
    client_id = airflow_oauth2_conn["login"]
    client_secret = airflow_oauth2_conn["password"]
    # step A, B - single call with client credentials as the basic auth header - will return access_token
    data = {'grant_type': 'client_credentials'}
    access_token_response = requests.post(token_url, data=data, verify=False, allow_redirects=False,
                                          auth=(client_id, client_secret))
    tokens = json.loads(access_token_response.text)

    # the URL for the existing expectation suite for the data asset defined in the DAG run config.
    get_expectation_url = "{0}/api/Expectation/Suite/{1}/{2}/{3}".format(nrda_backend_api_conn["host"],
                                                                         ledger['dataset_id'], ledger['label'],
                                                                         ledger['classification'])
    logger.info("get expectations backend url: {0}".format(get_expectation_url))

    # use access_token for NRDAv2 API that was retrieved using Airflow account details
    api_call_headers = {'Authorization': 'Bearer ' + tokens['access_token']}
    api_call_response = requests.get(get_expectation_url, headers=api_call_headers, verify=False)
    logger.info("response status code: {0}".format(api_call_response.status_code))
    # Cause the pipeline to fail if there is no existing expectation suite for this data asset.
    if api_call_response.status_code == 204:
        raise ValueError("No expectation suite found for that asset name / classification")
    res = api_call_response.json()

    # ONLY return the actual expectations, do not return metadata or storage data for the data asset the existing suite
    # was generated on
    return res['expectations']


def get_empty_expect_suite(bucket_name, dag_run_id, project_code) -> tuple[str, str, str, str]:
    """Generate an empty expectation suite.

    Generates an empty expectation suite using the configuration defined in expectations_config.get_expect_config() and
    writes to log the location on S3 where the various parts of the empty suite are stored.

    :param bucket_name: The name of the bucket on S3 where the expectations will be stored. Passed to Airflow as part of
    the ledger configuration.
    :type bucket_name: str
    :param dag_run_id: The unique run_id for the particular run of this DAG. Generated by Airflow.
    :type dag_run_id: str
    :param project_code: The project code of the dataset that the expectation suite will be run against. Passed to
    Airflow as part of the ledger configuration
    :type project_code: str
    :return: Paths on S3 corresponding to the location of the Expectation Suite json file, the Expectation Store, the
    Validation Store, and the Sitebuilder document Store.
    :rtype: tuple of str
    """

    # retrieve the config from the module
    config = expect_config.get_expect_config(bucket_name, project_code, dag_run_id)
    expect_suite_s3_loc = "{0}/jobs/{1}/ge_tmp/expectations/{1}.json".format(project_code, dag_run_id)
    expect_store_s3_loc = "{0}/jobs/{1}/ge_tmp/expectations".format(project_code, dag_run_id)
    expect_validations_s3_loc = "{0}/jobs/{1}/ge_tmp/uncommitted/validations".format(project_code, dag_run_id)
    expect_sitebuilder_s3_loc = "{0}/jobs/{1}/ge_tmp/uncommitted/data_docs/local_site".format(project_code, dag_run_id)

    # create the Great Expectation context using the retrieved config
    ge_context = BaseDataContext(project_config=config)
    logger.info("CREATED BASE DATA CONTEXT")
    # Create an empty Great Expectation Suite, configured by the retrieved config, using the dag_run_id as the empty
    # suite name, and overwriting any existing suite with the same name -- this is very unlikely given the DAG run id is
    # unique within the Airflow database
    # The created empty suite is written to the path in the S3 storage that is configured with the expect_config module,
    # and it is configured to be applied to a Spark DataFrame to ensure scalability with very large datasets.
    ge_context.create_expectation_suite(dag_run_id, overwrite_existing=True)
    logger.info("CREATED EXPECTATION SUITE AT: '{0}'".format(expect_suite_s3_loc))

    # Log all the Great Expectation configured write locations
    logger.info("Base data context bucket : {0}".format(bucket_name))
    logger.info("ExpectationsStore location : {0}".format(expect_store_s3_loc))
    logger.info(
        "ValidationsStore location : {0}".format(expect_validations_s3_loc))
    logger.info(
        "SiteBuilder location : {0}".format(expect_sitebuilder_s3_loc))

    return expect_suite_s3_loc, expect_store_s3_loc, expect_validations_s3_loc, expect_sitebuilder_s3_loc


def add_expectations_to_suite(bucket_name, s3_suite_path, existing_expects_s3_path) -> bool:
    """ Add existing expectations to the generated empty suite.

    This retrieves existing expectations that are stored in S3 and adds them to the generated empty BaseDataContext
    suite. The populated empty BaseDataContext is then saved back to S3 with the same file name, overwriting the empty
    suite and making it ready for application to the data asset defined in the DAG run configuration

    :param bucket_name: The name of the bucket on S3 where the expectations will be stored. Passed to Airflow as part of
    the ledger configuration.
    :type bucket_name: str
    :param s3_suite_path: Location of the empty Expectation Suite json file generated by get_empty_expect_suite() on S3.
    :type s3_suite_path: str
    :param existing_expects_s3_path: Location on S3 of the existing Expectation file retrieved from the NRDA API.
    :type existing_expects_s3_path: str
    :return: True
    :rtype: bool
    """

    s3_client = s3_connector.make_s3_client(s3_connector.get_conn_details())
    # get the empty suite
    suite_obj = s3_client.get_object(Bucket=bucket_name, Key=s3_suite_path)
    suite = json.loads(suite_obj['Body'].read())
    # get the pre-existing expectations
    expectations_obj = s3_client.get_object(Bucket=bucket_name, Key=existing_expects_s3_path)
    expectations_full = json.loads(expectations_obj['Body'].read())
    expectations = expectations_full['expectations']
    # if the pre-existing expectations file is not empty, then save them to the blank suite generated in a previous step
    if expectations:
        suite['expectations'] = expectations

    s3_client.put_object(
        Body=json.dumps(suite),
        Bucket=bucket_name,
        Key=s3_suite_path
    )

    return True


if __name__ == "__main__":
    bucket = sys.argv[1]
    project = sys.argv[2]
    ledger_path = sys.argv[3]
    run_id = sys.argv[4]

    exist_expects_s3_path = expectations_init(bucket, project, ledger_path, run_id)

    expect_suite_s3, expect_store_s3, expect_validations_s3, expect_sitebuilder_s3 = \
        get_empty_expect_suite(bucket, run_id, project)

    add_expectations_to_suite(bucket, expect_suite_s3, exist_expects_s3_path)
