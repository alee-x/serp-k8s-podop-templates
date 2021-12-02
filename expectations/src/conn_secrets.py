"""Gets connection secrets mounted in the pod.

This module gets the connection secrets mounted in the pod by Airflow so that they can be used to connected to the
corresponding services.
This file can be imported as a module and contains the following functions:

    * get_nrda_conn - gets the URL needed to connect to the NRDA API and returns it as dictionary.
    * get_airflow_auth_conn - gets the authentication deals for Airflow managed services to connect to the NRDA API.

"""
import os


def get_nrda_conn() -> dict[str]:
    """Gets the endpoint URL for connecting to the NRDA API.

    :return: A dictionary with one key, host, with value of the NRDA API endpoint URL as a string.
    :rtype: dict
    """
    nrda_conn = {
        "host": os.getenv("NRDAPI_HOST")
    }
    return nrda_conn


def get_airflow_auth_conn() -> dict[str]:
    """Gets the authentication information for querying NRDA API from an Airflow-managed process.

    :return: A dictionary with 3 connection keys required to query the NRDA API.
    :rtype: dict
    """
    af_oauth_conn = {
        "host": os.getenv("AFOAUTH_HOST"),
        "login": os.getenv("AFOAUTH_LOGIN"),
        "password": os.getenv("AFOAUTH_PASS")
    }
    return af_oauth_conn
