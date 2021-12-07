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
    try:
        nrda_conn = {"host": os.environ["NRDAPI_HOST"]}
        return nrda_conn
    except KeyError as e:
        raise KeyError(e)


def get_airflow_auth_conn() -> dict[str]:
    """Gets the authentication information for querying NRDA API from an Airflow-managed process.

    :return: A dictionary with 3 connection keys required to query the NRDA API.
    :rtype: dict
    """
    try:
        af_oauth_conn = {"host": os.environ["AFOAUTH_HOST"],
                         "login": os.environ["AFOAUTH_LOGIN"],
                         "password": os.environ["AFOAUTH_PASS"]
                         }
        return af_oauth_conn
    except KeyError as e:
        raise KeyError(e)
