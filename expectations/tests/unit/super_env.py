def get_s3_env():
    return {
        "S3_ENDPOINT": "http://192.168.49.1:9000",
        "S3_ACCESS_KEY": "admin",
        "S3_ACCESS_SECRET": "MyP4ssW0rd"
    }


def get_nrda_env():
    return {
        "NRDAPI_HOST": "http://192.168.49.1:5000"
    }


def get_airflow_env():
    return {
        "AFOAUTH_HOST": "http://192.168.49.1:5000",
        "AFOAUTH_LOGIN": "airflow",
        "AFOAUTH_PASS": "airflow"
    }