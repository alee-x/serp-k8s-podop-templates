import os
import struct
from unittest import TestCase, mock, main
import logging
import warnings
from pandas import DataFrame

from botocore.client import BaseClient
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from pyreadstat._readstat_parser import ReadstatError

from convert_to_csv.src import s3_connector
from convert_to_csv.src import file_to_csv
from . import super_env


class Test(TestCase):
    bucket = "pseudo"
    invalid_bucket = "not"
    project = "test_project"
    spss_ledger_path = "test/spss_ledger.json"
    stata_ledger_path = "test/stata_ledger.json"
    not_ledger_path = "not/a/ledger.json"
    invalid_type_ledger_path = "test/invalid_type.json"
    invalid_fileloc_ledger_path = "test/invalid_file.json"
    not_s3_ledger_path = "test/not_s3.json"
    run_id = "test_run_id"
    raw_spss_ledger = {
        "attributes": {
            "file_type": "spss",
            "targetclassification": "testclass"
        },
        "location_details": "s3a://pseudo/spss/survey.sav",
        "label": "",
        "version": "",
        "group_count": ""
    }
    raw_stata_ledger = {
        "attributes": {
            "file_type": "stata",
            "targetclassification": "testclass"
        },
        "location_details": "s3a://pseudo/stata/ch6data.dta",
        "label": "",
        "version": "",
        "group_count": ""
    }
    invalid_location_ledger = {
        "attributes": {
            "file_type": "stata",
            "targetclassification": "testclass"
        },
        "location_details": "s3a://pseudo/not/a/file.dta",
        "label": "",
        "version": "",
        "group_count": ""
    }
    invalid_type_ledger = {
        "attributes": {
            "file_type": "invalid_type",
            "targetclassification": "testclass"
        },
        "location_details": "s3a://pseudo/stata/ch6data.dta",
        "label": "",
        "version": "",
        "group_count": ""
    }
    not_s3_ledger = {
        "attributes": {
            "file_type": "spss",
            "targetclassification": "testclass"
        },
        "location_details": "hdfs://not/a/file/path.dta",
        "label": "",
        "version": "",
        "group_count": ""
    }

    @classmethod
    def setUpClass(cls, raw_spss_ledger=raw_spss_ledger, spss_ledger_path=spss_ledger_path,
                   raw_stata_ledger=raw_stata_ledger, stata_ledger_path=stata_ledger_path,
                   invalid_location_ledger=invalid_location_ledger,
                   invalid_fileloc_ledger_path=invalid_fileloc_ledger_path,
                   invalid_type_ledger=invalid_type_ledger, invalid_type_ledger_path=invalid_type_ledger_path,
                   not_s3_ledger=not_s3_ledger, not_s3_ledger_path=not_s3_ledger_path, bucket=bucket):
        env_vars = super_env.get_s3_env()
        cls.env_patcher = mock.patch.dict(os.environ, env_vars)
        cls.env_patcher.start()
        logging.disable(logging.CRITICAL)
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

        s3_client = s3_connector.make_s3_client(s3_connector.get_conn_details())
        s3_connector.put_item(raw_spss_ledger, bucket, spss_ledger_path, s3_client)
        s3_connector.put_item(raw_stata_ledger, bucket, stata_ledger_path, s3_client)
        s3_connector.put_item(invalid_type_ledger, bucket, invalid_type_ledger_path, s3_client)
        s3_connector.put_item(invalid_location_ledger, bucket, invalid_fileloc_ledger_path, s3_client)
        s3_connector.put_item(not_s3_ledger, bucket, not_s3_ledger_path, s3_client)

        super().setUpClass()

    @classmethod
    def tearDownClass(cls, bucket=bucket, spss_ledger_path=spss_ledger_path, stata_ledger_path=stata_ledger_path,
                      invalid_file_ledger_path=invalid_fileloc_ledger_path, project=project, run_id=run_id,
                      invalid_type_ledger_path=invalid_type_ledger_path, not_s3_ledger_path=not_s3_ledger_path):
        super().tearDownClass()
        s3_client = s3_connector.make_s3_client(s3_connector.get_conn_details())
        # delete the test ledgers
        for ledger_loc in [spss_ledger_path, stata_ledger_path, invalid_file_ledger_path, invalid_type_ledger_path,
                           not_s3_ledger_path]:
            s3_client.delete_object(Bucket=bucket, Key=ledger_loc)

        for file in s3_client.list_objects_v2(Bucket=bucket, Prefix=project+"/jobs")["Contents"]:
            s3_client.delete_object(Bucket=bucket, Key=file["Key"])

        logging.disable(logging.NOTSET)
        cls.env_patcher.stop()

    def test_put_converted_file_s3_spss(self, bucket_name=bucket, run_id=run_id, project_code=project,
                                        ledger=raw_spss_ledger):
        file_to_convert_path = ledger["location_details"]
        file_to_convert = file_to_csv.get_target_file(file_to_convert_path)
        main_df, secondary_dfs = file_to_csv.convert_spss(file_to_convert, file_to_convert_path)

        converted_csv_name = file_to_convert_path.split("/")[-1].split(".")[0]
        converted_csv_path = "{0}/jobs/{1}/converted_files/{2}.csv".format(project_code, run_id, converted_csv_name)

        result = file_to_csv.put_converted_file_s3(main_df, secondary_dfs, bucket_name, project_code, run_id,
                                                   file_to_convert_path)

        self.assertEqual(result, converted_csv_path)

    def test_put_converted_file_s3_stata(self, bucket_name=bucket, run_id=run_id, project_code=project,
                                         ledger=raw_stata_ledger):
        file_to_convert_path = ledger["location_details"]
        file_to_convert = file_to_csv.get_target_file(file_to_convert_path)
        main_df, secondary_dfs = file_to_csv.convert_stata(file_to_convert, file_to_convert_path)

        converted_csv_name = file_to_convert_path.split("/")[-1].split(".")[0]
        converted_csv_path = "{0}/jobs/{1}/converted_files/{2}.csv".format(project_code, run_id, converted_csv_name)

        result = file_to_csv.put_converted_file_s3(main_df, secondary_dfs, bucket_name, project_code, run_id,
                                                   file_to_convert_path)

        self.assertEqual(result, converted_csv_path)

    def test_put_converted_file_s3_fake_bucket(self, run_id=run_id, project_code=project, ledger=raw_stata_ledger):
        bucket_name = "notabucket"
        file_to_convert_path = ledger["location_details"]
        file_to_convert = file_to_csv.get_target_file(file_to_convert_path)
        main_df, secondary_dfs = file_to_csv.convert_stata(file_to_convert, file_to_convert_path)

        self.assertRaises(ClientError, file_to_csv.put_converted_file_s3, main_df, secondary_dfs, bucket_name,
                          project_code, run_id, file_to_convert_path)

    def test_main_valid_spss(self, bucket=bucket, project=project, ledger_path=spss_ledger_path, run_id=run_id):
        result = file_to_csv.main(bucket, project, ledger_path, run_id)
        self.assertTrue(result)

    def test_main_valid_stata(self, bucket=bucket, project=project, ledger_path=stata_ledger_path, run_id=run_id):
        result = file_to_csv.main(bucket, project, ledger_path, run_id)
        self.assertTrue(result)

    def test_main_invalid_bucket(self, bucket=invalid_bucket, project=project, ledger_path=stata_ledger_path,
                                 run_id=run_id):
        self.assertRaises(ClientError, file_to_csv.main, bucket, project, ledger_path, run_id)

    def test_main_not_path_to_ledger(self, bucket=bucket, project=project, ledger_path=not_ledger_path, run_id=run_id):
        self.assertRaises(ClientError, file_to_csv.main, bucket, project, ledger_path, run_id)

    def test_main_invalid_type(self, bucket=bucket, project=project, ledger_path=invalid_type_ledger_path,
                               run_id=run_id):
        self.assertRaises(ValueError, file_to_csv.main, bucket, project, ledger_path, run_id)

    def test_main_missing_tfile(self, bucket=bucket, project=project, ledger_path=invalid_fileloc_ledger_path,
                                run_id=run_id):
        self.assertRaises(ClientError, file_to_csv.main, bucket, project, ledger_path, run_id)

    def test_main_file_not_on_s3(self, bucket=bucket, project=project, ledger_path=not_s3_ledger_path, run_id=run_id):
        self.assertRaises(ValueError, file_to_csv.main, bucket, project, ledger_path, run_id)


if __name__ == "__main__":
    main()
