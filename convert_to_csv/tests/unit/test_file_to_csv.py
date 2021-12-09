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
    project = "test_project"
    spss_ledger_path = "test/spss_ledger.json"
    stata_ledger_path = "test/stata_ledger.json"
    run_id = "test_run_id"
    spss_converted_path = "test/converted_spss.csv"
    stata_converted_path = "test/converted_stata.csv"
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

    @classmethod
    def setUpClass(cls, raw_spss_ledger=raw_spss_ledger, spss_ledger_path=spss_ledger_path,
                   raw_stata_ledger=raw_stata_ledger, stata_ledger_path=stata_ledger_path, bucket=bucket):
        env_vars = super_env.get_s3_env()
        cls.env_patcher = mock.patch.dict(os.environ, env_vars)
        cls.env_patcher.start()
        logging.disable(logging.CRITICAL)
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

        s3_client = s3_connector.make_s3_client(s3_connector.get_conn_details())
        s3_connector.put_item(raw_spss_ledger, bucket, spss_ledger_path, s3_client)
        s3_connector.put_item(raw_stata_ledger, bucket, stata_ledger_path, s3_client)

        super().setUpClass()

    @classmethod
    def tearDownClass(cls, bucket=bucket, spss_ledger_path=spss_ledger_path, stata_ledger_path=stata_ledger_path,
                      project=project, run_id=run_id):
        super().tearDownClass()
        s3_client = s3_connector.make_s3_client(s3_connector.get_conn_details())
        # delete the test ledgers
        for ledger_loc in [spss_ledger_path, stata_ledger_path]:
            s3_client.delete_object(Bucket=bucket, Key=ledger_loc)
        logging.disable(logging.NOTSET)
        cls.env_patcher.stop()

    def test_get_ledger_spss(self, bucket_name=bucket, spss_ledger_path=spss_ledger_path, raw_ledger=raw_spss_ledger):
        result_ledger, file_to_convert_loc = file_to_csv.get_ledger(spss_ledger_path, bucket_name)
        self.assertEqual(result_ledger, raw_ledger)
        self.assertEqual(file_to_convert_loc, raw_ledger["location_details"])

    def test_get_ledger_stata(self, bucket_name=bucket, stata_ledger_path=stata_ledger_path, raw_ledger=raw_stata_ledger):
        result_ledger, file_to_convert_loc = file_to_csv.get_ledger(stata_ledger_path, bucket_name)
        self.assertEqual(result_ledger, raw_ledger)
        self.assertEqual(file_to_convert_loc, raw_ledger["location_details"])

    def test_get_target_file_spss(self, raw_ledger=raw_spss_ledger):
        spss_path = raw_ledger["location_details"]
        result = file_to_csv.get_target_file(spss_path)
        self.assertIsInstance(result, StreamingBody)
        self.assertIsInstance(result.read(), bytes)

    def test_get_target_file_stata(self, raw_ledger=raw_stata_ledger):
        stata_path = raw_ledger["location_details"]
        result = file_to_csv.get_target_file(stata_path)
        self.assertIsInstance(result, StreamingBody)
        self.assertIsInstance(result.read(), bytes)

    def test_get_target_file_nons3_location(self):
        file_path = "HDFS://this/is/not/a/path.spss"
        self.assertRaises(ValueError, file_to_csv.get_target_file, file_path)

    def test_get_target_file_s3_invalid_file(self):
        file_path = "s3a://pseudo/a/fake/path.spss"
        self.assertRaises(ClientError, file_to_csv.get_target_file, file_path)

    def test_get_target_file_s3_invalid_bucket(self):
        file_path = "s3a://this/is/not/a/bucket.file"
        self.assertRaises(ClientError, file_to_csv.get_target_file, file_path)

    def test_convert_spss_main_df(self, raw_ledger=raw_spss_ledger):
        loaded_path = raw_ledger["location_details"]
        loaded_file = file_to_csv.get_target_file(loaded_path)
        result_df, other_dfs = file_to_csv.convert_spss(loaded_file, loaded_path)
        self.assertIsInstance(result_df, DataFrame)

    def test_convert_spss_value_df(self, raw_ledger=raw_spss_ledger):
        loaded_path = raw_ledger["location_details"]
        loaded_file = file_to_csv.get_target_file(loaded_path)
        result_df, other_dfs = file_to_csv.convert_spss(loaded_file, loaded_path)
        self.assertIsInstance(other_dfs, dict)
        for name, frame in other_dfs.items():
            self.assertIn(name, ["-values", "-description"])
            self.assertIsInstance(frame, DataFrame)
        val_df = other_dfs["-values"]
        self.assertCountEqual(val_df.columns, ["Field", "Code", "Meaning"])

    def test_convert_spss_desc_df(self, raw_ledger=raw_spss_ledger):
        loaded_path = raw_ledger["location_details"]
        loaded_file = file_to_csv.get_target_file(loaded_path)
        result_df, other_dfs = file_to_csv.convert_spss(loaded_file, loaded_path)
        self.assertIsInstance(other_dfs, dict)
        des_df = other_dfs["-description"]
        self.assertCountEqual(des_df.columns, ["Field", "Meaning"])

    def test_convert_spss_empty_sav(self):
        loaded_path = "s3a://pseudo/spss/empty.sav"
        loaded_file = file_to_csv.get_target_file(loaded_path)
        self.assertRaises(ReadstatError, file_to_csv.convert_spss, loaded_file, loaded_path)

    def test_convert_spss_corrupt_sav(self):
        loaded_path = "s3a://pseudo/spss/invalid_spss.sav"
        loaded_file = file_to_csv.get_target_file(loaded_path)
        self.assertRaises(ReadstatError, file_to_csv.convert_spss, loaded_file, loaded_path)

    def test_convert_stata_main_df(self, raw_ledger=raw_stata_ledger):
        loaded_path = raw_ledger["location_details"]
        loaded_file = file_to_csv.get_target_file(loaded_path)
        result_df, other_dfs = file_to_csv.convert_stata(loaded_file, loaded_path)
        self.assertIsInstance(result_df, DataFrame)

    def test_convert_stata_value_df(self, raw_ledger=raw_stata_ledger):
        loaded_path = raw_ledger["location_details"]
        loaded_file = file_to_csv.get_target_file(loaded_path)
        result_df, other_dfs = file_to_csv.convert_stata(loaded_file, loaded_path)
        self.assertIsInstance(other_dfs, dict)
        for name, frame in other_dfs.items():
            self.assertIn(name, ["-values"])
            self.assertIsInstance(frame, DataFrame)
        val_df = other_dfs["-values"]
        self.assertCountEqual(val_df.columns, ["Field", "Code", "Meaning"])

    def test_convert_stata_empty_dta(self):
        loaded_path = "s3a://pseudo/stata/empty.dta"
        loaded_file = file_to_csv.get_target_file(loaded_path)
        self.assertRaises(struct.error, file_to_csv.convert_stata, loaded_file, loaded_path)

    def test_convert_stata_corrupt_dta(self):
        loaded_path = "s3a://pseudo/stata/invalid_stata.dta"
        loaded_file = file_to_csv.get_target_file(loaded_path)
        self.assertRaises(ValueError, file_to_csv.convert_stata, loaded_file, loaded_path)

    def test_convert_stata_given_spss(self, ledger=raw_spss_ledger):
        loaded_path = ledger["location_details"]
        loaded_file = file_to_csv.get_target_file(loaded_path)
        self.assertRaises(ValueError, file_to_csv.convert_stata, loaded_file, loaded_path)

    def test_check_converted_file_spss(self, location=spss_converted_path, bucket_name=bucket):
        result = file_to_csv.check_converted_file(bucket_name, location)
        self.assertEqual(result, True)

    def test_check_converted_file_stata(self, location=stata_converted_path, bucket_name=bucket):
        result = file_to_csv.check_converted_file(bucket_name, location)
        self.assertEqual(result, True)

    def test_check_converted_file_missing(self, bucket_name=bucket):
        location = "test/not/a/path.csv"
        self.assertRaises(ClientError, file_to_csv.check_converted_file, bucket_name, location)

    def test_check_converted_file_wrong_bucket(self, location=spss_converted_path):
        bucket_name = "notabucket"
        self.assertRaises(ClientError, file_to_csv.check_converted_file, bucket_name, location)


if __name__ == "__main__":
    main()
