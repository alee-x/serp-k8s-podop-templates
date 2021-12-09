import json
import os
import time
import logging
import warnings
from unittest import TestCase, mock, main

from expectations.src import init_expects, s3_connector
from . import super_env


class Test(TestCase):
    bucket_name = "testbucket"
    project_code = "test_code"
    dag_run_id = "test0-10-11-2021"
    s3_ledger = dict(attributes={
        "project": "test_code",
        "expectation_json_result": "",
        "expectation_html_result": "",
        "failexpectation": "failexpectation",
        "schema": "",
        "metrics": "",
        "has_header": "true",
        "delimiter": ","
    }, dataset_id="pedw", label="spell", classification="demo", version="121", group_count="gcount", location="s3",
        location_details="s3a://sail0000v/pedw_spell.csv")
    ledger_s3_path = "ledger/path/ledge.json"

    @classmethod
    def setUpClass(cls, s3_ledger=s3_ledger, bucket_name=bucket_name, ledger_s3_path=ledger_s3_path):
        env_vars = super_env.get_s3_env() | super_env.get_nrda_env() | super_env.get_airflow_env()
        cls.env_patcher = mock.patch.dict(os.environ, env_vars)
        cls.env_patcher.start()
        s3_client = s3_connector.make_s3_client(s3_connector.get_conn_details())
        s3_connector.put_item(s3_ledger, bucket_name, ledger_s3_path, s3_client)
        logging.disable(logging.CRITICAL)
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
        super().setUpClass()

    @classmethod
    def tearDownClass(cls, bucket_name=bucket_name, project_code=project_code, dag_run_id=dag_run_id):
        super().tearDownClass()
        s3_client = s3_connector.make_s3_client(s3_connector.get_conn_details())
        all_objs = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in all_objs:
            for item in all_objs['Contents']:
                s3_client.delete_object(Bucket=bucket_name, Key=item['Key'])
        s3_client.delete_bucket(
            Bucket=bucket_name
        )
        cls.env_patcher.stop()

    def test_get_expectations(self, ledger=s3_ledger):
        self.assertIsInstance(init_expects.get_expectations(ledger), list)
        self.assertIsInstance(init_expects.get_expectations(ledger)[0], dict)

    def test_get_expectations_invalid_url(self, ledger=s3_ledger):
        ledger['dataset_id'] = "afakedataset"
        self.assertRaises(ValueError, init_expects.get_expectations, ledger)

    def test_expectations_init(self, bucket_name=bucket_name, project_code=project_code, dag_run_id=dag_run_id,
                               ledger_s3_path=ledger_s3_path):
        s3_expect_json_path = "{0}/jobs/{1}/expects.json".format(project_code, dag_run_id)
        s3_client = s3_connector.make_s3_client(s3_connector.get_conn_details())
        result = init_expects.expectations_init(bucket_name, project_code, ledger_s3_path, dag_run_id)
        self.assertEqual(result, s3_expect_json_path)

        # check if the existing expectations got put on s3
        existing_expects = json.loads(s3_client.get_object(Bucket=bucket_name, Key=result)["Body"].read())
        self.assertIsInstance(existing_expects['expectations'], list)
        self.assertIsInstance(existing_expects['expectations'][0], dict)
        self.assertEqual(existing_expects['expectations'][0]['expectation_type'],
                         'expect_table_columns_to_match_ordered_list')

    def test_generate_empty_expects(self, bucket_name=bucket_name, dag_run_id=dag_run_id, project_code=project_code):
        expect_suite_form = "{0}/jobs/{1}/ge_tmp/expectations/{1}.json".format(project_code, dag_run_id)
        expect_store_form = "{0}/jobs/{1}/ge_tmp/expectations".format(project_code, dag_run_id)
        expect_validations_form = "{0}/jobs/{1}/ge_tmp/uncommitted/validations".format(project_code, dag_run_id)
        expect_sitebuilder_form = "{0}/jobs/{1}/ge_tmp/uncommitted/data_docs/local_site".format(project_code,
                                                                                                dag_run_id)

        expect_suite_s3_loc, expect_store_s3_loc, expect_validations_s3_loc, expect_sitebuilder_s3_loc = \
            init_expects.get_empty_expect_suite(bucket_name, dag_run_id, project_code)

        self.assertEqual(expect_suite_s3_loc, expect_suite_form)
        self.assertEqual(expect_store_form, expect_store_s3_loc)
        self.assertEqual(expect_validations_form, expect_validations_s3_loc)
        self.assertEqual(expect_sitebuilder_form, expect_sitebuilder_s3_loc)

    def test_add_exp_to_suite(self, bucket=bucket_name, dag_run_id=dag_run_id, project_code=project_code):
        time.sleep(15)
        expect_suite_form = "{0}/jobs/{1}/ge_tmp/expectations/{1}.json".format(project_code, dag_run_id)
        s3_expect_json_path = "{0}/jobs/{1}/expects.json".format(project_code, dag_run_id)
        s3_client = s3_connector.make_s3_client(s3_connector.get_conn_details())

        self.assertTrue(init_expects.add_expectations_to_suite(bucket, expect_suite_form, s3_expect_json_path))

        # check the expect suite is not empty
        result = json.loads(s3_client.get_object(Bucket=bucket, Key=expect_suite_form)["Body"].read())
        eesuite = json.loads(s3_client.get_object(Bucket=bucket, Key=s3_expect_json_path)["Body"].read())
        self.assertEqual(result['expectation_suite_name'], dag_run_id)
        self.assertEqual(result['expectations'], eesuite['expectations'])


if __name__ == "__main__":
    main()
