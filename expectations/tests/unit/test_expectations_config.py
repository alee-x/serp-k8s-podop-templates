from expectations.src import expectations_config as expect_config
from . import super_env
import os
from great_expectations.data_context.types.base import DataContextConfig
from unittest import TestCase, mock, main


class Test(TestCase):
    bucket_name = "test_bucket"
    project_code = "test_code"
    dag_run_id = "test0-10-11-2021"

    @classmethod
    def setUpClass(cls):
        env_vars = super_env.get_s3_env()
        cls.env_patcher = mock.patch.dict(os.environ, env_vars)
        cls.env_patcher.start()
        super().setUpClass()

    def test_get_expect_config(self, bucket_name=bucket_name, project_code=project_code, dag_run_id=dag_run_id):
        """
        Test that it returns an object of type DataContextConfig
        :param bucket_name:
        :param project_code:
        :param dag_run_id:
        :return:
        """
        result = expect_config.get_expect_config(bucket_name, project_code, dag_run_id)
        self.assertEqual(type(result), DataContextConfig)

    def test_correct_expect_store(self, bucket_name=bucket_name, project_code=project_code, dag_run_id=dag_run_id):
        """
        Test that it returns the correct configurations for the expectation store, given the passed params.
        :param bucket_name:
        :param project_code:
        :param dag_run_id:
        :return:
        """
        result = expect_config.get_expect_config(bucket_name, project_code, dag_run_id)
        env_vars = super_env.get_s3_env()
        expectation_store_config = result.stores['expectations_store']

        self.assertEqual(expectation_store_config['store_backend']['class_name'], "TupleS3StoreBackend")
        self.assertEqual(expectation_store_config['store_backend']['bucket'], bucket_name)
        self.assertEqual(expectation_store_config['store_backend']['prefix'],
                         "{0}/jobs/{1}/ge_tmp/expectations/".format(project_code, dag_run_id))
        self.assertEqual(expectation_store_config['store_backend']['endpoint_url'], env_vars["S3_ENDPOINT"])
        self.assertEqual(expectation_store_config['store_backend']['boto3_options']['aws_access_key_id'],
                         env_vars["S3_ACCESS_KEY"])
        self.assertEqual(expectation_store_config['store_backend']['boto3_options']['aws_secret_access_key'],
                         env_vars["S3_ACCESS_SECRET"])

    def test_correct_val_store(self, bucket_name=bucket_name, project_code=project_code, dag_run_id=dag_run_id):
        """
        Test that it returns the correct configuration for the validation store, given the passed params.
        :param bucket_name:
        :param project_code:
        :param dag_run_id:
        :return:
        """
        result = expect_config.get_expect_config(bucket_name, project_code, dag_run_id)
        val_store_config = result.stores['validations_store']

        self.assertEqual(val_store_config['store_backend']['bucket'], bucket_name)
        self.assertEqual(val_store_config['store_backend']['prefix'],
                         "{0}/jobs/{1}/ge_tmp/uncommitted/validations/".format(project_code, dag_run_id))


if __name__ == "__main__":
    main()
