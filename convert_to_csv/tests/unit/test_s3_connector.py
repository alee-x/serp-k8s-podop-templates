import os
from unittest import TestCase, mock, main

from botocore.client import BaseClient

from expectations.src import s3_connector
from . import super_env


class Test(TestCase):
    new_bucket = "notabucket"
    existing_bucket = "sail0000v"
    target_path = "not/a/path"

    @classmethod
    def setUpClass(cls):
        env_vars = super_env.get_s3_env()
        cls.env_patcher = mock.patch.dict(os.environ, env_vars)
        cls.env_patcher.start()
        super().setUpClass()

    @classmethod
    def tearDownClass(cls, new_bucket=new_bucket, existing_bucket=existing_bucket, target_path=target_path):
        super().tearDownClass()
        s3_client = s3_connector.make_s3_client(s3_connector.get_conn_details())
        # delete from the not-a-bucket
        s3_client.delete_object(
            Bucket=new_bucket,
            Key=target_path
        )
        s3_client.delete_bucket(
            Bucket=new_bucket
        )
        s3_client.delete_object(
            Bucket=existing_bucket,
            Key=target_path
        )
        cls.env_patcher.stop()

    def test_get_conn_details(self):
        env_vars = super_env.get_s3_env()
        result = s3_connector.get_conn_details()
        self.assertEqual(result["endpoint"], env_vars["S3_ENDPOINT"])
        self.assertEqual(result["access_key"], env_vars["S3_ACCESS_KEY"])
        self.assertEqual(result["access_secret"], env_vars["S3_ACCESS_SECRET"])

    def test_make_s3_client_invalid_endpoint(self):
        params = {
            "endpoint": "inv",
            "access_key": "wrong",
            "access_secret": "sixth"
        }
        with self.assertRaises(ValueError):
            result = s3_connector.make_s3_client(params)

    def test_make_s3_client_no_param(self):
        result = s3_connector.make_s3_client()
        self.assertIsInstance(result, BaseClient)

    def test_put_item_existing_bucket(self, bucket_name=existing_bucket, target_path=target_path):
        object_to_put = {"stuff": "nonsense"}
        s3_client = s3_connector.make_s3_client(s3_connector.get_conn_details())
        self.assertTrue(s3_connector.put_item(object_to_put, bucket_name, target_path, s3_client))

    def test_put_item_nonexist_bucket_invalid_client(self, bucket_name=new_bucket, target_path=target_path):
        object_to_put = {"stuff": "nonsense"}
        s3_client = s3_connector.make_s3_client(s3_connector.get_conn_details())
        self.assertTrue(s3_connector.put_item(object_to_put, bucket_name, target_path, s3_client))


if __name__ == "__main__":
    main()
