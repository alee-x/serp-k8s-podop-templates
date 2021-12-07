from unittest import TestCase, mock, main
from expectations.src import conn_secrets
import os
from . import super_env


class Test(TestCase):
    @classmethod
    def setUpClass(cls):
        env_vars = super_env.get_nrda_env() | super_env.get_airflow_env()
        cls.env_patcher = mock.patch.dict(os.environ, env_vars)
        cls.env_patcher.start()
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.env_patcher.stop()

    def test_get_nrda_conn(self):
        env_var = super_env.get_nrda_env()
        result = conn_secrets.get_nrda_conn()
        self.assertEqual(result["host"], env_var["NRDAPI_HOST"])

    def test_get_airflow_auth_conn(self):
        env_var = super_env.get_airflow_env()
        result = conn_secrets.get_airflow_auth_conn()
        self.assertEqual(result["host"], env_var["AFOAUTH_HOST"])
        self.assertEqual(result["login"], env_var["AFOAUTH_LOGIN"])
        self.assertEqual(result["password"], env_var["AFOAUTH_PASS"])

    @mock.patch.dict(os.environ, {"FROBNICATION_COLOUR": "ROUGE"}, clear=True)
    def test_get_nrda_conn_no_env(self):
        self.assertRaises(KeyError, conn_secrets.get_nrda_conn)

    @mock.patch.dict(os.environ, {"FROBNICATION_COLOUR": "ROUGE"}, clear=True)
    def test_get_af_conn_no_env(self):
        self.assertRaises(KeyError, conn_secrets.get_airflow_auth_conn)


if __name__ == "__main__":
    main()
