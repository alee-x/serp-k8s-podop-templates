"""Great Expectations Base Config builder

This defines the configuration for the empty Great Expectations DataContext.
It ensures that the GE Suite is configured to load Spark DataFrames to apply existing expectations, and also that the
backend store for the Expectation Suite is a location on S3 that is defined in the DAG run configuration.

"""
from great_expectations.data_context.types.base import DataContextConfig
from . import s3_connector


def get_expect_config(bucket_name, project_code, dag_run_id) -> DataContextConfig:
    """Get the configuration for a Great Expectations suite

    Set up the configuration for a Great Expectations suite to use Spark DataFrames as a data type to run expectations
    on and an S3 bucket to use as a temporary storage location for the results of the expectation suite.

    :param bucket_name: the name of the S3 bucket to use as temporary storage of the blank expectation suite and the
        suite results.
    :type bucket_name: str
    :param project_code: the project code of the dataset that the expectations will be applied to, used in formatting
        the path in which the expectation suite will be saved.
    :type project_code: int or str
    :param dag_run_id: the run_id of the DAG, used to format the expectation suite storage path to ensure that stored
        items are unique and won't be overwritten by other DAGs.
    :type dag_run_id: int or str
    :return: configured DataContextConfig object
    :rtype: DataContextConfig
    """
    s3_conn = s3_connector.get_conn_details()

    config = DataContextConfig(
        config_version=2,
        plugins_directory=None,
        config_variables_file_path=None,

        datasources={
            "my_spark_datasource": {
                "data_asset_type": {
                    "class_name": "SparkDFDataset",
                    "module_name": "great_expectations.dataset",
                },
                "class_name": "SparkDFDatasource",
                "module_name": "great_expectations.datasource",
                "batch_kwargs_generators": {},
            }
        },
        stores={
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": bucket_name,
                    "prefix": "{0}/jobs/{1}/ge_tmp/expectations/".format(project_code, dag_run_id),
                    "endpoint_url": s3_conn['endpoint'],
                    "boto3_options": {
                        "aws_access_key_id": s3_conn['access_key'],
                        "aws_secret_access_key": s3_conn['access_secret'],
                        "endpoint_url": s3_conn['endpoint'],
                        "signature_version": "s3v4"
                    }
                },
            },
            "validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": bucket_name,
                    "prefix": "{0}/jobs/{1}/ge_tmp/uncommitted/validations/".format(project_code, dag_run_id),
                    "endpoint_url": s3_conn['endpoint'],
                    "boto3_options": {
                        "aws_access_key_id": s3_conn['access_key'],
                        "aws_secret_access_key": s3_conn['access_secret'],
                        "endpoint_url": s3_conn['endpoint'],
                        "signature_version": "s3v4"
                    }
                },
            },
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
        },
        expectations_store_name="expectations_store",
        validations_store_name="validations_store",
        evaluation_parameter_store_name="evaluation_parameter_store",
        data_docs_sites={
            "local_site": {
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": bucket_name,
                    "prefix": "{0}/jobs/{1}/ge_tmp/uncommitted/data_docs/local_site/".format(project_code, dag_run_id),
                    "endpoint_url": s3_conn['endpoint'],
                    "boto3_options": {
                        "aws_access_key_id": s3_conn['access_key'],
                        "aws_secret_access_key": s3_conn['access_secret'],
                        "endpoint_url": s3_conn['endpoint'],
                        "signature_version": "s3v4"
                    }
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                    "show_cta_footer": True,
                },
            }
        },
        validation_operators={
            "action_list_operator": {
                "class_name": "ActionListValidationOperator",
                "action_list": [
                    {
                        "name": "store_validation_result",
                        "action": {"class_name": "StoreValidationResultAction"},
                    },
                    {
                        "name": "store_evaluation_params",
                        "action": {"class_name": "StoreEvaluationParametersAction"},
                    },
                    {
                        "name": "update_data_docs",
                        "action": {"class_name": "UpdateDataDocsAction"},
                    },
                ],
            }
        },
        anonymous_usage_statistics={
            "enabled": True
        }
    )
    return config
