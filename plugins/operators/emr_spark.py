# pylint: disable=import-error
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import emr_job_flow


class EMRSparkOperator(BaseOperator):

    ui_color = '#FF9933'

    @apply_defaults
    def __init__( self,
                  emr_cluster_prefix="",
                  s3_script_key="",
                  local_script_key="",
                  bucket_name="",
                  region_name="",
                  aws_access_key_id="",
                  aws_secret_access_key="",                  
                 *args, **kwargs):

        super(EMRSparkOperator, self).__init__(*args, **kwargs)
        self.emr_cluster_prefix = emr_cluster_prefix
        self.s3_script_key = s3_script_key
        self.local_script_key = local_script_key
        self.bucket_name = bucket_name
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def execute(self, context):
      emr_job_flow.demo_short_lived_cluster(self.emr_cluster_prefix, \
      self.bucket_name, self.s3_script_key, self.local_script_key, \
      self.region_name, self.aws_access_key_id, self.aws_secret_access_key)