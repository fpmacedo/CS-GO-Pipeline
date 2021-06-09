# pylint: disable=import-error
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LocalToS3Operator(BaseOperator):

    ui_color = '#FF9933'

    @apply_defaults
    def __init__( self,
                  filepath="",
                  filename="",
                  key="",
                  aws_credentials_id="",
                  bucket_name=None,
                 *args, **kwargs):

        super(LocalToS3Operator, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.filename = filename
        self.key = key
        self.bucket_name = bucket_name
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        self.log.info('LocalToS3Operator not implemented yet')
        s3 = S3Hook(aws_conn_id=self.aws_credentials_id)

        
        if s3.check_for_key(key="{}/{}".format(self.key,self.filename), bucket_name=self.bucket_name):
            self.log.info("File Already exists.")
            self.log.info("Overwriting file to S3 in the destination s3://{}/{}/{}".format(self.bucket_name, self.key, self.filename))
            s3.load_file(filename="{}/{}".format(self.filepath, self.filename), key="{}/{}".format(self.key,self.filename), bucket_name=self.bucket_name, replace=True)
        else:
            self.log.info("Uploading file to S3 in the destination s3://{}/{}/{}".format(self.bucket_name, self.key, self.filename))
            s3.load_file(filename="{}/{}".format(self.filepath, self.filename), key="{}/{}".format(self.key,self.filename), bucket_name=self.bucket_name)