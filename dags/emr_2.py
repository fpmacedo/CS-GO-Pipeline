import boto3    

client = boto3.client('emr', region_name='us-east-1')

S3_BUCKET = 'fpmacedo'
S3_KEY = 'spark/main.py'
S3_URI = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=S3_KEY)

# upload file to an S3 bucket
s3 = boto3.resource('s3',region_name='us-west-2', aws_access_key_id='AKIAXBIZ76BFXT53FPQG',
                       aws_secret_access_key='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')

s3.meta.client.upload_file("calc_pi.py", S3_BUCKET, S3_KEY)

client = boto3.client('emr', region_name='us-west-2', aws_access_key_id='AKIAXBIZ76BF6X6GJ24O',
                       aws_secret_access_key='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')
response = client.run_job_flow(
    Name="My Spark Cluster",
    ReleaseLabel='emr-4.6.0',
    Instances={
        'MasterInstanceType': 'm4.xlarge',
        'SlaveInstanceType': 'm4.xlarge',
        'InstanceCount': 4,
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    Applications=[
        {
            'Name': 'Spark'
        }
    ],
    BootstrapActions=[
        {
            'Name': 'Maximize Spark Default Config',
            'ScriptBootstrapAction': {
                'Path': 's3://support.elasticmapreduce/spark/maximize-spark-default-config',
            }
        },
    ],
    Steps=[
    {
        'Name': 'Run Spark',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/main.py']
        }
    }
    ],
    VisibleToAllUsers=True,
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole'
)