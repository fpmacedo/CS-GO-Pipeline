# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Purpose

Shows how to use the AWS SDK for Python (Boto3) with the Amazon EMR API to create
two kinds of clusters:
    * A short-lived cluster that runs a single step to estimate the value of pi and
      then automatically terminates.
    * A long-lived cluster that runs several steps to query for top-rated items from
      historical Amazon review data. The cluster is manually terminated at the end of
      the demo.

The demos also create several other AWS resources:
    * An Amazon S3 bucket to store demo scripts and data.
    * AWS Identity and Access Management (IAM) security roles used by the demo.
    * Amazon Elastic Compute Cloud (Amazon EC2) security groups used by the demo.
"""

import argparse
import logging
import json
import sys
import time

import boto3
from botocore.exceptions import ClientError

import emr_basics 



logger = logging.getLogger(__name__)


def status_poller(intro, done_status, func):
    """
    Polls a function for status, sleeping for 10 seconds between each query,
    until the specified status is returned.

    :param intro: An introductory sentence that informs the reader what we're
                  waiting for.
    :param done_status: The status we're waiting for. This function polls the status
                        function until it returns the specified status.
    :param func: The function to poll for status. This function must eventually
                 return the expected done_status or polling will continue indefinitely.
    """
    emr_basics.logger.setLevel(logging.WARNING)
    status = None
    print(intro)
    print("Current status: ", end='')
    while status != done_status:
        prev_status = status
        status = func()
        if prev_status == status:
            print('.', end='')
        else:
            print(status, end='')
        sys.stdout.flush()
        time.sleep(10)
    print()
    emr_basics.logger.setLevel(logging.INFO)


def setup_bucket(bucket_name, script_file_name, script_key, s3_resource):
    """
    Creates an Amazon S3 bucket and uploads the specified script file to it.

    :param bucket_name: The name of the bucket to create.
    :param script_file_name: The name of the script file to upload.
    :param script_key: The key of the script object in the Amazon S3 bucket.
    :param s3_resource: The Boto3 Amazon S3 resource object.
    :return: The newly created bucket.
    """
    try:
        bucket = s3_resource.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': s3_resource.meta.client.meta.region_name
            }
        )
        bucket.wait_until_exists()
        logger.info("Created bucket %s.", bucket_name)
    except ClientError:
        logger.exception("Couldn't create bucket %s.", bucket_name)
        raise

    try:
        bucket.upload_file(script_file_name, script_key)
        logger.info(
            "Uploaded script %s to %s.", script_file_name,
            f'{bucket_name}/{script_key}')
    except ClientError:
        logger.exception("Couldn't upload %s to %s.", script_file_name, bucket_name)
        raise

    return bucket


def delete_bucket(bucket):
    """
    Deletes all objects in the specified bucket and deletes the bucket.

    :param bucket: The bucket to delete.
    """
    try:
        bucket.objects.delete()
        bucket.delete()
        logger.info("Emptied and removed bucket %s.", bucket.name)
    except ClientError:
        logger.exception("Couldn't remove bucket %s.", bucket.name)
        raise


def create_roles(job_flow_role_name, service_role_name, iam_resource):
    """
    Creates IAM roles for the job flow and for the service.

    The job flow role is assumed by the cluster's Amazon EC2 instances and grants
    them broad permission to use services like Amazon DynamoDB and Amazon S3.

    The service role is assumed by Amazon EMR and grants it permission to use various
    Amazon EC2, Amazon S3, and other actions.

    For demo purposes, these roles are fairly permissive. In practice, it's more
    secure to restrict permissions to the minimum needed to perform the required
    tasks.

    :param job_flow_role_name: The name of the job flow role.
    :param service_role_name: The name of the service role.
    :param iam_resource: The Boto3 IAM resource object.
    :return: The newly created roles.
    """
    try:
        job_flow_role = iam_resource.create_role(
            RoleName=job_flow_role_name,
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2008-10-17",
                "Statement": [{
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "ec2.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                }]
            })
        )
        waiter = iam_resource.meta.client.get_waiter('role_exists')
        waiter.wait(RoleName=job_flow_role_name)
        logger.info("Created job flow role %s.", job_flow_role_name)
    except ClientError:
        logger.exception("Couldn't create job flow role %s.", job_flow_role_name)
        raise

    try:
        job_flow_role.attach_policy(
            PolicyArn=
            "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
        )
        logger.info("Attached policy to role %s.", job_flow_role_name)
    except ClientError:
        logger.exception("Couldn't attach policy to role %s.", job_flow_role_name)
        raise

    try:
        job_flow_inst_profile = iam_resource.create_instance_profile(
            InstanceProfileName=job_flow_role_name)
        job_flow_inst_profile.add_role(RoleName=job_flow_role_name)
        logger.info(
            "Created instance profile %s and added job flow role.", job_flow_role_name)
    except ClientError:
        logger.exception("Couldn't create instance profile %s.", job_flow_role_name)
        raise

    try:
        service_role = iam_resource.create_role(
            RoleName=service_role_name,
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2008-10-17",
                "Statement": [{
                        "Sid": "",
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "elasticmapreduce.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                }]
            })
        )
        waiter = iam_resource.meta.client.get_waiter('role_exists')
        waiter.wait(RoleName=service_role_name)
        logger.info("Created service role %s.", service_role_name)
    except ClientError:
        logger.exception("Couldn't create service role %s.", service_role_name)
        raise

    try:
        service_role.attach_policy(
            PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
        )
        logger.info("Attached policy to service role %s.", service_role_name)
    except ClientError:
        logger.exception(
            "Couldn't attach policy to service role %s.", service_role_name)
        raise

    return job_flow_role, service_role


def delete_roles(roles):
    """
    Deletes the roles created for this demo.

    :param roles: The roles to delete.
    """
    try:
        for role in roles:
            for policy in role.attached_policies.all():
                role.detach_policy(PolicyArn=policy.arn)
            for inst_profile in role.instance_profiles.all():
                inst_profile.remove_role(RoleName=role.name)
                inst_profile.delete()
            role.delete()
            logger.info("Detached policies and deleted role %s.", role.name)
    except ClientError:
        logger.exception("Couldn't delete roles %s.", [role.name for role in roles])
        raise


def create_security_groups(prefix, ec2_resource):
    """
    Creates Amazon EC2 security groups for the instances contained in the cluster.

    When the cluster is created, Amazon EMR adds all required rules to these
    security groups. Because this demo needs only the default rules, it creates
    empty security groups and lets Amazon EMR fill them in.

    :param prefix: The name prefix for the security groups.
    :param ec2_resource: The Boto3 Amazon EC2 resource object.
    :return: The newly created security groups.
    """
    try:
        default_vpc = list(ec2_resource.vpcs.filter(
            Filters=[{'Name': 'isDefault', 'Values': ['true']}]))[0]
        logger.info("Got default VPC %s.", default_vpc.id)
    except ClientError:
        logger.exception("Couldn't get VPCs.")
        raise
    except IndexError:
        logger.exception("No default VPC in the list.")
        raise

    groups = {'manager': None, 'worker': None}
    for group in groups.keys():
        try:
            groups[group] = default_vpc.create_security_group(
                GroupName=f'{prefix}-{group}', Description=f"EMR {group} group.")
            logger.info(
                "Created security group %s in VPC %s.",
                groups[group].id, default_vpc.id)
        except ClientError:
            logger.exception("Couldn't create security group.")
            raise

    return groups


def delete_security_groups(security_groups):
    """
    Deletes the security groups used by the demo. When there are dependencies
    on a security group, it cannot be deleted. Because it can take some time
    to release all dependencies after a cluster is terminated, this function retries
    the delete until it succeeds.

    :param security_groups: The security groups to delete.
    """
    try:
        for sg in security_groups.values():
            sg.revoke_ingress(IpPermissions=sg.ip_permissions)
        max_tries = 5
        while True:
            try:
                for sg in security_groups.values():
                    sg.delete()
                break
            except ClientError as error:
                max_tries -= 1
                if max_tries > 0 and \
                        error.response['Error']['Code'] == 'DependencyViolation':
                    logger.warning(
                        "Attempt to delete security group got DependencyViolation. "
                        "Waiting for 10 seconds to let things propagate.")
                    time.sleep(10)
                else:
                    raise
        logger.info("Deleted security groups %s.", security_groups)
    except ClientError:
        logger.exception("Couldn't delete security groups %s.", security_groups)
        raise



def demo_short_lived_cluster():
    """
    Shows how to create a short-lived cluster that runs a step and automatically
    terminates after the step completes.
    """
    print('-'*88)
    print(f"Welcome to the Amazon EMR short-lived cluster demo.")
    print('-'*88)

    prefix = f'demo-short-emr5'

    s3_resource = boto3.resource('s3')
    iam_resource = boto3.resource('iam')
    emr_client = boto3.client('emr')
    ec2_resource = boto3.resource('ec2')

    S3_BUCKET = 'fpmacedo'
    S3_KEY = 'spark/pyspark_script.py'
    S3_URI = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=S3_KEY)

    s3_resource.meta.client.upload_file("pyspark_script.py", S3_BUCKET, S3_KEY)

    # Set up resources for the demo.
    bucket_name = 'fpmacedo'
    # bucket_name = f'{prefix}-{time.time_ns()}'
    #script_file_name = 'pyspark_script.py'
    #script_key = f'scripts/{script_file_name}'
    #bucket = setup_bucket(bucket_name, script_file_name, script_key, s3_resource)
    job_flow_role='EMR_EC2_DefaultRole'
    service_role = 'EMR_DefaultRole'
    security_groups = create_security_groups(prefix, ec2_resource)

    # Run the job.
    pi_step = {
        'name': 'pyspark_test',
        'script_uri': S3_URI,
        'script_args':
            []
    }
    print("Wait for 10 seconds to give roles and profiles time to propagate...")
    time.sleep(10)
    max_tries = 5
    while True:
        try:
            cluster_id = emr_basics.run_job_flow(
                f'{prefix}-cluster', f's3://{bucket_name}/logs',
                False, ['Hadoop', 'Hive', 'Spark'], job_flow_role, service_role,
                security_groups, [pi_step], emr_client)
            print(f"Running job flow for cluster {cluster_id}...")
            break
        except ClientError as error:
            max_tries -= 1
            if max_tries > 0 and \
                    error.response['Error']['Code'] == 'ValidationException':
                print("Instance profile is not ready, let's give it more time...")
                time.sleep(10)
            else:
                raise

    status_poller(
        "Waiting for cluster, this typically takes several minutes...",
        'RUNNING',
        lambda: emr_basics.describe_cluster(cluster_id, emr_client)['Status']['State'],
    )
    status_poller(
        "Waiting for step to complete...",
        'PENDING',
        lambda: emr_basics.list_steps(cluster_id, emr_client)[0]['Status']['State'])
    status_poller(
        "Waiting for cluster to terminate.",
        'TERMINATED',
        lambda: emr_basics.describe_cluster(cluster_id, emr_client)['Status']['State']
    )

    print(f"Job complete!. The script, logs, and output for this demo are in "
          f"Amazon S3 bucket {bucket_name}. The output is:")
    #for obj in bucket.objects.filter(Prefix=output_prefix):
     #   print(obj.get()['Body'].read().decode())

    # Clean up demo resources (if you want to).
    remove_everything = input(
            f"Do you want to delete the security roles, groups, and bucket (y/n)? ")
    if remove_everything.lower() == 'y':
        delete_security_groups(security_groups)
        #delete_roles([job_flow_role, service_role])
        #delete_bucket(bucket)
    else:
        print(
            f"Remember that objects kept in an Amazon S3 bucket can incur charges"
            f"against your account.")
    print("Thanks for watching!")




if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    demo_short_lived_cluster()
  
