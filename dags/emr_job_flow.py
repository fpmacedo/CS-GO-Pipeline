# SPDX-License-Identifier: Apache-2.0

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
                elif error.response['Error']['Code'] == 'InvalidGroup.NotFound':
                    logger.info("Group not found: %s.", sg)
                    break
                
                else:
                    raise
        logger.info("Deleted security groups %s.", security_groups)
    except ClientError:
        logger.exception("Couldn't delete security groups: %s.", security_groups)
        raise


def demo_short_lived_cluster(PREFIX, S3_BUCKET, S3_KEY, LOCAL_SCRIPT_KEY, REGION, AWS_ACESS_KEY, AWS_SECRET):
    """
    Create a short-lived cluster that runs a step and automatically
    terminates after the step completes.

    :param PREFIX: The prefix to use in the EMR cluster and security groups creation.
    :param S3_BUCKET: The name of the S3 Bucket used.
    :param S3_KEY: The key where the PySpark script will be uploaded.
    :param LOCAL_SCRIPT_KEY: The key where the PySpark script is alocated locally.
    """
    print('-'*88)
    print(f"Welcome to the Amazon EMR short-lived cluster.")
    print('-'*88)

    #prefix = f'CSGO-PIPELINE-EMR-CLUSTER'
    prefix = PREFIX

    

    s3_resource = boto3.resource('s3',
                       region_name = REGION,
                       aws_access_key_id= AWS_ACESS_KEY,
                       aws_secret_access_key= AWS_SECRET)

    iam_resource = boto3.resource('iam',
                       region_name = REGION,
                       aws_access_key_id= AWS_ACESS_KEY,
                       aws_secret_access_key= AWS_SECRET)

    emr_client = boto3.client('emr',
                       region_name = REGION,
                       aws_access_key_id= AWS_ACESS_KEY,
                       aws_secret_access_key= AWS_SECRET)

    ec2_resource = boto3.resource('ec2',
                       region_name = REGION,
                       aws_access_key_id= AWS_ACESS_KEY,
                       aws_secret_access_key= AWS_SECRET)

   
    S3_URI = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=S3_KEY)

    s3_resource.meta.client.upload_file(LOCAL_SCRIPT_KEY, S3_BUCKET, S3_KEY)
    
    
    # Set up resources.
    bucket_name = S3_BUCKET

    #get the local time
    named_tuple = time.localtime() 
    time_string = time.strftime("%m.%d.%Y-%Hh%Mm%Ss", named_tuple)

    job_flow_role, service_role = create_roles(f'{time_string}-{prefix}-ec2-role', f'{time_string}-{prefix}-service-role', iam_resource)

    security_groups = create_security_groups(f'{time_string}-{prefix}', ec2_resource)

    # Run the job.
    step = {
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
                security_groups, [step], emr_client)
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

    print(f"Job complete!. The script, logs, and output are in "
          f"Amazon S3 bucket {bucket_name}/logs.")

    #steps = emr_basics.list_steps(cluster_id, emr_client)

    #for step in steps:
    #   print(emr_basics.describe_step(cluster_id, step, emr_client))

    
    delete_security_groups(security_groups)
    delete_roles([job_flow_role, service_role])




#if __name__ == '__main__':
#    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
#    demo_short_lived_cluster("CS_GO_PIPELINE", "fpmacedo", "spark/pyspark_script.py", "pyspark_script.py")