import time
import logging
import boto3
from botocore.exceptions import ClientError

# Configure AWS region and resource details
AWS_REGION = "us-east-1"
ID = "YourID"
IAM = "IAMNumber"
REQ_QUEUE_NAME = f"{ID}-req-queue"
MAX_INSTANCES = 15
GRACE_PERIOD = 5

# Initialize AWS clients
sqs = boto3.client("sqs", region_name=AWS_REGION)
ec2 = boto3.client("ec2", region_name=AWS_REGION)
ec2_resource = boto3.resource("ec2", region_name=AWS_REGION)

# SQS request queue URL
REQ_QUEUE_URL = f"https://sqs.{AWS_REGION}.amazonaws.com/{IAM}/{REQ_QUEUE_NAME}"

# Tracking for scaling decisions
last_request_time = time.time()
over_capacity_start = None


def get_queue_size():
    """Fetches the estimated number of pending messages in the SQS request queue."""
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=REQ_QUEUE_URL, AttributeNames=["ApproximateNumberOfMessages"]
        )
        return int(response["Attributes"].get("ApproximateNumberOfMessages", 0))
    except ClientError as error:
        print(f"Failed to retrieve queue size: {error}")
        return 0


def get_application_instances():
    """Retrieves EC2 instances associated with the application tier."""
    filters = [
        {"Name": "tag:Name", "Values": ["app-tier-instance-*"]},
        {"Name": "instance-state-name", "Values": ["stopped", "pending", "running"]},
    ]
    return list(ec2_resource.instances.filter(Filters=filters))


def manage_scaling():
    """Monitors the queue and adjusts EC2 instances accordingly."""
    global last_request_time, over_capacity_start
    logging.info("Autoscaling service initiated")

    while True:
        queue_size = get_queue_size()
        instances = get_application_instances()
        running_instances = [
            inst for inst in instances if inst.state["Name"] in ["running", "pending"]
        ]
        stopped_instances = [
            inst for inst in instances if inst.state["Name"] == "stopped"
        ]

        if queue_size > 0:
            last_request_time = time.time()

        required_instances = min(queue_size, MAX_INSTANCES)
        print(f"Target active instances: {required_instances}")

        if len(running_instances) < required_instances:
            instances_to_start = required_instances - len(running_instances)
            if instances_to_start > 0 and stopped_instances:
                instance_ids = [
                    inst.id for inst in stopped_instances[:instances_to_start]
                ]
                try:
                    ec2.start_instances(InstanceIds=instance_ids)
                except ClientError as error:
                    print(f"Failed to start instances {instance_ids}: {error}")
            over_capacity_start = None

        elif len(running_instances) > required_instances:
            if over_capacity_start is None:
                over_capacity_start = time.time()
            elif time.time() - over_capacity_start >= GRACE_PERIOD:
                excess_instances = running_instances[required_instances:]
                instance_ids = [inst.id for inst in excess_instances]
                if instance_ids:
                    try:
                        ec2.stop_instances(InstanceIds=instance_ids)
                    except ClientError as error:
                        print(f"Failed to stop instances {instance_ids}: {error}")

        else:
            over_capacity_start = None

        time.sleep(2)


if __name__ == "__main__":
    manage_scaling()
