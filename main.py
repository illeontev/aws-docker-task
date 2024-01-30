import argparse
import logging
import sys
import threading
import time
from collections import deque

import boto3
import docker
import docker.errors

LOGS_BATCH_SIZE = 10
LOGS_SEND_INTERVAL = 10

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def parse_arguments():
    parser = argparse.ArgumentParser(description="Docker and AWS CloudWatch Integration")
    parser.add_argument("--docker-image", required=True, help="Name of the Docker image")
    parser.add_argument("--bash-command", required=True, help="Bash command to run inside the Docker image")
    parser.add_argument("--aws-cloudwatch-group", required=True, help="Name of the AWS CloudWatch group")
    parser.add_argument("--aws-cloudwatch-stream", required=True, help="Name of the AWS CloudWatch stream")
    parser.add_argument("--aws-access-key-id", required=True, help="AWS Access Key ID")
    parser.add_argument("--aws-secret-access-key", required=True, help="AWS Secret Access Key")
    parser.add_argument("--aws-region", required=True, help="Name of the AWS region")
    return parser.parse_args()


def create_cloudwatch_client(access_key, secret_key, region):
    return boto3.client("logs", aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region)


def ensure_cloudwatch_group_and_stream(client, group_name, stream_name):
    try:
        client.create_log_group(logGroupName=group_name)
    except client.exceptions.ResourceAlreadyExistsException:
        pass

    try:
        client.create_log_stream(logGroupName=group_name, logStreamName=stream_name)
    except client.exceptions.ResourceAlreadyExistsException:
        pass


def send_logs_to_cloudwatch(client, group_name, stream_name, log_queue):
    timestamp = int(time.time() * 1000)
    log_events = [{"timestamp": timestamp, "message": log} for log in log_queue]
    log_queue.clear()

    client.put_log_events(logGroupName=group_name, logStreamName=stream_name, logEvents=log_events)


def log_reader(container, log_queue):
    try:
        for log in container.logs(stream=True):
            log_message = log.decode("utf-8").strip()
            logger.info(log_message)
            log_queue.append(log_message)
    except Exception as e:
        logger.error(f"Error reading logs from container: {e}")


def log_sender(cloudwatch_client, group_name, stream_name, log_queue, send_interval):
    while True:
        time.sleep(send_interval)
        if log_queue:
            try:
                send_logs_to_cloudwatch(cloudwatch_client, group_name, stream_name, log_queue)
            except Exception as e:
                logger.error(f"Error sending logs to CloudWatch: {e}")


def run_docker_container(image, command, cloudwatch_client, group_name, stream_name):
    try:
        docker_client = docker.from_env()
        container = docker_client.containers.run(
            image, ["sh", "-c", command], detach=True, stdout=True, stderr=True, stream=True
        )

        log_queue = deque()

        reader_thread = threading.Thread(target=log_reader, args=(container, log_queue))
        reader_thread.start()

        sender_thread = threading.Thread(
            target=log_sender, args=(cloudwatch_client, group_name, stream_name, log_queue, LOGS_SEND_INTERVAL)
        )
        sender_thread.start()

        reader_thread.join()
        sender_thread.join()
    except docker.errors.DockerException as e:
        logger.error(f"Error with Docker operation: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in run_docker_container: {e}")


def main():
    args = parse_arguments()

    cloudwatch_client = create_cloudwatch_client(args.aws_access_key_id, args.aws_secret_access_key, args.aws_region)
    ensure_cloudwatch_group_and_stream(cloudwatch_client, args.aws_cloudwatch_group, args.aws_cloudwatch_stream)

    try:
        run_docker_container(
            args.docker_image,
            args.bash_command,
            cloudwatch_client,
            args.aws_cloudwatch_group,
            args.aws_cloudwatch_stream,
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
