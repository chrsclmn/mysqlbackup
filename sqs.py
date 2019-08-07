import json

import boto3
import click

import mysqlbackup


@click.command()
@click.option('--queue-url', required=True)
@click.option('--region')
@click.option('--max-number-of-messages', type=(int), default=1)
@click.option('--visibility-timeout', type=(int), default=300)
@click.option('--wait-time-seconds', type=(int), default=10)
def work(queue_url, region, max_number_of_messages, visibility_timeout,
         wait_time_seconds):
    sqs = boto3.resource('sqs', region_name=region)
    queue = sqs.Queue(queue_url)
    messages = queue.receive_messages(
        MaxNumberOfMessages=max_number_of_messages,
        VisibilityTimeout=visibility_timeout,
        WaitTimeSeconds=wait_time_seconds
    )
    for message in messages:
        mysqlbackup.backup(**json.loads(message.body))
        message.delete()


if __name__ == '__main__':
    work(auto_envvar_prefix='MYSQLBACKUP')
