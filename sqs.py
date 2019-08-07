import json

import boto3
import click

import mysqlbackup


@click.command()
@click.option('--queue-url', required=True)
@click.option('--region')
def work(queue_url, region):
    sqs = boto3.resource('sqs', region_name=region)
    queue = sqs.Queue(queue_url)
    messages = queue.receive_messages(MaxNumberOfMessages=1,
                                      WaitTimeSeconds=10)
    for message in messages:
        mysqlbackup.backup(**json.loads(message.body))
        message.delete()


if __name__ == '__main__':
    work(auto_envvar_prefix='MYSQLBACKUP')
