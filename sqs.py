import json

import boto3
import click

import mysqlbackup

sqs = boto3.resource('sqs')


@click.command()
@click.option('--queue-url', required=True)
def work(queue_url):
    queue = sqs.Queue(queue_url)
    messages = queue.receive_messages(MaxNumberOfMessages=1)
    while len(messages) > 0:
        for message in messages:
            mysqlbackup.backup(**json.loads(message.body))
            message.delete()


if __name__ == '__main__':
    work(auto_envvar_prefix='MYSQLBACKUP')
