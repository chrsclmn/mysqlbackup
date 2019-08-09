import datetime
import re
import subprocess
import tempfile

import click
import pymysql


def backup(host, user, password, s3_bucket, s3_prefix='', port=3306,
           include=(), exclude=(), include_re=(), exclude_re=()):
    if s3_prefix:
        s3_prefix = '/' + s3_prefix.strip('/')
        if s3_prefix == '/':
            s3_prefix = ''
    now = datetime.datetime.utcnow()
    s3uri = f's3://{s3_bucket}{s3_prefix}/{host}/{now.year}/{now.month:02}/{now.day:02}' # noqa
    with tempfile.NamedTemporaryFile(mode='w') as cnf:
        cnf.write(f'[client]\npassword="{password}"\n')
        cnf.flush()
        con = pymysql.connect(host=host, user=user, passwd=password)
        cur = con.cursor()
        cur.execute('show databases')
        for row in cur:
            db = row[0]
            if db in {'information_schema', 'innodb', 'mysql',
                      'performance_schema', 'sys', 'tmp'}:
                continue
            if db in exclude:
                continue
            if any([re.match(r, db) for r in exclude_re]):
                continue
            if include and db not in include:
                continue
            if include_re and not any([re.match(r, db) for r in include_re]):
                continue
            mysqldump = subprocess.Popen([
                'mysqldump',
                f'--defaults-file={cnf.name}',
                f'--host={host}',
                f'--port={port}',
                f'--user={user}',
                '--single-transaction',
                '--databases',
                db
            ], stdout=subprocess.PIPE)
            lz4 = subprocess.Popen([
                'lz4', '-c'
            ], stdin=mysqldump.stdout, stdout=subprocess.PIPE)
            aws = subprocess.run([
                'aws', 's3', 'cp', '-', f'{s3uri}/{db}.sql.lz4'
            ], stdin=lz4.stdout, stderr=subprocess.PIPE)
            if aws.returncode != 0:
                raise Exception(aws.stderr)


@click.command()
@click.option('--host', required=True)
@click.option('--port', default=3306, type=int)
@click.option('--user', required=True)
@click.option('--password', required=True)
@click.option('--include',  multiple=True)
@click.option('--include-re',  multiple=True)
@click.option('--exclude',  multiple=True)
@click.option('--exclude-re',  multiple=True)
@click.option('--s3-bucket', required=True)
@click.option('--s3-prefix', default='')
def backup_command(**kwargs):
    return backup(**kwargs)


if __name__ == '__main__':
    backup_command(auto_envvar_prefix='MYSQLBACKUP')
