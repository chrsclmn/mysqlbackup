import datetime
import subprocess
import tempfile

import click
import pymysql


@click.command()
@click.option('--host', required=True)
@click.option('--port', default=3306, type=int)
@click.option('--user', required=True)
@click.option('--password', required=True)
@click.option('--include',  multiple=True)
@click.option('--exclude',  multiple=True)
@click.option('--s3-bucket', required=True)
@click.option('--s3-prefix', default='')
def backup(host, port, user, password, include, exclude, s3_bucket, s3_prefix):
    with tempfile.NamedTemporaryFile(mode='w') as cnf:
        cnf.write('[client]\n'
                  f'host={host}\n'
                  f'port={port}\n'
                  f'user={user}\n'
                  f'password="{password}"\n'
                  '\n'
                  '[mysqldump]\n'
                  'single-transaction\n')
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
            if include and db not in include:
                continue
            date = datetime.datetime.utcnow().isoformat().split('T')[0]
            if s3_prefix and not s3_prefix.startswith('/'):
                s3_prefix = '/' + s3_prefix
            s3uri = f's3://{s3_bucket}{s3_prefix}/{host}/{date}/{db}.sql.lz4'
            mysqldump = subprocess.Popen(['mysqldump',
                                          '--defaults-file=' + cnf.name,
                                          '--databases', db],
                                         stdout=subprocess.PIPE)
            lz4 = subprocess.Popen(['lz4', '-c'], stdin=mysqldump.stdout,
                                   stdout=subprocess.PIPE)
            aws = subprocess.run(['aws', 's3', 'cp', '-', s3uri],
                                 stdin=lz4.stdout)
            if aws.returncode != 0:
                break


if __name__ == '__main__':
    backup(auto_envvar_prefix='MYSQLBACKUP')
