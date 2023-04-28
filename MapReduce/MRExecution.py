import os
import time
import paramiko


class MRExecution:

    def __init__(self, config, tableName):
        self.config = config
        self.tableName = tableName
    def executeJob(self):
        emr_dns = self.config['emr_dns']
        key_file = self.config['key_file']
        username = self.config['username']
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=emr_dns, username=username, key_filename=key_file)
        cmd = 'hadoop fs -put {data_file}/{table}.csv /user/hadoop'.format(
            data_file = self.config['local_in_out_dir'],
            table=self.tableName
        )
        print(cmd)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        result = stdout.read().decode()
        print("completed:",result)
        cmd = 'hadoop jar {hadoop_streaming_jar} -files {element_path},{mapper_path},{reducer_path} ' \
               '-mapper mapper.py  -reducer reducer.py -input hdfs://{input_dir}/{table}.csv -output '\
               'hdfs://{output_dir}/{table}'.format(
            hadoop_streaming_jar=self.config['hadoop_streaming_jar'],
            mapper_path=self.config['mapper_path'],
            reducer_path=self.config['reducer_path'],
            input_dir=self.config['input_output_dir'],
            table=self.tableName,
            output_dir=self.config['input_output_dir'],
            element_path=self.config['element_path']
            )
        print("executing: ",cmd)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        result = stdout.read().decode()
        print("completed:",result)
        ssh.close()
        return ''