import os
import paramiko, csv
from io import StringIO

class MRResult:

    def __init__(self, config, schema, parsedQuery):
        self.mrResult = {}
        self.queryResult = []
        self.config = config
        self.schema = schema
        self.columns = parsedQuery['columnIndex']
        self.tableName = parsedQuery['tableName']
    def getResult(self):
        self.getFileFromHDFS()
        with open('./part-00000', 'r') as f:
            results = f.readlines()
        if len(self.columns) == 0:
            self.columns = list(range(len(self.schema[self.tableName])))
            print(self.columns)
        for result in results:
            csv_file = StringIO(''.join([result]))
            csv_reader = csv.reader(csv_file)
            csv_list = list(csv_reader)
            result = csv_list[0]
            tempDict = {
                self.schema[self.tableName][self.columns[x]]:result[x] for x in self.columns
            }
            self.queryResult.append(tempDict)

        self.mrResult['result'] = self.queryResult
        return self.mrResult
   
    def getFileFromHDFS(self):
        emr_dns = self.config['emr_dns']
        key_file = self.config['key_file']
        username = self.config['username']
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=emr_dns, username=username, key_filename=key_file)
        cmd = 'sudo hadoop fs -get {data_file}/{table} {local_in_out_dir}/'.format(
            data_file = self.config['input_output_dir'],
            table=self.tableName,
            local_in_out_dir = self.config['local_in_out_dir']
        )
        print('executing: ',cmd)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        result = stdout.read().decode()
        print('error:',stderr.read().decode())
        print("completed:",result)
        ssh.close()
        cmd = 'scp -i {key_file} hadoop@{emr_dns}:/home/hadoop/{table}/part-00000 ./'.format(
            emr_dns = self.config['emr_dns'],
            table=self.tableName,
            key_file = self.config['key_file']
        )
        print(cmd)
        os.system(cmd)
        return
