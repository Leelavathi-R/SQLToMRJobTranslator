import yaml,os
from  parser import Parser
from MapReduce.MRExecution import MRExecution
from MapReduce.MRResult import MRResult

class Controller:
    def __init__(self,query):
        self.query = query
        self.schema = {}
        self.parsedQuery = {}
        self.config = {}

    def run(self):
        self.parsedQuery = self.getParsedQuery()
        print(self.parsedQuery)
        if 'error' in self.parsedQuery:
            return self.parsedQuery
        self.copyFilesToEMR()
        mrExecution = MRExecution(self.config,self.parsedQuery['tableName'])
        mrExecution.executeJob()
        mrResult = MRResult(self.config,self.schema,self.parsedQuery)
        return    mrResult.getResult()

    def getParsedQuery(self):
        with open('config/schema.yaml','r') as f:
            self.schema = yaml.load(f, Loader=yaml.FullLoader)
        with open('config/config.yaml','r') as f:
            self.config = yaml.load(f, Loader=yaml.FullLoader)
        self.parsedQuery = Parser(self.query, self.schema).parseQuery()
        return self.parsedQuery
    
    def copyFilesToEMR(self):
        cmd = 'scp -i {key_file} {data_file} {local_mapper} {local_reducer} {local_element} hadoop@{emr_dns}:/home/hadoop'.format(
            emr_dns = self.config['emr_dns'],
            key_file = self.config['key_file'],
            local_mapper =  self.config['local_mapper'],
            local_reducer = self.config['local_reducer'],
            data_file = self.config['data_file'],
            local_element = self.config['local_element']
        )
        print(cmd)
        os.system(cmd)