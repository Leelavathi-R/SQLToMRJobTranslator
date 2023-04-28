#!/usr/bin/python3
"""reducer.py"""
import operator
import json
import sys


class Reducer:

    def __init__(self):
        self.data = {}

    def reduce(self):
        for row in sys.stdin:
            key, value = row.strip().split('\t')
            print(value)
if __name__ == '__main__':
    '''with open('./elements.json', 'r') as f:
        elements = json.load(f)'''
    elements ={}
    reducer = Reducer()
    reducer.reduce()

#ssh -i emr-key-pair.pem hadoop@ec2-3-235-184-206.compute-1.amazonaws.com
#hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files /home/hadoop/mapper.py,/home/hadoop/reducer.py -mapper mapper.py  -reducer reducer.py -input hdfs:///user/hadoop/word.txt -output output
