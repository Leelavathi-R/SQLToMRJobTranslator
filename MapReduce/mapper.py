#!/usr/bin/python3
"""mapper.py"""
import json,csv
import sys
import operator
from io import StringIO

class Mapper:

    def __init__(self, element):
        self.csv_data = ""
        self.csv_file_path = "/user/hadoop/NetflixShows.csv"
        self.columnIndex = element['columnIndex']
        self.whereColumnIndex = element['whereColumnIndex']
        self.whereValue = element['whereValue']
        self.whereOperator = element['whereOperator']
        self.operator = {
            '=': operator.eq,
            '<': operator.lt,
            '>': operator.gt,
            '<=': operator.le,
            '>=': operator.ge,
            '!=': operator.ne
        }

    def execute(self):
        for line in sys.stdin:
            csv_file = StringIO(''.join([line]))
            csv_reader = csv.reader(csv_file)
            csv_list = list(csv_reader)
            row = csv_list[0]
            if len(self.columnIndex) == 0:
                key = row[0]
                if(self.whereColumnIndex != None):
                    if self.operator[self.whereOperator](row[self.whereColumnIndex],self.whereValue):
                        value = line.strip()
                        print(f"{key}\t{value}")
                else:
                    value = line.strip()
                    print(f"{key}\t{value}")
            else:
                key = row[0]
                if(self.whereColumnIndex != None):
                    if self.operator[self.whereOperator](row[self.whereColumnIndex],self.whereValue):
                        selected_columns =[row[x] for x in self.columnIndex]
                        value = ','.join(selected_columns)
                        print(f"{key}\t{value}")
                else:
                    selected_columns =[row[x] for x in self.columnIndex]
                    value = ','.join(selected_columns)
                    print(f"{key}\t{value}")

if __name__ == '__main__':
    with open('./elements.json', 'r') as f:
        elements = json.load(f)
    mapper = Mapper(elements)
    mapper.execute()


#{"tableName": "NetflixShows", "columnIndex": [0,1,2,3,4,5,6,7,8,9,10,11], "whereColumnIndex": 5, "whereValue": "\"India\"", "whereOperator": "="}