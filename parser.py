import json
ELEMENTS = {'select','from','where'}
class Parser:
    def __init__(self, query,schema):
        self.query = query
        self.schema = schema
        self.parsedQuery = {}
        self.tableSchema = []
    
    def parseQuery(self):
        query = self.query
        query = query.strip().split()
        queryParts = []
        parts = ''
        for element in query[1:]:
            if(element.lower() in ELEMENTS):
                queryParts.append(parts)
                parts = ''
            else:
                parts += element
        queryParts.append(parts)
        print(queryParts)
        try:
            self.schema[queryParts[1]]
        except Exception as e:
            return {'error':'Wrong Table Name!'}
        self.parsedQuery['tableName'] = queryParts[1]
        self.tableSchema = list(self.schema[self.parsedQuery['tableName']])
        if not self.parseColumnName(queryParts[0]):
            return {'error':'Invalid ColumnName(s)!'}
        try:
            self.parseWhereClause(queryParts[2])
        except Exception as e:
             self.parsedQuery['whereColumnIndex'] = None
             self.parsedQuery['whereValue'] = ""
             self.parsedQuery['whereOperator'] = ""
        print(self.parsedQuery)
        with open('config/elements.json', 'w') as f:
            json.dump(self.parsedQuery, f)
        return self.parsedQuery

    def parseColumnName(self, columns):
        if ',' in columns:
            columnNames = columns.split(',')
            columnIndex = []
            for column in  columnNames:
                try:
                    print("--->:")
                    self.tableSchema.index(column)
                except Exception as e:
                    return 0
                columnIndex.append(self.tableSchema.index(column))
            self.parsedQuery['columnIndex'] = columnIndex
        elif '*' in columns:
            self.parsedQuery['columnIndex'] = []
        else:
            try:
                print("===>:")
                self.parsedQuery['columnIndex'] = self.tableSchema.index(column)
            except Exception as e:
                return 0
        return 1

    def parseWhereClause(self, whereClause):
        operators = ['<','<=','>','>=','!=','=']
        for operator in operators:
            if(operator in whereClause):
                operands = whereClause.strip().split(operator)
                self.parsedQuery['whereColumnIndex'] = self.tableSchema.index(operands[0])
                print(operands[1])
                self.parsedQuery['whereValue'] = operands[1]
                print(self.parsedQuery['whereValue'] )
                self.parsedQuery['whereOperator'] = operator
                break