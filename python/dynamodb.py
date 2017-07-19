from __future__ import print_function # Python 2/3 compatibility
import boto3

class Dynamodb():
    def __init__(self, table_name):
        # self.origindb = boto3.resource('dynamodb')
        self.origindb = boto3.resource('dynamodb',
                region_name='ap-northeast-1', endpoint_url='http://localhost:4569')
        self.table = self.create_table(table_name)

    def create_table(self, table_name):
        table = self.origindb.create_table(
            TableName='Movies',
            KeySchema=[
                {
                    'AttributeName': 'year',
                    'KeyType': 'HASH'  #Partition key
                },
                {
                    'AttributeName': 'title',
                    'KeyType': 'RANGE'  #Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'year',
                    'AttributeType': 'N'
                },
                {
                    'AttributeName': 'title',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        return(table)

    def table_status(self):
        return self.table.table_status

    def put_item(self, item):
        return self.table.put_item(Item=item)

    def get_item(self, year, title):
        res = self.table.get_item(
            Key={
                'year': year,
                'title': title,
            },
        )
        if 'Item' in res:
            return res['Item']
        else:
            return None

    def drop_table(self):
        table = self.origindb.Table('Movies')
        table.delete()
