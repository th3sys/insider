from __future__ import print_function # Python 2/3 compatibility
import boto3


def create_order():
    table = client.create_table(
        TableName='Insiders.Analytics',
        KeySchema=[
            {
                'AttributeName': 'AnalyticId',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'TransactionTime',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'AnalyticId',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'TransactionTime',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )

    w = client.get_waiter('table_exists')
    w.wait(TableName='Insiders.Analytics')
    print("table Insiders.Analytics created")
    print("Table status:", table)


client = boto3.client('dynamodb', region_name='us-east-1')

try:

    if 'Insiders.Analytics' in client.list_tables()['TableNames']:
        client.delete_table(TableName='Insiders.Analytics')
        waiter = client.get_waiter('table_not_exists')
        waiter.wait(TableName='Insiders.Analytics')
        print("table Insiders.Analytics deleted")

except Exception as e:
    print(e)

create_order()
