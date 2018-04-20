from __future__ import print_function # Python 2/3 compatibility
import boto3


def create_company():
    table = client.create_table(
        TableName='Companies',
        KeySchema=[
            {
                'AttributeName': 'CIK',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'State',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'CIK',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'State',
                'AttributeType': 'S'
            }

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 2,
            'WriteCapacityUnits': 3
        }
    )

    w = client.get_waiter('table_exists')
    w.wait(TableName='Companies')
    print("table Companies created")
    print("Table status:", table)


def create_state():
    table = client.create_table(
        TableName='States',
        KeySchema=[
            {
                'AttributeName': 'Code',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'Code',
                'AttributeType': 'S'
            }

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )

    w = client.get_waiter('table_exists')
    w.wait(TableName='States')
    print("table States created")
    print("Table status:", table)


client = boto3.client('dynamodb', region_name='us-east-1')

try:

    if 'Companies' in client.list_tables()['TableNames']:
        client.delete_table(TableName='Companies')
        waiter = client.get_waiter('table_not_exists')
        waiter.wait(TableName='Companies')
        print("table Companies deleted")

    if 'States' in client.list_tables()['TableNames']:
        client.delete_table(TableName='States')
        waiter = client.get_waiter('table_not_exists')
        waiter.wait(TableName='States')
        print("table States deleted")


except Exception as e:
    print(e)

create_state()
create_company()
