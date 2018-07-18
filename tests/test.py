import boto3
import json
import utils
import os

with open("../events/find.json") as json_file:
    test_event = json.load(json_file, parse_float=utils.DecimalEncoder)

client = boto3.client('lambda')
for x in range(1, 32):
    test_event['time'] = '2016-03-%sT20:20:00Z' % x
    test_event['resources'] = ["arn:aws:events:us-east-1::rule/EOD3"]
    response = client.invoke(
        FunctionName=os.environ['func'],
        InvocationType='Event',
        Payload=json.dumps(test_event))

    print(response['Payload'].read())
    print("\n")
