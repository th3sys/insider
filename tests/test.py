import boto3
import json
import utils
import os

with open("../events/check.json") as json_file:
    test_event = json.load(json_file, parse_float=utils.DecimalEncoder)

client = boto3.client('lambda')
for x in [1,4,5,6,7,8,11,12,13,14,15,18,19,20,21,22,25,26,27,28,29]:
    test_event['time'] = '2017-12-%sT20:20:00Z' % x
    test_event['resources'] = ["arn:aws:events:us-east-1::rule/EOD3"]
    response = client.invoke(
        FunctionName=os.environ['func'],
        InvocationType='Event',
        Payload=json.dumps(test_event))

    print(response['Payload'].read())
    print("\n")
