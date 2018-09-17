import boto3
import json
import utils
import os

with open("../events/find.json") as json_file:
    test_event = json.load(json_file, parse_float=utils.DecimalEncoder)

client = boto3.client('lambda')
for x in [2,3,6,7,8,9,10,13,14,15,16,17,20,21,22,23,24,27,28,29,30,31]:
    test_event['time'] = '2018-03-%sT20:20:00Z' % x
    test_event['resources'] = ["arn:aws:events:us-east-1::rule/EOD3"]
    response = client.invoke(
        FunctionName=os.environ['func'],
        InvocationType='Event',
        Payload=json.dumps(test_event))

    print(response['Payload'].read())
    print("\n")
