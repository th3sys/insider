import asyncio
import json
import logging
import boto3
import os
import utils
import datetime
import uvloop
import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

from trading import EdgarParams, Scheduler, FileType


async def main(loop, logger, items, today, requestId, chunk_id):
    try:
        params = EdgarParams()
        params.Url = os.environ['EDGAR_URL']
        params.PageSize = os.environ['PAGE_SIZE']
        params.Timeout = int(os.environ['TIMEOUT'])
        params.StartYear = os.environ['START_YEAR']

        notify = ''

        async with Scheduler(notify, params, logger, loop) as scheduler:
            if scheduler.CheckIfProcessed(items, today, requestId, chunk_id):
                logger.info('Stop processing')
                return

            res, stats = await scheduler.SyncTransactions(items, FileType.ISSUER)
            scheduler.Save({'Received': items, 'Processed': res, 'Codes': stats}, today, 'ISSUERS', len(items),
                           'CIKs that reported on the day and had direct purchases in the past', requestId, chunk_id)
            logger.info('%s issuers loaded in db reqId: %s' % (len(res), requestId))
            res, stats = await scheduler.SyncTransactions(items, FileType.OWNER)
            scheduler.Save({'Received': items, 'Processed': res, 'Codes': stats}, today, 'OWNERS', len(items),
                           'CIKs that reported on the day and had direct purchases in the past', requestId, chunk_id)
            logger.info('%s owners loaded in db reqId: %s' % (len(res), requestId))
            logger.info('%s transactions loaded in db' % len(items))

            scheduler.UpdateProcessed(today, requestId, chunk_id)
            logger.info('UpdateProcessed')

    except Exception as e:
        logger.error(e)


def lambda_handler(event, context):

    if os.environ['DEPLOYMENT_MODE'] == 'LAMBDA':
        logger = logging.getLogger()
        if 'LOGGING_LEVEL' in os.environ and os.environ['LOGGING_LEVEL'] == 'DEBUG':
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
    else:
        logger = utils.CloudLogger()

    logger.info('event %s' % event)
    logger.info('context %s' % context)
    fixed = event['Records'][0]['body'] if isinstance(event, dict) else event.body
    logger.info(fixed)
    fixed_json = json.loads(fixed, parse_float=utils.DecimalEncoder)
    items = fixed_json['CIK']
    today = str(fixed_json['Date']).strip()
    today = datetime.datetime.strptime(today, '%Y%m%d')
    requestId = fixed_json['RequestId']
    chunk_id = fixed_json['ChunkId']

    if 'EDGAR_URL' not in os.environ or 'PAGE_SIZE' not in os.environ or 'TIMEOUT' not in os.environ \
            or 'START_YEAR' not in os.environ:
        logger.error('ENVIRONMENT VARS are not set')
        return json.dumps({'State': 'ERROR'})

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    app_loop = asyncio.get_event_loop()
    app_loop.run_until_complete(main(app_loop, logger, items, today, requestId, chunk_id))

    return json.dumps({'State': 'OK'})


if __name__ == '__main__':
    if 'DEPLOYMENT_MODE' not in os.environ or 'TRN_FOUND_ARN' not in os.environ:
        raise Exception('DEPLOYMENT_MODE or TRN_FOUND_ARN is not set')

    if os.environ['DEPLOYMENT_MODE'] == 'LAMBDA':
        with open("events/save.json") as json_file:
            test_event = json.load(json_file, parse_float=utils.DecimalEncoder)

        lambda_handler(test_event, None)
    else:
        # Get the service resource
        sqs = boto3.resource('sqs')

        # Get the queue
        queue = sqs.get_queue_by_name(QueueName=os.environ['TRN_FOUND_ARN'])

        messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=20, VisibilityTimeout=3600)
        while messages:
            for message in messages:
                lambda_handler(message, None)
                # Let the queue know that the message is processed
                message.delete()
            messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=20, VisibilityTimeout=3600)
