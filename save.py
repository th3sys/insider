import asyncio
import json
import logging
import os
import time
import datetime
import utils
from trading import EdgarParams, Scheduler, FileType
import uvloop


async def main(loop, logger, items, today):
    try:
        params = EdgarParams()
        params.Url = os.environ['EDGAR_URL']
        params.PageSize = os.environ['PAGE_SIZE']
        params.Timeout = int(os.environ['TIMEOUT'])

        notify = os.environ['NOTIFY_ARN']

        async with Scheduler(notify, params, logger, loop) as scheduler:
            res = await scheduler.SyncTransactions(items, FileType.ISSUER)
            scheduler.Save({'Received': items, 'Processed': res}, today, 'ISSUERS')
            res = await scheduler.SyncTransactions(items, FileType.OWNER)
            scheduler.Save({'Received': items, 'Processed': res}, today, 'OWNERS')
            logger.info('%s transactions loaded in db' % len(items))

    except Exception as e:
        logger.error(e)


def lambda_handler(event, context):

    logger = logging.getLogger()
    if 'LOGGING_LEVEL' in os.environ and os.environ['LOGGING_LEVEL'] == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

    logger.info('event %s' % event)
    logger.info('context %s' % context)
    items = event['Records'][0]['Sns']['Message']['CIK']
    today = event['Records'][0]['Sns']['Message']['Date']

    if 'EDGAR_URL' not in os.environ or 'PAGE_SIZE' not in os.environ or 'TIMEOUT' not in os.environ \
            or 'NOTIFY_ARN'not in os.environ or 'TRN_FOUND_ARN' not in os.environ:
        logger.error('ENVIRONMENT VARS are not set')
        return json.dumps({'State': 'ERROR'})

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    app_loop = asyncio.get_event_loop()
    app_loop.run_until_complete(main(app_loop, logger, items, today))

    return json.dumps({'State': 'OK'})


if __name__ == '__main__':
    with open("events/save.json") as json_file:
        test_event = json.load(json_file, parse_float=utils.DecimalEncoder)
    lambda_handler(test_event, None)
