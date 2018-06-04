import asyncio
import json
import logging
import os
import boto3
import datetime
from trading import EdgarParams, Scheduler


async def main(loop, logger):
    try:
        params = EdgarParams()
        params.Url = os.environ['EDGAR_URL']
        params.PageSize = os.environ['PAGE_SIZE']
        params.Timeout = int(os.environ['TIMEOUT'])
        if 'TODAY' in os.environ:
            today = datetime.datetime.strptime(os.environ['TODAY'], '%Y-%m-%d')
        else:
            today = datetime.datetime.today()

        notify = os.environ['NOTIFY_ARN']

        async with Scheduler(notify, params, logger, loop) as scheduler:
            cik_list = await scheduler.SyncDailyIndex(today)

            await scheduler.SyncTransactions(cik_list)
            logger.info('All transactions loaded in db')

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

    if 'EDGAR_URL' not in os.environ or 'PAGE_SIZE' not in os.environ or 'TIMEOUT' not in os.environ \
            or 'NOTIFY_ARN'not in os.environ:
        logger.error('ENVIRONMENT VARS are not set')
        return json.dumps({'State': 'ERROR'})

    app_loop = asyncio.get_event_loop()
    app_loop.run_until_complete(main(app_loop, logger))

    return json.dumps({'State': 'OK'})


if __name__ == '__main__':
    lambda_handler(None, None)
