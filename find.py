import asyncio
import json
import logging
import os
import time
import datetime
from trading import EdgarParams, Scheduler


async def main(loop, logger, today):
    try:
        params = EdgarParams()
        params.Url = os.environ['EDGAR_URL']
        params.PageSize = os.environ['PAGE_SIZE']
        params.Timeout = int(os.environ['TIMEOUT'])

        notify = os.environ['NOTIFY_ARN']
        trn_notify = os.environ['TRN_FOUND_ARN']

        async with Scheduler(notify, params, logger, loop) as scheduler:
            cik_list = await scheduler.SyncDailyIndex(today)
            logger.info('%s CIK numbers received' % len(cik_list))
            chunks = [cik_list[x:x+100] for x in range(0, len(cik_list), 100)]
            for chunk in chunks:
                scheduler.Notify(chunk, trn_notify, today)
                time.sleep(1)

            logger.info('%s CIK numbers sent' % len(cik_list))

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

    if 'TODAY' in os.environ:
        today = datetime.datetime.strptime(os.environ['TODAY'], '%Y-%m-%d')
    else:
        today = datetime.datetime.today()

    if 'EDGAR_URL' not in os.environ or 'PAGE_SIZE' not in os.environ or 'TIMEOUT' not in os.environ \
            or 'NOTIFY_ARN'not in os.environ or 'TRN_FOUND_ARN' not in os.environ:
        logger.error('ENVIRONMENT VARS are not set')
        return json.dumps({'State': 'ERROR'})

    app_loop = asyncio.get_event_loop()
    app_loop.run_until_complete(main(app_loop, logger, today))

    return json.dumps({'State': 'OK'})


if __name__ == '__main__':
    lambda_handler(None, None)