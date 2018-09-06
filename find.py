import asyncio
import json
import utils
import logging
import os
import time
import datetime
import uuid
import uvloop
from trading import EdgarParams, Scheduler


async def main(loop, logger, today):
    try:
        params = EdgarParams()
        params.Url = os.environ['EDGAR_URL']
        params.PageSize = os.environ['PAGE_SIZE']
        params.Timeout = int(os.environ['TIMEOUT'])
        delay = float(os.environ['DELAY'])
        buffer = int(os.environ['BUFFER_SIZE'])

        notify = ''
        trn_notify = os.environ['TRN_FOUND_ARN']

        async with Scheduler(notify, params, logger, loop) as scheduler:
            requestId = str(uuid.uuid4().hex)
            cik_list = await scheduler.SyncDailyIndex(today)
            logger.info('%s CIK numbers received' % len(cik_list))
            chunks = [cik_list[x:x+buffer] for x in range(0, len(cik_list), buffer)]
            i = 0
            chunk_ids = {}
            for chunk in chunks:
                i += 1
                chunk_ids[str(i)] = chunk
            scheduler.Save({'Received': cik_list}, today, 'FOUND', len(cik_list),
                           'CIKs that reported on the day', requestId, chunk_ids)
            i = 0
            for chunk in chunks:
                i += 1
                scheduler.Notify(chunk, trn_notify, today, requestId, i)
                time.sleep(delay)
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

    today = event['time'].split('T')[0]
    today = datetime.datetime.strptime(today, '%Y-%m-%d')
    today = today - datetime.timedelta(1)

    if 'EDGAR_URL' not in os.environ or 'PAGE_SIZE' not in os.environ or 'TIMEOUT' not in os.environ \
            or 'TRN_FOUND_ARN' not in os.environ or 'DELAY' not in os.environ or 'BUFFER_SIZE' not in os.environ:
        logger.error('ENVIRONMENT VARS are not set')
        return json.dumps({'State': 'ERROR'})

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    app_loop = asyncio.get_event_loop()
    app_loop.run_until_complete(main(app_loop, logger, today))

    return json.dumps({'State': 'OK'})


if __name__ == '__main__':
    with open("events/find.json") as json_file:
        test_event = json.load(json_file, parse_float=utils.DecimalEncoder)
    lambda_handler(test_event, None)
