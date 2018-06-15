import asyncio
import json
import utils
import logging
import os
import time
import datetime
import uuid
from trading import EdgarParams, Scheduler


async def main(loop, logger, today):
    try:
        params = EdgarParams()
        delay = float(os.environ['DELAY'])
        buffer = int(os.environ['BUFFER_SIZE'])
        notify = ''
        error_arn = os.environ['TRN_ERROR_ARN']

        async with Scheduler(notify, params, logger, loop) as scheduler:
            scheduler.ValidateResults(today, error_arn)
            logger.info('Check Succeeded')

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

    if 'TRN_FOUND_ARN' not in os.environ or 'DELAY' not in os.environ \
            or 'BUFFER_SIZE' not in os.environ or 'TRN_ERROR_ARN' not in os.environ:
        logger.error('ENVIRONMENT VARS are not set')
        return json.dumps({'State': 'ERROR'})

    app_loop = asyncio.get_event_loop()
    app_loop.run_until_complete(main(app_loop, logger, today))

    return json.dumps({'State': 'OK'})


if __name__ == '__main__':
    with open("events/check.json") as json_file:
        test_event = json.load(json_file, parse_float=utils.DecimalEncoder)
    lambda_handler(test_event, None)
