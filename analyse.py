import asyncio
import datetime
import json
import logging
import os
import uvloop
import utils
from trading import EdgarParams, Scheduler


async def main(loop, logger, today):
    try:
        params = EdgarParams()
        timeout = os.environ['TIMEOUT']
        arn = os.environ['TRN_ERROR_ARN']
        count = int(os.environ['TRN_COUNT'])
        notify = os.environ['TRN_NOTIFY']

        async with Scheduler(notify, params, logger, loop) as scheduler:
            scheduler.AnalyseThat(today, arn, count)
            logger.info('Analyse That Succeeded')

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

    if 'TIMEOUT' not in os.environ or 'TRN_ERROR_ARN' not in os.environ or 'TRN_COUNT' not in os.environ \
            or 'TRN_NOTIFY' not in os.environ:
        logger.error('ENVIRONMENT VARS are not set')
        return json.dumps({'State': 'ERROR'})

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    app_loop = asyncio.get_event_loop()
    app_loop.run_until_complete(main(app_loop, logger, today))

    return json.dumps({'State': 'OK'})


if __name__ == '__main__':
    if 'DEPLOYMENT_MODE' not in os.environ:
        raise Exception('DEPLOYMENT_MODE is not set')

    if os.environ['DEPLOYMENT_MODE'] == 'LAMBDA':
        with open("events/analyse.json") as json_file:
            test_event = json.load(json_file, parse_float=utils.DecimalEncoder)
        lambda_handler(test_event, None)
    else:
        e = {}
        t = datetime.datetime.now()
        e['time'] = t.strftime('%Y-%m-%dT%H:%M:%S')
        lambda_handler(e, None)

