import json
import logging
import os
import asyncio
from trading import EdgarClient, EdgarParams, Scheduler
from connectors import InsiderDb, DbParams


async def main(loop, logger):
    try:
        dbParams = DbParams()
        dbParams.Name = os.environ['DB_NAME']
        dbParams.Host = os.environ['DB_HOST']
        dbParams.Password = os.environ['DB_PASSWORD']
        dbParams.User = os.environ['DB_USER']

        params = EdgarParams()
        params.Url = os.environ['EDGAR_URL']
        params.PageSize = os.environ['PAGE_SIZE']

        async with Scheduler(params, dbParams, logger, loop) as scheduler:
            await scheduler.SyncCompanies()
            logger.info('All companies loaded in db')

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

    if 'EDGAR_URL' not in os.environ or 'PAGE_SIZE' not in os.environ or 'DB_NAME' not in os.environ \
            or 'DB_USER' not in os.environ or 'DB_HOST' not in os.environ or 'DB_PASSWORD' not in os.environ:
        logger.error('ENVIRONMENT VARS are not set')
        return json.dumps({'State': 'ERROR'})

    app_loop = asyncio.get_event_loop()
    app_loop.run_until_complete(main(app_loop, logger))

    return json.dumps({'State': 'OK'})


if __name__ == '__main__':
    lambda_handler(None, None)
