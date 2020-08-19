import argparse
import logging

from hdx.facades.keyword_arguments import facade
from hdx.utilities.database import Database
from hdx.utilities.dictandlist import args_to_dict
from hdx.utilities.easy_logging import setup_logging

from databaseactions import DatabaseActions

setup_logging()
logger = logging.getLogger(__name__)


def main(dbread_url, dbread_params, dbwrite_url, dbwrite_params, action, run_numbers, **ignore):
    if dbread_params:
        readparams = args_to_dict(dbread_params)
    elif dbread_url:
        readparams = Database.get_params_from_sqlalchemy_url(dbread_url)
    else:
        readparams = {'driver': 'sqlite', 'database': 'input.db'}
    logger.info('> Database (to read) parameters: %s' % readparams)
    if dbwrite_params:
        writeparams = args_to_dict(dbwrite_params)
    elif dbwrite_url:
        writeparams = Database.get_params_from_sqlalchemy_url(dbwrite_url)
    else:
        writeparams = {'driver': 'sqlite', 'database': 'output.db'}
    logger.info('> Database (to write) parameters: %s' % writeparams)
    with Database(**readparams) as readsession:
        with Database(**writeparams) as writesession:
            dbactions = DatabaseActions(readsession, writesession, run_numbers)
            if action == 'duplicate':
                dbactions.duplicate()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Data Freshness DBUtils')
    parser.add_argument('-dbr', '--dbread_url', default=None, help='Database connection string')
    parser.add_argument('-dpr', '--dbread_params', default=None, help='Database connection parameters. Overrides --dbread_url.')
    parser.add_argument('-dbw', '--dbwrite_url', default=None, help='Database connection string')
    parser.add_argument('-dpw', '--dbwrite_params', default=None, help='Database connection parameters. Overrides --dbwrite_url.')
    parser.add_argument('-act', '--action', default=None, help='Action to take (currently only "duplicate")')
    parser.add_argument('-rn', '--run_numbers', default=None, help='Run numbers to use or "latest"')
    args = parser.parse_args()
    dbread_url = args.dbread_url
    if dbread_url and '://' not in dbread_url:
        dbread_url = 'postgresql://%s' % dbread_url
    dbwrite_url = args.dbwrite_url
    if dbwrite_url and '://' not in dbwrite_url:
        dbwrite_url = 'postgresql://%s' % dbwrite_url
    run_numbers = args.run_numbers
    logger.info('> Run numbers: %s' % run_numbers)
    run_numbers = run_numbers.split(',')
    facade(main, user_agent='test', dbread_url=dbread_url, dbread_params=args.dbread_params,
           dbwrite_url=dbwrite_url, dbwrite_params=args.dbwrite_params, action=args.action, run_numbers=run_numbers)
