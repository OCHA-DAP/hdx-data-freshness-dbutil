# hdx-data-freshness-dbutil

Utility to perform actions on the data freshness database. Currently the only actions is to duplicate it with the option of only copying only specifics runs which is useful for testing.

It is run using: `python run.py ARGS` where possible ARGS are below:

    parser.add_argument('-dbr', '--dbread_url', default=None, help='Database connection string')
    parser.add_argument('-dpr', '--dbread_params', default=None, help='Database connection parameters. Overrides --dbread_url.')
    parser.add_argument('-dbw', '--dbwrite_url', default=None, help='Database connection string')
    parser.add_argument('-dpw', '--dbwrite_params', default=None, help='Database connection parameters. Overrides --dbwrite_url.')
    parser.add_argument('-act', '--action', default=None, help='Action to take (currently only "duplicate")')
    parser.add_argument('-rn', '--run_numbers', default=None, help='Run numbers to use or "latest"')
