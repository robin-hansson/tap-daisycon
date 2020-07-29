#!/usr/bin/env python3
import sys
import json
import requests

import singer

from tap_daisycon.client import DaisyconClient
from tap_daisycon.discover import discover
from tap_daisycon.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'username',
    'password'
]

def do_discover():
    LOGGER.info('Starting discover')
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')

@singer.utils.handle_top_exception(LOGGER)
def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    config = {}
    if parsed_args.config:
        config = parsed_args.config

    with DaisyconClient(parsed_args.config['username'],
                        parsed_args.config['password'],
                        parsed_args.config.get('user_agent')) as client:

        state = {}
        if parsed_args.state:
            state = parsed_args.state

        if parsed_args.discover:
            do_discover()
        elif parsed_args.catalog:
            sync(client=client,
                 config=config,
                 catalog=parsed_args.catalog,
                 state=state)

if __name__ == '__main__':
    main()
