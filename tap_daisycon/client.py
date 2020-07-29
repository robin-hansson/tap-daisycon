import time
import backoff
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import ConnectionError

import singer
from singer import metrics

API_URL = 'https://services.daisycon.com'
LOGGER = singer.get_logger()


class Server5xxError(Exception):
    pass


# TODO: missing implementation for https://www.daisycon.com/en/developers/api/rate-limits/ --> 503 'Please reduce your request rate.' = limit reached, wait for 1 hour
class Server429Error(Exception):
    pass


class DaisyconError(Exception):
    pass


class DaisyconNotFoundError(DaisyconError):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    404: DaisyconNotFoundError}


def get_exception_for_error_code(status_code):
    return ERROR_CODE_EXCEPTION_MAPPING.get(status_code, DaisyconError)

# Error message example
# ....
def raise_for_error(response):
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            content_length = len(response.content)
            if content_length == 0:
                # There is nothing we can do here since Snapchat has neither sent
                # us a 2xx response nor a response content.
                return
            #if status_code in [404]:
            #        LOGGER.error(response)

            raise NotImplementedError()
            response_json = response.json()
            status_code = response.status_code
            error_type = response_json.get('error')
            error_description = response_json.get('description')

            if error_type:
                error_message = '{} {}: {}'.format(status_code, error_type, error_description)
                LOGGER.error(error_message)
                ex = get_exception_for_error_code(status_code)
                raise ex(error_message)
            else:
                raise DaisyconError(error)
        except (ValueError, TypeError):
            raise DaisyconError(error)

class DaisyconClient:
    def __init__(self,
                 username,
                 password,
                 user_agent=None):
        self.__username = username
        self.__password = password
        self.__user_agent = user_agent
        self.__session = requests.Session()
        self.base_url = API_URL
        self.last_response_headers = None

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, Server429Error),
                          max_tries=7,
                          factor=3)
    def request(self, method, path=None, url=None, **kwargs):

        if not url and path:
            url = '{}/{}'.format(self.base_url, path)

        # endpoint = stream_name (from sync.py API call)
        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code == 429:
            raise Server429Error()

        if response.status_code == 204: # no content
            return None

        if response.status_code != 200:
            LOGGER.error('{}: {}'.format(response.status_code, response.text))
            raise_for_error(response)

        self.last_response_headers = response.headers

        # Catch invalid json response
        try:
            response_json = response.json()
        except Exception as err:
            LOGGER.error('{}'.format(err))
            LOGGER.error('response.headers = {}'.format(response.headers))
            LOGGER.error('response.reason = {}'.format(response.reason))
            raise Exception(err)

        return response_json

    def get(self, url, **kwargs):
        return self.request('GET', url=url, auth=HTTPBasicAuth(self.__username, self.__password), *kwargs)
