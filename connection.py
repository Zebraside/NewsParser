import logging

import urllib3
from urllib3 import PoolManager
from urllib3.response import HTTPResponse
from urllib3.exceptions import ReadTimeoutError, NewConnectionError, MaxRetryError
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Connection:
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:65.0) Gecko/20100101 Firefox/65.0'
    }

    def __init__(self, timeout: float = 5.0, num_pools: int = 50) -> None:
        self.http = PoolManager(num_pools=num_pools, timeout=timeout)

    def get_connection(self, url: str) -> HTTPResponse:
        try:
            r = self.http.request('GET', url, preload_content=False, headers=self.headers)
            if r.status == 200:
                return r
            else:
                logging.warning("Code " +  r.status + " at " + url)
        except (NewConnectionError, MaxRetryError, ReadTimeoutError) as e:
            logging.warning(e.__class__.__name__ + " at " + url)
