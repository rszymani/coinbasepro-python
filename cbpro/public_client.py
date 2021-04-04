#
# cbpro/PublicClient.py
# Daniel Paquin
#
# For public requests to the Coinbase exchange
import time

import requests
import sys


class PublicClient(object):
    """cbpro public client API.

    All requests default to the `product_id` specified at object
    creation if not otherwise specified.

    Attributes:
        url (Optional[str]): API URL. Defaults to cbpro API.

    """

    def __init__(self, api_url='https://api.pro.coinbase.com', timeout=30):
        """Create cbpro API public client.

        Args:
            api_url (Optional[str]): API URL. Defaults to cbpro API.

        """
        self.url = api_url.rstrip('/')
        self.auth = None
        self.session = requests.Session()

    def get_products(self):
        """Get a list of available currency pairs for trading.

        Returns:
            list: Info about all currency pairs. Example::
                [
                    {
                        "id": "BTC-USD",
                        "display_name": "BTC/USD",
                        "base_currency": "BTC",
                        "quote_currency": "USD",
                        "base_min_size": "0.01",
                        "base_max_size": "10000.00",
                        "quote_increment": "0.01"
                    }
                ]

        """
        return self._send_message('get', '/products')

    def get_product_order_book(self, product_id, level=1):
        """Get a list of open orders for a product.

        The amount of detail shown can be customized with the `level`
        parameter:
        * 1: Only the best bid and ask
        * 2: Top 50 bids and asks (aggregated)
        * 3: Full order book (non aggregated)

        Level 1 and Level 2 are recommended for polling. For the most
        up-to-date data, consider using the websocket stream.

        **Caution**: Level 3 is only recommended for users wishing to
        maintain a full real-time order book using the websocket
        stream. Abuse of Level 3 via polling will cause your access to
        be limited or blocked.

        Args:
            product_id (str): Product
            level (Optional[int]): Order book level (1, 2, or 3).
                Default is 1.

        Returns:
            dict: Order book. Example for level 1::
                {
                    "sequence": "3",
                    "bids": [
                        [ price, size, num-orders ],
                    ],
                    "asks": [
                        [ price, size, num-orders ],
                    ]
                }

        """
        params = {'level': level}
        return self._send_message('get',
                                  '/products/{}/book'.format(product_id),
                                  params=params)

    def get_product_ticker(self, product_id):
        """Snapshot about the last trade (tick), best bid/ask and 24h volume.

        **Caution**: Polling is discouraged in favor of connecting via
        the websocket stream and listening for match messages.

        Args:
            product_id (str): Product

        Returns:
            dict: Ticker info. Example::
                {
                  "trade_id": 4729088,
                  "price": "333.99",
                  "size": "0.193",
                  "bid": "333.98",
                  "ask": "333.99",
                  "volume": "5957.11914015",
                  "time": "2015-11-14T20:46:03.511254Z"
                }

        """
        return self._send_message('get',
                                  '/products/{}/ticker'.format(product_id))

    def get_product_trades(self, product_id, after='', before='', stop_pagination=0, limit=100):
        """List the latest trades for a product.

        This method returns a generator which may make multiple HTTP requests
        while iterating through it. Science `before` it is not supported by coinbase it is not listed in
        method params.

        Args:
             product_id (str): Product
             after (Optional[str]): request page after (older) this pagination id.
             before (Optional[str]): request page before (newer) this pagination id.
             limit (Optional[int]): the desired number of trades (can be more than 100,
                          automatically paginated)
        Returns:
             list: Latest trades. Example::
                 [{
                     "time": "2014-11-07T22:19:28.578544Z",
                     "trade_id": 74,
                     "price": "10.00000000",
                     "size": "0.01000000",
                     "side": "buy"
                 }, {
                     "time": "2014-11-07T01:08:43.642366Z",
                     "trade_id": 73,
                     "price": "100.00000000",
                     "size": "0.01000000",
                     "side": "sell"
         }]
        """
        params = {}
        if before:
            params['before'] = before
        if limit:
            params['limit'] = limit
        if after:
            params['after'] = after
        return self._send_paginated_message(
            f'/products/{product_id}/trades', stop_pagination=stop_pagination, params=params
        )

    def get_product_historic_rates(self, product_id, start=None, end=None,
                                   granularity=None):
        """Historic rates for a product.

        Rates are returned in grouped buckets based on requested
        `granularity`. If start, end, and granularity aren't provided,
        the exchange will assume some (currently unknown) default values.

        Historical rate data may be incomplete. No data is published for
        intervals where there are no ticks.

        **Caution**: Historical rates should not be polled frequently.
        If you need real-time information, use the trade and book
        endpoints along with the websocket feed.

        The maximum number of data points for a single request is 200
        candles. If your selection of start/end time and granularity
        will result in more than 200 data points, your request will be
        rejected. If you wish to retrieve fine granularity data over a
        larger time range, you will need to make multiple requests with
        new start/end ranges.

        Args:
            product_id (str): Product
            start (Optional[str]): Start time in ISO 8601
            end (Optional[str]): End time in ISO 8601
            granularity (Optional[int]): Desired time slice in seconds

        Returns:
            list: Historic candle data. Example:
                [
                    [ time, low, high, open, close, volume ],
                    [ 1415398768, 0.32, 4.2, 0.35, 4.2, 12.3 ],
                    ...
                ]

        """
        params = {}
        if start is not None:
            params['start'] = start
        if end is not None:
            params['end'] = end
        if granularity is not None:
            acceptedGrans = [60, 300, 900, 3600, 21600, 86400]
            if granularity not in acceptedGrans:
                raise ValueError( 'Specified granularity is {}, must be in approved values: {}'.format(
                        granularity, acceptedGrans) )

            params['granularity'] = granularity
        return self._send_message('get',
                                  '/products/{}/candles'.format(product_id),
                                  params=params)

    def get_product_24hr_stats(self, product_id):
        """Get 24 hr stats for the product.

        Args:
            product_id (str): Product

        Returns:
            dict: 24 hour stats. Volume is in base currency units.
                Open, high, low are in quote currency units. Example::
                    {
                        "open": "34.19000000",
                        "high": "95.70000000",
                        "low": "7.06000000",
                        "volume": "2.41000000"
                    }

        """
        return self._send_message('get',
                                  '/products/{}/stats'.format(product_id))

    def get_currencies(self):
        """List known currencies.

        Returns:
            list: List of currencies. Example::
                [{
                    "id": "BTC",
                    "name": "Bitcoin",
                    "min_size": "0.00000001"
                }, {
                    "id": "USD",
                    "name": "United States Dollar",
                    "min_size": "0.01000000"
                }]

        """
        return self._send_message('get', '/currencies')

    def get_time(self):
        """Get the API server time.

        Returns:
            dict: Server time in ISO and epoch format (decimal seconds
                since Unix epoch). Example::
                    {
                        "iso": "2015-01-07T23:47:25.201Z",
                        "epoch": 1420674445.201
                    }

        """
        return self._send_message('get', '/time')

    def _send_message(self, method, endpoint, params=None, data=None):
        """Send API request.

        Args:
            method (str): HTTP method (get, post, delete, etc.)
            endpoint (str): Endpoint (to be added to base URL)
            params (Optional[dict]): HTTP request parameters
            data (Optional[str]): JSON-encoded string payload for POST

        Returns:
            dict/list: JSON response

        """
        url = self.url + endpoint
        r = self.session.request(method, url, params=params, data=data,
                                 auth=self.auth, timeout=30)
        return r.json()

    def _send_paginated_message(self, endpoint, stop_pagination=0, params=None):
        """ Send API message that results in a paginated response.

        The paginated responses are abstracted away by making API requests on
        demand as the response is iterated over.

        Paginated API messages support 3 additional parameters: `before`,
        `after`, and `limit`. `before` and `after` are mutually exclusive. To
        use them, supply an index value for that endpoint (the field used for
        indexing varies by endpoint - get_fills() uses 'trade_id', for example).
            `before`: Only get data that occurs more recently than index
            `after`: Only get data that occurs further in the past than index
            `limit`: Set amount of data per HTTP response. Default (and
                maximum) of 100.

        Args:
            endpoint (str): Endpoint (to be added to base URL)
            params (Optional[dict]): HTTP request parameters

        Yields:
            dict: API response objects

        """
        if params is None:
            params = dict()
        url = self.url + endpoint
        if params.get('before'):
            # coinbase pro doesn't support before.
            raise ValueError('Before param not work without after param. It should be passed togheter.')
        while True:
            response = self.session.get(url, params=params, auth=self.auth, timeout=30)
            results = response.json()
            time.sleep(0.5)
            for result in results:
                yield result
            # If there are no more pages, we're done. Otherwise update `after`
            # param to get next page.
            # Since coinbase pro doesn't support before, it was needed artificially compute stop of the pagination.
            next_after = int(response.headers['Cb-After']) if response.headers.get('Cb-After') else None
            if not next_after or stop_pagination == params.get('after', 0) - params.get('limit'):
                break

            elif stop_pagination > next_after - params.get('limit'):
                params['limit'] = next_after - stop_pagination
            params['after'] = next_after
