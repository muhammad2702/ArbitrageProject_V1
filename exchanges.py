# exchanges.py
import json
import time
import threading
import requests
from websocket import WebSocketApp, WebSocketException
from datetime import datetime

import json
import time
import requests
from websocket import WebSocketApp
from datetime import datetime

def handle_binance(redis_client, symbols, config):
    exchange = "binance"
    upload_speed = config.get('upload_speed', 100)
    terminal_callback = config.get('terminal_callback', False)
    depth_level = config.get('depth_level', 20)  # For initial snapshot

    symbols_upper = [symbol.upper() for symbol in symbols]
    symbols_lower = [symbol.lower() for symbol in symbols]

    # Initialize variables
    lastUpdateIds = {}          # Stores lastUpdateId from snapshot for each symbol
    order_books = {}            # Local order book for each symbol
    buffers = {}                # Buffers events before snapshot is loaded

    # Prepare streams
    streams = [f"{symbol}@depth@{upload_speed}ms" for symbol in symbols_lower]
    ws_url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"

    def get_initial_snapshot(symbol):
        """Fetches initial snapshot and initializes local order book."""
        url = 'https://api.binance.com/api/v3/depth'
        params = {'symbol': symbol, 'limit': 1000}  # Binance recommends limit=1000
        response = requests.get(url, params=params)
        data = response.json()

        # Initialize lastUpdateId and local order book
        lastUpdateIds[symbol] = data['lastUpdateId']
        order_books[symbol] = {
            'bids': {price: quantity for price, quantity in data['bids']},
            'asks': {price: quantity for price, quantity in data['asks']}
        }

    def on_message(ws, message):
        data = json.loads(message)
        if 'data' in data:
            stream = data['stream']
            symbol = stream.split('@')[0].upper()  # e.g., 'BTCUSDT'

            event = data['data']
            U = event['U']
            u = event['u']
            bids = event['b']
            asks = event['a']
            event_time = event['E'] * 1000  # Convert to microseconds

            # Buffer events until snapshot is loaded
            if symbol not in lastUpdateIds:
                if symbol not in buffers:
                    buffers[symbol] = []
                buffers[symbol].append(event)
                return

            # Apply buffered events
            if symbol in buffers:
                for buffered_event in buffers[symbol]:
                    apply_event(symbol, buffered_event)
                del buffers[symbol]  # Clear buffer

            # Apply current event
            apply_event(symbol, event)

    def apply_event(symbol, event):
        U = event['U']
        u = event['u']
        bids = event['b']
        asks = event['a']

        lastUpdateId = lastUpdateIds[symbol]

        # Discard events with u <= lastUpdateId
        if u <= lastUpdateId:
            return

        # Check for sequence gap
        if not (U <= lastUpdateId + 1 <= u):
            # Sequence gap detected
            error_message = (f"[{exchange}] Sequence gap detected for {symbol}. "
                             f"Expected U ≤ {lastUpdateId + 1} ≤ u, Received U: {U}, u: {u}. "
                             "Requesting new snapshot.")
            print(error_message)
            redis_client.log_error(exchange, symbol, error_message)
            # Reinitialize snapshot and reset buffers
            get_initial_snapshot(symbol)
            buffers[symbol] = []
            return

        # Update local order book
        for price, quantity in bids:
            if float(quantity) == 0:
                order_books[symbol]['bids'].pop(price, None)
            else:
                order_books[symbol]['bids'][price] = quantity

        for price, quantity in asks:
            if float(quantity) == 0:
                order_books[symbol]['asks'].pop(price, None)
            else:
                order_books[symbol]['asks'][price] = quantity

        # Update lastUpdateId
        lastUpdateIds[symbol] = u

        # Prepare order book data with limited depth
        bids_sorted = sorted(order_books[symbol]['bids'].items(), key=lambda x: float(x[0]), reverse=True)[:depth_level]
        asks_sorted = sorted(order_books[symbol]['asks'].items(), key=lambda x: float(x[0]))[:depth_level]

        order_book = {
            'bids': bids_sorted,
            'asks': asks_sorted,
            'timestamp': event['E'] * 1000  # Microseconds
        }

        # Store in Redis
        redis_client.store_order_book(exchange, symbol, order_book)
        if terminal_callback:
            human_time = datetime.fromtimestamp(order_book['timestamp'] / 1e6).strftime('%Y-%m-%d %H:%M:%S.%f')
            print(f"[{exchange}] {symbol} Order book stored at {order_book['timestamp']} \n"
                  f"Human Readable time: {human_time} \n")
            print("===========================================================================")

    def on_open(ws):
        print(f"[{exchange}] Connected to WebSocket.")
        # Fetch initial snapshots
        for symbol in symbols_upper:
            get_initial_snapshot(symbol)

    def on_error(ws, error):
        print(f"[{exchange}] Error: {error}")
        redis_client.log_error(exchange, 'general', str(error))

    def on_close(ws):
        print(f"[{exchange}] WebSocket connection closed.")

    ws = WebSocketApp(ws_url,
                      on_open=on_open,
                      on_message=on_message,
                      on_error=on_error,
                      on_close=on_close)
    ws.run_forever()



def handle_bitstamp(redis_client, symbols, config):
    exchange = "bitstamp"
    depth_level = config.get('depth_level', 20)
    terminal_callback = config.get('terminal_callback', False)
    ws_url = "wss://ws.bitstamp.net"

    # Initialize variables
    order_books = {}        # Local order book for each symbol
    last_microtimestamps = {}  # Stores last microtimestamp for each symbol
    buffers = {}            # Buffers events before snapshot is loaded

    # Convert symbols to lower case as required by Bitstamp API
    symbols_lower = [symbol.lower() for symbol in symbols]

    def get_initial_snapshot(symbol):
        """Fetches initial snapshot and initializes local order book."""
        url = f'https://www.bitstamp.net/api/v2/order_book/{symbol}/'
        params = {'limit': 1000} #NOT WHATS DEFINED IN CONFIG
        response = requests.get(url, params=params)
        data = response.json()

        # Initialize last microtimestamp and local order book
        last_microtimestamps[symbol] = int(data['microtimestamp'])
        order_books[symbol] = {
            'bids': {price: quantity for price, quantity in data['bids']},
            'asks': {price: quantity for price, quantity in data['asks']}
        }

        # Store the initial order book in Redis
        bids_sorted = sorted(order_books[symbol]['bids'].items(), key=lambda x: float(x[0]), reverse=True)
        asks_sorted = sorted(order_books[symbol]['asks'].items(), key=lambda x: float(x[0]))
        order_book = {
            'bids': bids_sorted[:depth_level],
            'asks': asks_sorted[:depth_level],
            'timestamp': int(data['microtimestamp'])  # Microseconds
        }
        redis_client.store_order_book(exchange, symbol.upper(), order_book)

    def on_message(ws, message):
        data = json.loads(message)
        if data.get('event') == 'bts:subscription_succeeded':
            return  # Subscription succeeded message
        if data.get('event') == 'data':
            channel = data.get('channel')
            symbol = channel.replace('diff_order_book_', '')
            symbol = symbol.lower()

            event = data['data']
            microtimestamp = int(event['microtimestamp'])
            bids = event['bids']
            asks = event['asks']

            # Buffer events until snapshot is loaded
            if symbol not in last_microtimestamps:
                if symbol not in buffers:
                    buffers[symbol] = []
                buffers[symbol].append(event)
                return

            # Apply buffered events
            if symbol in buffers:
                for buffered_event in sorted(buffers[symbol], key=lambda x: int(x['microtimestamp'])):
                    apply_event(symbol, buffered_event)
                del buffers[symbol]  # Clear buffer

            # Apply current event
            apply_event(symbol, event)

    def apply_event(symbol, event):
        microtimestamp = int(event['microtimestamp'])
        bids = event['bids']
        asks = event['asks']

        last_microtimestamp = last_microtimestamps[symbol]

        # Check that the update is sequential
        if microtimestamp <= last_microtimestamp:
            # Outdated or duplicate update
            return

        # Update local order book
        for price, quantity in bids:
            if float(quantity) == 0:
                order_books[symbol]['bids'].pop(price, None)
            else:
                order_books[symbol]['bids'][price] = quantity

        for price, quantity in asks:
            if float(quantity) == 0:
                order_books[symbol]['asks'].pop(price, None)
            else:
                order_books[symbol]['asks'][price] = quantity

        # Update last_microtimestamp
        last_microtimestamps[symbol] = microtimestamp

        # Prepare order book data
        bids_sorted = sorted(order_books[symbol]['bids'].items(), key=lambda x: float(x[0]), reverse=True)
        asks_sorted = sorted(order_books[symbol]['asks'].items(), key=lambda x: float(x[0]))

        order_book = {
            'bids': bids_sorted[:depth_level],
            'asks': asks_sorted[:depth_level],
            'timestamp': microtimestamp  # Microseconds
        }

        # Store in Redis
        redis_client.store_order_book(exchange, symbol.upper(), order_book)
        if terminal_callback:
            human_time = datetime.fromtimestamp(order_book['timestamp'] / 1e6).strftime('%Y-%m-%d %H:%M:%S.%f')
            print(f"[{exchange}] {symbol.upper()} Order book stored at {order_book['timestamp']} \n"
                  f"Human Readable time: {human_time} \n")
            print("--------------------------------------------------------------------------------")

    def on_error(ws, error):
        print(f"[{exchange}] Error: {error}")
        redis_client.log_error(exchange, 'general', str(error))

    def on_close(ws):
        print(f"[{exchange}] WebSocket connection closed.")

    def on_open(ws):
        print(f"[{exchange}] Connected to WebSocket.")

        # Fetch initial snapshots
        for symbol in symbols_lower:
            get_initial_snapshot(symbol)

        # Subscribe to the diff_order_book channels for each symbol
        for symbol in symbols_lower:
            subscribe_message = json.dumps({
                "event": "bts:subscribe",
                "data": {
                    "channel": f"diff_order_book_{symbol}"
                }
            })
            ws.send(subscribe_message)
            print(f"[{exchange}] Subscription message sent for {symbol}.")

    ws = WebSocketApp(ws_url,
                      on_open=on_open,
                      on_message=on_message,
                      on_error=on_error,
                      on_close=on_close)

    ws.run_forever()

