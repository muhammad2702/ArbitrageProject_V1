**Crypto Order Book Collector**! 
This project allows you to gather real-time order book data from multiple cryptocurrency exchanges, process it efficiently, and store it using a Redis database for further analysis and integration.

---

## Key Features
- **Multi-Exchange Support**: Collect data from Binance and Bitstamp simultaneously.
- **Real-Time Processing**: Handle live WebSocket streams and manage order book updates seamlessly.
- **Redis Integration**: Store processed order book data for rapid access and scalability.
- **Configurable**: Flexible YAML-based configuration for symbol mappings, depth levels, and data collection durations.
- **Error Handling**: Robust error handling with reconnection logic and sequence gap detection.

---

## Project Structure

- **`collector.py`**  
  The main entry point. It initializes the system, loads configuration, and starts threads for each exchange.

- **`exchanges.py`**  
  Contains the logic for interacting with Binance and Bitstamp, including fetching initial snapshots and processing WebSocket streams.

- **`config.yaml`**  
  Configuration file to define Redis settings, exchange-specific parameters, and symbols to track.

---

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/your-repo/crypto-order-book-collector.git
   cd crypto-order-book-collector
