# Order Book Aggregator

Aggregates order book data from Coinbase and Gemini exchanges and calculates execution prices for Bitcoin trades.

## Setup

1. Install dependencies:
```bash
pip3 install -r requirements.txt
```

2. Run the application:
```bash
python3 api.py
```

Use `--qty` to specify quantity in BTC (default: 10.0):
```bash
python3 api.py --qty 5.0
```

Assumptions:
The Coinbase and Gemini API will run at an interval of 2 seconds, and the Order book is built after every 2 seconds. This is to just ensure consistency. 
I could have used Events and Locks to create the order book only when the two api's have data in them, but I have prioritised the fact that it is necessary to get the execution price from the latest available data, instead of blocking the order book creation till the API's return.