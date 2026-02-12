import time
import threading
import requests
import argparse


class RateLimiter:
    def __init__(self, min_interval_seconds):
        self.min_interval = min_interval_seconds
        self.last_called = 0.0
        self._lock = threading.Lock()

    def allow(self):
        with self._lock:
            now = time.time()
            if now - self.last_called >= self.min_interval:
                self.last_called = now
                return True
            return False


class OrderBook:
    def __init__(self, depth):
        self.coinbase_order_book = None
        self.gemini_order_book = None
        self.internal_order_book = None
        self._internal_lock = threading.Lock()
        self._coinbase_lock = threading.Lock()
        self._gemini_lock = threading.Lock()
        self.depth = depth
        self.coinbase_limiter = RateLimiter(2.0)
        self.gemini_limiter = RateLimiter(2.0)

    def fetch_coinbase(self):
        if not self.coinbase_limiter.allow():
            return
        url = "https://api.exchange.coinbase.com/products/BTC-USD/book"
        params = {"level": 2}
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        with self._coinbase_lock:
            self.coinbase_order_book = data

    def fetch_gemini(self):
        if not self.gemini_limiter.allow():
            return
        url = "https://api.gemini.com/v1/book/BTCUSD"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        with self._gemini_lock:
            self.gemini_order_book = data

    def build_internal_order_book(self):
        print("Building internal order book")
        with self._coinbase_lock:
            coinbase_book = self.coinbase_order_book
        with self._gemini_lock:
            gemini_book = self.gemini_order_book

        coinbase_bids = coinbase_book.get("bids") if coinbase_book else []
        gemini_bids = gemini_book.get("bids") if gemini_book else []
        coinbase_asks = coinbase_book.get("asks") if coinbase_book else []
        gemini_asks = gemini_book.get("asks") if gemini_book else []
        coinbase_bids_depth = min(self.depth, len(coinbase_bids)) if self.depth else len(coinbase_bids)
        gemini_bids_depth = min(self.depth, len(gemini_bids)) if self.depth else len(gemini_bids)
        coinbase_asks_depth = min(self.depth, len(coinbase_asks)) if self.depth else len(coinbase_asks)
        gemini_asks_depth = min(self.depth, len(gemini_asks)) if self.depth else len(gemini_asks)

        bids = []
        asks = []

        for level in coinbase_bids[: coinbase_bids_depth]:
            price, size = float(level[0]), float(level[1])
            bids.append({"price": price, "size": size, "exchange": "coinbase"})

        for level in gemini_bids[: gemini_bids_depth]:
            price, size = float(level["price"]), float(level["amount"])
            bids.append({"price": price, "size": size, "exchange": "gemini"})

        for level in coinbase_asks[: coinbase_asks_depth]:
            price, size = float(level[0]), float(level[1])
            asks.append({"price": price, "size": size, "exchange": "coinbase"})

        for level in gemini_asks[: gemini_asks_depth]:
            price, size = float(level["price"]), float(level["amount"])
            asks.append({"price": price, "size": size, "exchange": "gemini"})

        new_internal_book = {
            "bids": bids,
            "asks": asks,
        }

        with self._internal_lock:
            self.internal_order_book = new_internal_book

    def print_order_books(self):
        with self._internal_lock:
            book = self.internal_order_book

        if not book:
            print("Internal order book is not yet available.")
            return

        bids = book["bids"]
        asks = book["asks"]

        coinbase_bids = [b for b in bids if b["exchange"] == "coinbase"]
        gemini_bids = [b for b in bids if b["exchange"] == "gemini"]
        coinbase_asks = [a for a in asks if a["exchange"] == "coinbase"]
        gemini_asks = [a for a in asks if a["exchange"] == "gemini"]

        print("Bids:")
        for i in range(1, self.depth + 1):
            cb = coinbase_bids[i - 1] if len(coinbase_bids) >= i else None
            gm = gemini_bids[i - 1] if len(gemini_bids) >= i else None
            cb_text = f"Coinbase: price={cb['price']}, size={cb['size']}" if cb else "Coinbase: None"
            gm_text = f"Gemini: price={gm['price']}, size={gm['size']}" if gm else "Gemini: None"
            print(f"Level {i} - {cb_text} | {gm_text}")

        print("\nAsks:")
        for i in range(1, self.depth + 1):
            cb = coinbase_asks[i - 1] if len(coinbase_asks) >= i else None
            gm = gemini_asks[i - 1] if len(gemini_asks) >= i else None
            cb_text = f"Coinbase: price={cb['price']}, size={cb['size']}" if cb else "Coinbase: None"
            gm_text = f"Gemini: price={gm['price']}, size={gm['size']}" if gm else "Gemini: None"
            print(f"Level {i} - {cb_text} | {gm_text}")

    def calculate_execution_prices(self, quantity_btc=10.0):
        with self._internal_lock:
            book = self.internal_order_book

        if not book:
            print("Execution prices: internal order book not yet available.")
            return

        asks = list(book["asks"])
        bids = list(book["bids"])
        asks.sort(key=lambda x: x["price"])
        bids.sort(key=lambda x: x["price"], reverse=True)

        remaining_buy = quantity_btc
        cost_buy = 0.0
        buy_fills = []

        for level in asks:
            if remaining_buy <= 0:
                break
            size_take = min(level["size"], remaining_buy)
            cost = size_take * level["price"]
            cost_buy += cost
            buy_fills.append((level["exchange"], size_take, level["price"], cost))
            remaining_buy -= size_take

        remaining_sell = quantity_btc
        revenue_sell = 0.0
        sell_fills = []

        for level in bids:
            if remaining_sell <= 0:
                break
            size_take = min(level["size"], remaining_sell)
            rev = size_take * level["price"]
            revenue_sell += rev
            sell_fills.append((level["exchange"], size_take, level["price"], rev))
            remaining_sell -= size_take

        print("\n--- Execution prices (internal order book: Gemini + Coinbase) ---")
        print(f"To Buy {quantity_btc} BTC = ${cost_buy:,.2f}")
        print(f"To Sell {quantity_btc} BTC = ${revenue_sell:,.2f}")
        print("---\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Order book aggregator for Coinbase and Gemini")
    parser.add_argument("--qty", type=float, default=10.0, help="Quantity in BTC for execution price calculation (default: 10.0)")
    args = parser.parse_args()

    ob = OrderBook(depth=None)

    def coinbase_worker():
        while True:
            ob.fetch_coinbase()
            time.sleep(2)

    def gemini_worker():
        while True:
            ob.fetch_gemini()
            time.sleep(2)

    def printer_worker():
        while True:
            time.sleep(2)
            ob.build_internal_order_book()
            ob.calculate_execution_prices(quantity_btc=args.qty)

    threading.Thread(target=coinbase_worker).start()
    threading.Thread(target=gemini_worker).start()
    threading.Thread(target=printer_worker).start()
