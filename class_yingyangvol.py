import time
import os
import logging
import requests
import pyupbit
import numpy as np
import pandas as pd
from notion_client import Client
from dotenv import load_dotenv
from datetime import datetime

class YingYangTradingBot:
    def __init__(self, symbol, interval, count, ema=True, window=20, span=10, stop_loss_percentage=5, take_profit_percentage=10):
        self.symbol = symbol
        self.interval = interval
        self.count = count
        self.ema = ema
        self.window = window
        self.span = span
        self.price = None
        self.ying_yang_vol = None
        self.pan_bands = None
        self.signals = None
        self.last_signal = None
        self.stop_loss_percentage = stop_loss_percentage
        self.take_profit_percentage = take_profit_percentage
        self.stop_loss_price = None
        self.take_profit_price = None

        # Set up logging
        logging.basicConfig(
            filename='trading_bot.log',
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # Initialize Upbit client
        load_dotenv()
        access_key = os.getenv("ACCESS_KEY")
        secret_key = os.getenv("SECRET_KEY")
        if not access_key or not secret_key:
            logging.error("ACCESS_KEY or SECRET_KEY not found in environment variables")
            raise ValueError("ACCESS_KEY and SECRET_KEY must be set in the .env file")
        
        self.upbit = pyupbit.Upbit(access_key, secret_key)
        
        # Check if authentication was successful
        if not self.upbit.get_balances():
            logging.error("Failed to authenticate with Upbit API")
            raise ValueError("Failed to authenticate with Upbit API. Please check your ACCESS_KEY and SECRET_KEY")
        
        self.position = self.get_current_position()

    def get_current_position(self):
        try:
            asset = self.symbol.split('-')[1]
            btc_balance = self.upbit.get_balance(asset)
            if btc_balance is None:
                logging.warning(f"Unable to get balance for {self.symbol}. Assuming neutral position.")
                return "neutral"
            return "long" if btc_balance > 0 else "neutral"
        except Exception as e:
            logging.error(f"Error getting current position: {str(e)}")
            return "neutral"  # Assume neutral position in case of error

    def download_data(self):
        try:
            self.price = pyupbit.get_ohlcv(self.symbol, self.interval, self.count)
            if self.price is None or self.price.empty:
                raise ValueError(f"Failed to download data for {self.symbol}")
            return self.price
        except Exception as e:
            logging.error(f"Error downloading data: {str(e)}")
            raise

    def calculate_volatility(self):
        if self.price is None or self.price.empty:
            raise ValueError("Price data is not available. Please download data first.")
        
        price_close = self.price['close']
        if self.ema:
            ma = price_close.ewm(span=self.window, adjust=False).mean()
        else:
            ma = price_close.rolling(window=self.window).mean()

        diff = price_close - ma
        slow_window = self.span

        yang_vol = np.sqrt((diff**2 * (diff > 0)).rolling(window=self.window).mean())
        ying_vol = np.sqrt((diff**2 * (diff <= 0)).rolling(window=self.window).mean())
        total_vol = np.sqrt(yang_vol**2 + ying_vol**2)
        YYL = ((yang_vol - ying_vol) / total_vol) * 100
        YYL_slow = YYL.rolling(window=slow_window).mean()

        self.ying_yang_vol = pd.DataFrame({
            'ma': ma,
            'yang_vol': yang_vol,
            'ying_vol': ying_vol,
            'total_vol': total_vol,
            'YYL': YYL,
            'YYL_slow': YYL_slow
        })

        return self.ying_yang_vol

    def calculate_pan_bands(self):
        if self.ying_yang_vol is None:
            raise ValueError("Volatility must be calculated before Pan Bands.")
        
        ma = self.price['close'].ewm(span=self.span, adjust=False).mean()
        upper_band = ma + 2 * self.ying_yang_vol['yang_vol']
        lower_band = ma - 2 * self.ying_yang_vol['ying_vol']
        
        self.pan_bands = pd.DataFrame({
            'upper_band': upper_band,
            'lower_band': lower_band,
            'pan_river_up': (ma + upper_band) / 2,
            'pan_river_down': (ma + lower_band) / 2
        })

        return self.pan_bands

    def trading_signal(self):
        if self.ying_yang_vol is None or self.pan_bands is None:
            raise ValueError("Volatility and Pan Bands must be calculated before generating signals.")
        
        df = self.ying_yang_vol.join(self.pan_bands).join(self.price['close'])

        signals = pd.DataFrame(index=df.index, columns=['Signal', 'Position', 'Entry_Price', 'Exit_Price'])
        signals = signals.fillna(0)

        status = df.apply(
            lambda row: 1 if row['YYL'] > row['YYL_slow'] else (-1 if row['YYL'] < row['YYL_slow'] else 0),
            axis=1
        )
        prev_status = status.shift(1)

        for i in range(1, len(df)):
            current_status = status.iloc[i]
            previous_status = prev_status.iloc[i]
            current_price = df['close'].iloc[i]

            signal_diff = current_status - previous_status

            if (signal_diff == 2 or signal_diff == 1) and df['YYL'].iloc[i] < -75:
                signals['Signal'].iloc[i] = 1
                signals['Entry_Price'].iloc[i] = current_price
            elif (signal_diff == -2 or signal_diff == -1) and df['YYL'].iloc[i] > 75:
                signals['Signal'].iloc[i] = -1
                signals['Exit_Price'].iloc[i] = current_price
            elif self.position == "long":
                if self.stop_loss_price and current_price <= self.stop_loss_price:
                    signals['Signal'].iloc[i] = -1
                    signals['Exit_Price'].iloc[i] = current_price
                    logging.info(f"Stop loss triggered at {current_price}")
                elif self.take_profit_price and current_price >= self.take_profit_price:
                    signals['Signal'].iloc[i] = -1
                    signals['Exit_Price'].iloc[i] = current_price
                    logging.info(f"Take profit triggered at {current_price}")

        self.signals = signals
        return self.signals

    def get_last_signal(self):
        if self.signals is None:
            raise ValueError("Trading signals must be generated before getting last signal.")
        df = self.ying_yang_vol.join(self.pan_bands).join(self.price['close']).join(self.signals).dropna()
        
        timestamp = df.index[-1]
        last_signal_value = df['Signal'].iloc[-1]
        last_signal_str = 'Buy' if last_signal_value == 1 else 'Sell' if last_signal_value == -1 else 'No Signal'
        last_entry_price = df['close'].iloc[-1]

        last_signal_df = pd.DataFrame({
            'Ticker': [self.symbol],
            'last_signal': [last_signal_str],
            'timestamp': [timestamp],
            'entry_price': [last_entry_price]
        })
    
        self.last_signal = last_signal_df
        return self.last_signal

    def get_current_price(self):
        try:
            orderbook = pyupbit.get_orderbook(tickers=self.symbol)
            if orderbook and "orderbook_units" in orderbook[0]:
                ask_price = orderbook[0]["orderbook_units"][0]["ask_price"]
                bid_price = orderbook[0]["orderbook_units"][0]["bid_price"]
                current_price = (ask_price + bid_price) / 2  # 중간 가격 사용
                return current_price
            else:
                raise ValueError("Orderbook data is not available.")
        except Exception as e:
            logging.error(f"Error fetching current price: {str(e)}")
            raise

    def execute_trade(self):
        if self.last_signal is None:
            raise ValueError("Last signal must be generated before executing trade.")

        signal_data = self.last_signal.iloc[0]
        signal = signal_data['last_signal']

        try:
            if signal == 'Buy' and self.position == "neutral":
                current_price = self.get_current_price()
                time.sleep(1)  # 1초 대기

                krw_balance = self.upbit.get_balance("KRW")
                if krw_balance is None:
                    raise ValueError("Unable to get KRW balance")
                amount = krw_balance * 0.3  # 30% of total KRW balance
                volume = amount / current_price

                # 제한가 매수 주문 시도
                order = self.upbit.buy_limit_order(self.symbol, current_price, volume)
                if order and 'uuid' in order:
                    order_uuid = order['uuid']
                    logging.info(f"Buy limit order placed. UUID: {order_uuid}, Price: {current_price}, Volume: {volume}")

                    # 주문 체결 대기 (최대 5분)
                    start_time = time.time()
                    while time.time() - start_time < 300:  # 300초 = 5분
                        order_result = self.upbit.get_order(order_uuid)
                        state = order_result['state']
                        if state == 'done':
                            executed_price = order_result['price']
                            self.position = "long"
                            self.stop_loss_price = executed_price * (1 - self.stop_loss_percentage / 100)
                            self.take_profit_price = executed_price * (1 + self.take_profit_percentage / 100)
                            logging.info(f"Buy limit order filled at {executed_price} KRW")
                            return f"Bought {self.symbol} for {amount:.2f} KRW at {executed_price:.2f} KRW. Stop Loss: {self.stop_loss_price:.2f}, Take Profit: {self.take_profit_price:.2f}"
                        elif state == 'cancel':
                            logging.warning(f"Buy limit order {order_uuid} was canceled.")
                            break
                        else:
                            time.sleep(10)  # 10초 간격으로 상태 확인

                    # 제한가 주문이 체결되지 않은 경우 주문 취소 시도
                    cancel_result = self.upbit.cancel_order(order_uuid)
                    if cancel_result and 'uuid' in cancel_result:
                        logging.info(f"Buy limit order {order_uuid} canceled after 5 minutes.")
                    else:
                        logging.warning(f"Failed to cancel buy limit order {order_uuid}.")

                    # 시장가 매수 주문 시도
                    market_order = self.upbit.buy_market_order(self.symbol, amount)
                    if market_order and 'uuid' in market_order:
                        market_order_uuid = market_order['uuid']
                        logging.info(f"Market buy order placed. UUID: {market_order_uuid}, Amount: {amount}")

                        # 시장가 주문 체결 확인
                        market_order_result = self.upbit.get_order(market_order_uuid)
                        if market_order_result['state'] == 'done':
                            executed_price = market_order_result['price']
                            self.position = "long"
                            self.stop_loss_price = executed_price * (1 - self.stop_loss_percentage / 100)
                            self.take_profit_price = executed_price * (1 + self.take_profit_percentage / 100)
                            logging.info(f"Market buy order executed at {executed_price} KRW")
                            return f"Bought {self.symbol} for {amount:.2f} KRW at market price {executed_price:.2f} KRW. Stop Loss: {self.stop_loss_price:.2f}, Take Profit: {self.take_profit_price:.2f}"
                        else:
                            raise ValueError("Market buy order was not completed.")
                    else:
                        raise ValueError(f"Market buy order failed: {market_order.get('error', 'Unknown error')}")
                else:
                    raise ValueError(f"Buy limit order failed: {order.get('error', 'Unknown error')}")

            elif signal == 'Sell' and self.position == "long":
                current_price = self.get_current_price()
                time.sleep(1)  # 1초 대기

                btc_balance = self.upbit.get_balance(self.symbol.split('-')[1])
                if btc_balance is None:
                    raise ValueError(f"Unable to get {self.symbol} balance")

                # 제한가 매도 주문 시도
                order = self.upbit.sell_limit_order(self.symbol, current_price, btc_balance)
                if order and 'uuid' in order:
                    order_uuid = order['uuid']
                    logging.info(f"Sell limit order placed. UUID: {order_uuid}, Price: {current_price}, Volume: {btc_balance}")

                    # 주문 체결 대기 (최대 5분)
                    start_time = time.time()
                    while time.time() - start_time < 300:  # 300초 = 5분
                        order_result = self.upbit.get_order(order_uuid)
                        state = order_result['state']
                        if state == 'done':
                            executed_price = order_result['price']
                            self.position = "neutral"
                            self.stop_loss_price = None
                            self.take_profit_price = None
                            logging.info(f"Sell limit order filled at {executed_price} KRW")
                            return f"Sold {btc_balance} {self.symbol} at {executed_price:.2f} KRW"
                        elif state == 'cancel':
                            logging.warning(f"Sell limit order {order_uuid} was canceled.")
                            break
                        else:
                            time.sleep(10)  # 10초 간격으로 상태 확인

                    # 제한가 주문이 체결되지 않은 경우 주문 취소 시도
                    cancel_result = self.upbit.cancel_order(order_uuid)
                    if cancel_result and 'uuid' in cancel_result:
                        logging.info(f"Sell limit order {order_uuid} canceled after 5 minutes.")
                    else:
                        logging.warning(f"Failed to cancel sell limit order {order_uuid}.")

                    # 시장가 매도 주문 시도
                    market_order = self.upbit.sell_market_order(self.symbol, btc_balance)
                    if market_order and 'uuid' in market_order:
                        market_order_uuid = market_order['uuid']
                        logging.info(f"Market sell order placed. UUID: {market_order_uuid}, Volume: {btc_balance}")

                        # 시장가 주문 체결 확인
                        market_order_result = self.upbit.get_order(market_order_uuid)
                        if market_order_result['state'] == 'done':
                            executed_price = market_order_result['price']
                            self.position = "neutral"
                            self.stop_loss_price = None
                            self.take_profit_price = None
                            logging.info(f"Market sell order executed at {executed_price} KRW")
                            return f"Sold {btc_balance} {self.symbol} at market price {executed_price:.2f} KRW"
                        else:
                            raise ValueError("Market sell order was not completed.")
                    else:
                        raise ValueError(f"Market sell order failed: {market_order.get('error', 'Unknown error')}")
                else:
                    raise ValueError(f"Sell limit order failed: {order.get('error', 'Unknown error')}")
            else:
                return f"No trade executed. Current position: {self.position}, Signal: {signal}"
        except Exception as e:
            logging.error(f"Error executing trade: {str(e)}")
            return f"Trade execution failed: {str(e)}"

    def notion_update(self):
        NOTION_API = os.getenv('NOTION_API')
        DATABASE_ID = os.getenv('DATABASE_ID')
        if not NOTION_API or not DATABASE_ID:
            logging.error("NOTION_API and DATABASE_ID must be set as environment variables.")
            return

        try:
            notion = Client(auth=NOTION_API)
            if self.last_signal is None:
                raise ValueError("Last signal must be generated before updating Notion.")

            signal_data = self.last_signal.iloc[0]
            
            new_page = {
                "parent": {"database_id": DATABASE_ID},
                "properties": {
                    "Ticker": {"title": [{"text": {"content": signal_data['Ticker']}}]},
                    "Signal_time": {"rich_text": [{"text": {"content": signal_data['timestamp'].isoformat()}}]},
                    "Last_signal": {"rich_text": [{"text": {"content": signal_data['last_signal']}}]},
                    "Entry_price": {"number": signal_data['entry_price']},
                    "YYL": {"number": float(self.ying_yang_vol['YYL'].iloc[-1])},
                    "YYL_slow": {"number": float(self.ying_yang_vol['YYL_slow'].iloc[-1])},
                    "Current_position": {"rich_text": [{"text": {"content": self.position}}]},
                    "Interval": {"rich_text": [{"text": {"content": self.interval}}]},
                    "Stop_loss": {"number": self.stop_loss_price if self.stop_loss_price else None},
                    "Take_profit": {"number": self.take_profit_price if self.take_profit_price else None}
                }
            }
            notion.pages.create(**new_page)
            logging.info(f"Notion updated: {signal_data['Ticker']} - {signal_data['last_signal']} at {signal_data['entry_price']}, Position: {self.position}, Interval: {self.interval}, Stop Loss: {self.stop_loss_price}, Take Profit: {self.take_profit_price}")
        except Exception as e:
            logging.error(f"Error updating Notion: {str(e)}")

    def send_telegram_message(self, message):
        TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
        TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            logging.error("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set as environment variables.")
            return

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': message,
            'parse_mode': 'Markdown'
        }
        
        try:
            response = requests.post(url, data=payload)
            response.raise_for_status()
            logging.info(f"Telegram message sent: {message}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error sending Telegram message: {e}")

    def run(self):
        try:
            self.download_data()
            self.calculate_volatility()
            self.calculate_pan_bands()
            self.trading_signal()
            self.get_last_signal()
            
            trade_result = self.execute_trade()
            self.notion_update()
            
            message = f"YingYang Bot Update for {self.symbol}:\n"
            message += f"Interval: {self.interval}\n"
            message += f"Signal: {self.last_signal.iloc[0]['last_signal']}\n"
            message += f"Price: {self.last_signal.iloc[0]['entry_price']}\n"
            message += f"Timestamp: {self.last_signal.iloc[0]['timestamp']}\n"
            message += f"YYL: {self.ying_yang_vol['YYL'].iloc[-1]:.2f}\n"
            message += f"YYL_slow: {self.ying_yang_vol['YYL_slow'].iloc[-1]:.2f}\n"
            message += f"Current Position: {self.position}\n"
            if self.stop_loss_price:
                message += f"Stop Loss: {self.stop_loss_price:.2f}\n"
            if self.take_profit_price:
                message += f"Take Profit: {self.take_profit_price:.2f}\n"
            message += f"Trade Result: {trade_result}"
            
            self.send_telegram_message(message)
            logging.info(f"Bot cycle completed: {message}")
        except Exception as e:
            error_message = f"Error in bot execution: {str(e)}"
            logging.error(error_message)
            self.send_telegram_message(f"ERROR: {error_message}")

