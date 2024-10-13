import numpy as np
import pandas as pd
import pyupbit
import os
import requests
from notion_client import Client
from dotenv import load_dotenv
import logging
from datetime import datetime
import time
import uuid  # 추가: 고유 ID 생성을 위한 uuid 모듈

load_dotenv()

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
        logging.basicConfig(filename='trading_bot.log', level=logging.INFO, 
                            format='%(asctime)s - %(levelname)s - %(message)s')
        
        # Initialize Upbit client
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
        
        # Retrieve and store initial KRW balance
        self.initial_balance = self.upbit.get_balance("KRW")
        if self.initial_balance is None:
            logging.error("Unable to retrieve initial KRW balance")
            raise ValueError("Unable to retrieve initial KRW balance")
        logging.info(f"Initial KRW Balance: {self.initial_balance}")
        
        # Initialize invested BTC quantity
        self.btc_invested = 0.0
        self.position = "neutral"
    
    def get_current_position(self):
        return "long" if self.btc_invested > 0 else "neutral"
    
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
    
    def execute_trade(self):
        if self.last_signal is None:
            raise ValueError("Last signal must be generated before executing trade.")

        signal_data = self.last_signal.iloc[0]
        signal = signal_data['last_signal']
        price = signal_data['entry_price']

        try:
            if signal == 'Buy' and self.btc_invested == 0:
                amount = self.initial_balance * 0.3  # 30% of initial KRW balance
                order = self.upbit.buy_market_order(self.symbol, amount)
                if order and 'uuid' in order:
                    uuid_order = order['uuid']
                    # Polling for order completion
                    for _ in range(10):
                        order_details = self.upbit.get_order(uuid_order)
                        if order_details and order_details['state'] == 'done':
                            executed_volume = float(order_details['executed_volume'])
                            self.btc_invested = executed_volume
                            self.position = "long"
                            self.stop_loss_price = price * (1 - self.stop_loss_percentage / 100)
                            self.take_profit_price = price * (1 + self.take_profit_percentage / 100)
                            logging.info(f"Bought {self.btc_invested} BTC at {price} KRW")

                            # Generate unique trade ID
                            buy_trade_id = str(uuid.uuid4())
                            logging.info(f"Generated Buy Trade ID: {buy_trade_id}")

                            # Update Notion with buy trade
                            self.notion_create_buy_trade(buy_trade_id, price, self.stop_loss_price, self.take_profit_price, self.btc_invested)

                            # Send Slack message about Buy
                            buy_message = f"**Buy Executed**\nTrade ID: {buy_trade_id}\nPrice: {price} KRW\nQuantity: {self.btc_invested} BTC\nStop Loss: {self.stop_loss_price:.2f} KRW\nTake Profit: {self.take_profit_price:.2f} KRW"
                            self.send_slack_message(buy_message)

                            return f"Bought {self.btc_invested} BTC for {amount} KRW (30% of initial balance). Stop Loss: {self.stop_loss_price:.2f}, Take Profit: {self.take_profit_price:.2f}, Trade ID: {buy_trade_id}"
                        else:
                            time.sleep(1)  # Wait before next poll
                    raise ValueError("Buy order was not completed in time.")
                else:
                    raise ValueError(f"Buy order failed: {order.get('error', 'Unknown error')}")
            
            elif signal == 'Sell' and self.btc_invested > 0:
                # Retrieve the latest open Buy Trade ID from Notion
                buy_trade_id = self.notion_get_latest_open_buy_trade_id()
                if not buy_trade_id:
                    logging.error("No open Buy trade found in Notion for selling.")
                    return "No open Buy trade found to sell."

                btc_to_sell = self.btc_invested
                order = self.upbit.sell_market_order(self.symbol, btc_to_sell)
                if order and 'uuid' in order:
                    uuid_order = order['uuid']
                    # Polling for order completion
                    for _ in range(10):
                        order_details = self.upbit.get_order(uuid_order)
                        if order_details and order_details['state'] == 'done':
                            self.btc_invested = 0.0
                            self.position = "neutral"
                            self.stop_loss_price = None
                            self.take_profit_price = None
                            logging.info(f"Sold {btc_to_sell} BTC at {price} KRW")

                            # Generate unique Sell Trade ID
                            sell_trade_id = str(uuid.uuid4())
                            logging.info(f"Generated Sell Trade ID: {sell_trade_id}")

                            # Update Notion with sell trade
                            self.notion_create_sell_trade(sell_trade_id, buy_trade_id, price)

                            # Send Slack message about Sell
                            sell_message = f"**Sell Executed**\nTrade ID: {sell_trade_id}\nRelated Buy Trade ID: {buy_trade_id}\nPrice: {price} KRW\nQuantity: {btc_to_sell} BTC"
                            self.send_slack_message(sell_message)

                            return f"Sold {btc_to_sell} BTC at market price. Trade ID: {sell_trade_id}"
                        else:
                            time.sleep(1)  # Wait before next poll
                    raise ValueError("Sell order was not completed in time.")
                else:
                    raise ValueError(f"Sell order failed: {order.get('error', 'Unknown error')}")
            else:
                return f"No trade executed. Current position: {self.position}, Signal: {signal}"
        except Exception as e:
            logging.error(f"Error executing trade: {str(e)}")
            return f"Trade execution failed: {str(e)}"
    
    def notion_create_buy_trade(self, trade_id, price, stop_loss, take_profit, quantity):
        """
        노션 데이터베이스에 매수 거래를 생성합니다.
        """
        current_time = datetime.now().isoformat()
        new_page = {
            "parent": {"database_id": os.getenv('DATABASE_ID')},
            "properties": {
                "Trade ID": {"title": [{"text": {"content": trade_id}}]},
                "Type": {"select": {"name": "Buy"}},
                "Timestamp": {"date": {"start": current_time}},
                "Price": {"number": price},
                "Stop Loss": {"number": stop_loss},
                "Take Profit": {"number": take_profit},
                "Quantity": {"number": quantity},
                "Status": {"select": {"name": "Open"}},
                "Sell Timestamp": {"date": None},
                "Sell Price": {"number": None}
            }
        }

        try:
            notion = Client(auth=os.getenv('NOTION_API'))
            notion.pages.create(**new_page)
            logging.info(f"Notion Buy Trade created: Trade ID {trade_id}")
        except Exception as e:
            logging.error(f"Error creating Buy Trade in Notion: {str(e)}")
    
    def notion_create_sell_trade(self, sell_trade_id, buy_trade_id, sell_price):
        """
        노션 데이터베이스에 매도 거래를 생성합니다.
        """
        sell_timestamp = datetime.now().isoformat()
        new_page = {
            "parent": {"database_id": os.getenv('DATABASE_ID')},
            "properties": {
                "Trade ID": {"title": [{"text": {"content": sell_trade_id}}]},
                "Type": {"select": {"name": "Sell"}},
                "Timestamp": {"date": {"start": sell_timestamp}},
                "Price": {"number": sell_price},
                "Stop Loss": {"number": None},
                "Take Profit": {"number": None},
                "Quantity": {"number": 0},  # 매도 시 수량은 0으로 설정
                "Status": {"select": {"name": "Closed"}},
                "Buy Trade ID": {"rich_text": [{"text": {"content": buy_trade_id}}]},
                "Sell Timestamp": {"date": {"start": sell_timestamp}},
                "Sell Price": {"number": sell_price}
            }
        }

        try:
            notion = Client(auth=os.getenv('NOTION_API'))
            notion.pages.create(**new_page)
            logging.info(f"Notion Sell Trade created: Sell Trade ID {sell_trade_id}, Related Buy Trade ID {buy_trade_id}")
        except Exception as e:
            logging.error(f"Error creating Sell Trade in Notion: {str(e)}")
    
    def notion_get_latest_open_buy_trade_id(self):
        """
        노션 데이터베이스에서 상태가 'Open'인 가장 최근의 매수 거래 Trade ID를 조회합니다.
        """
        try:
            notion = Client(auth=os.getenv('NOTION_API'))
            response = notion.databases.query(
                **{
                    "database_id": os.getenv('DATABASE_ID'),
                    "filter": {
                        "and": [
                            {
                                "property": "Type",
                                "select": {
                                    "equals": "Buy"
                                }
                            },
                            {
                                "property": "Status",
                                "select": {
                                    "equals": "Open"
                                }
                            }
                        ]
                    },
                    "sorts": [
                        {
                            "property": "Timestamp",
                            "direction": "descending"
                        }
                    ],
                    "page_size": 1
                }
            )

            if response['results']:
                page = response['results'][0]
                trade_id = page['properties']['Trade ID']['title'][0]['text']['content']
                logging.info(f"Retrieved Open Buy Trade ID: {trade_id}")
                return trade_id
            else:
                logging.info("No open Buy trades found in Notion.")
                return None
        except Exception as e:
            logging.error(f"Error querying Notion for Open Buy Trade: {str(e)}")
            return None
    
    def send_slack_message(self, message):
        SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')

        if not SLACK_WEBHOOK_URL:
            logging.error("SLACK_WEBHOOK_URL must be set as an environment variable.")
            return

        payload = {
            'text': message
        }
        
        try:
            response = requests.post(SLACK_WEBHOOK_URL, json=payload)
            response.raise_for_status()
            logging.info(f"Slack message sent: {message}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error sending Slack message: {e}")
    
    def run(self):
        try:
            self.download_data()
            self.calculate_volatility()
            self.calculate_pan_bands()
            self.trading_signal()
            self.get_last_signal()
            
            # Send Slack message about the current signal
            signal_message = f"**YingYang Bot Update for {self.symbol}**\n"
            signal_message += f"Interval: {self.interval}\n"
            signal_message += f"Signal: {self.last_signal.iloc[0]['last_signal']}\n"
            signal_message += f"Price: {self.last_signal.iloc[0]['entry_price']}\n"
            signal_message += f"Timestamp: {self.last_signal.iloc[0]['timestamp']}\n"
            signal_message += f"YYL: {self.ying_yang_vol['YYL'].iloc[-1]:.2f}\n"
            signal_message += f"YYL_slow: {self.ying_yang_vol['YYL_slow'].iloc[-1]:.2f}\n"
            signal_message += f"Current Position: {self.position}\n"
            signal_message += f"BTC Invested: {self.btc_invested}\n"
            if self.stop_loss_price:
                signal_message += f"Stop Loss: {self.stop_loss_price:.2f} KRW\n"
            if self.take_profit_price:
                signal_message += f"Take Profit: {self.take_profit_price:.2f} KRW\n"
            
            self.send_slack_message(signal_message)
            logging.info(f"Signal Message Sent: {signal_message}")
            
            trade_result = self.execute_trade()
            # Trade execution messages are already sent within execute_trade
            
            # Log the completion of the bot cycle
            logging.info(f"Bot cycle completed: {trade_result}")
        except Exception as e:
            error_message = f"Error in bot execution: {str(e)}"
            logging.error(error_message)
            self.send_slack_message(f"ERROR: {error_message}")
    
    # Notion Create Buy Trade
    def notion_create_buy_trade(self, trade_id, price, stop_loss, take_profit, quantity):
        """
        노션 데이터베이스에 매수 거래를 생성합니다.
        """
        current_time = datetime.now().isoformat()
        new_page = {
            "parent": {"database_id": os.getenv('DATABASE_ID')},
            "properties": {
                "Trade ID": {"title": [{"text": {"content": trade_id}}]},
                "Type": {"select": {"name": "Buy"}},
                "Timestamp": {"date": {"start": current_time}},
                "Price": {"number": price},
                "Stop Loss": {"number": stop_loss},
                "Take Profit": {"number": take_profit},
                "Quantity": {"number": quantity},
                "Status": {"select": {"name": "Open"}},
                "Sell Timestamp": {"date": None},
                "Sell Price": {"number": None}
            }
        }

        try:
            notion = Client(auth=os.getenv('NOTION_API'))
            notion.pages.create(**new_page)
            logging.info(f"Notion Buy Trade created: Trade ID {trade_id}")
        except Exception as e:
            logging.error(f"Error creating Buy Trade in Notion: {str(e)}")
    
    # Notion Create Sell Trade
    def notion_create_sell_trade(self, sell_trade_id, buy_trade_id, sell_price):
        """
        노션 데이터베이스에 매도 거래를 생성합니다.
        """
        sell_timestamp = datetime.now().isoformat()
        new_page = {
            "parent": {"database_id": os.getenv('DATABASE_ID')},
            "properties": {
                "Trade ID": {"title": [{"text": {"content": sell_trade_id}}]},
                "Type": {"select": {"name": "Sell"}},
                "Timestamp": {"date": {"start": sell_timestamp}},
                "Price": {"number": sell_price},
                "Stop Loss": {"number": None},
                "Take Profit": {"number": None},
                "Quantity": {"number": 0},  # 매도 시 수량은 0으로 설정
                "Status": {"select": {"name": "Closed"}},
                "Buy Trade ID": {"rich_text": [{"text": {"content": buy_trade_id}}]},
                "Sell Timestamp": {"date": {"start": sell_timestamp}},
                "Sell Price": {"number": sell_price}
            }
        }

        try:
            notion = Client(auth=os.getenv('NOTION_API'))
            notion.pages.create(**new_page)
            logging.info(f"Notion Sell Trade created: Sell Trade ID {sell_trade_id}, Related Buy Trade ID {buy_trade_id}")
        except Exception as e:
            logging.error(f"Error creating Sell Trade in Notion: {str(e)}")
    
    # Notion Get Latest Open Buy Trade ID
    def notion_get_latest_open_buy_trade_id(self):
        """
        노션 데이터베이스에서 상태가 'Open'인 가장 최근의 매수 거래 Trade ID를 조회합니다.
        """
        try:
            notion = Client(auth=os.getenv('NOTION_API'))
            response = notion.databases.query(
                **{
                    "database_id": os.getenv('DATABASE_ID'),
                    "filter": {
                        "and": [
                            {
                                "property": "Type",
                                "select": {
                                    "equals": "Buy"
                                }
                            },
                            {
                                "property": "Status",
                                "select": {
                                    "equals": "Open"
                                }
                            }
                        ]
                    },
                    "sorts": [
                        {
                            "property": "Timestamp",
                            "direction": "descending"
                        }
                    ],
                    "page_size": 1
                }
            )

            if response['results']:
                page = response['results'][0]
                trade_id = page['properties']['Trade ID']['title'][0]['text']['content']
                logging.info(f"Retrieved Open Buy Trade ID: {trade_id}")
                return trade_id
            else:
                logging.info("No open Buy trades found in Notion.")
                return None
        except Exception as e:
            logging.error(f"Error querying Notion for Open Buy Trade: {str(e)}")
            return None
