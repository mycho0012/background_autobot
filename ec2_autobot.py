import time
import os
from dotenv import load_dotenv
from class_yingyangvol import YingYangTradingBot
import logging
from datetime import datetime, timedelta

def run_bot(interval_minutes):
    load_dotenv()

    ticker = 'KRW-BTC'
    interval = f'minute{interval_minutes}'
    count = 300

    try:
        bot = YingYangTradingBot(ticker, interval, count, ema=True, window=20, span=10, stop_loss_percentage=5, take_profit_percentage=10)
        bot.run()
    except Exception as e:
        logging.error(f"Error running bot: {str(e)}")
        print(f"Error running bot: {str(e)}")

def get_next_run_time(interval_minutes):
    now = datetime.now()
    minutes = now.minute
    next_run = now.replace(second=0, microsecond=0)
    
    if interval_minutes == 15:
        if minutes < 15:
            next_run = next_run.replace(minute=15)
        elif minutes < 30:
            next_run = next_run.replace(minute=30)
        elif minutes < 45:
            next_run = next_run.replace(minute=45)
        else:
            next_run = (next_run + timedelta(hours=1)).replace(minute=0)
    elif interval_minutes == 30:
        if minutes < 30:
            next_run = next_run.replace(minute=30)
        else:
            next_run = (next_run + timedelta(hours=1)).replace(minute=0)
    else:  # 60 minutes
        next_run = (next_run + timedelta(hours=1)).replace(minute=0)
    
    return next_run

def main():
    # Set up logging
    logging.basicConfig(filename='trading_bot.log', level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')

    print("YingYang Trading Bot started")
    logging.info("YingYang Trading Bot started")

    # Ask user for the trading interval
    while True:
        try:
            interval_minutes = int(input("Enter the trading interval in minutes (15, 30, or 60): "))
            if interval_minutes in [15, 30, 60]:
                break
            else:
                print("Invalid input. Please enter 15, 30, or 60.")
        except ValueError:
            print("Invalid input. Please enter a number.")

    # Create a file to indicate the bot is running
    with open("bot_running.txt", "w") as f:
        f.write("Bot is running. Delete this file to stop the bot.")

    # Send initial Telegram message
    try:
        bot = YingYangTradingBot('KRW-BTC', f'minute{interval_minutes}', 300, stop_loss_percentage=5, take_profit_percentage=10)
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        bot.send_telegram_message(f"YingYang Trading Bot started at {start_time} with {interval_minutes}-minute intervals, 5% stop loss, and 10% take profit")
    except Exception as e:
        error_message = f"Error initializing bot: {str(e)}"
        logging.error(error_message)
        print(error_message)
        return

    while os.path.exists("bot_running.txt"):
        next_run = get_next_run_time(interval_minutes)
        now = datetime.now()
        
        if now < next_run:
            sleep_time = (next_run - now).total_seconds()
            logging.info(f"Waiting until {next_run.strftime('%Y-%m-%d %H:%M:%S')} for next run")
            time.sleep(sleep_time)
        
        run_bot(interval_minutes)
        
        # Log that the bot completed a cycle
        logging.info(f"Bot cycle completed. Next run at {get_next_run_time(interval_minutes).strftime('%Y-%m-%d %H:%M:%S')}")

    print("YingYang Trading Bot stopped")
    logging.info("YingYang Trading Bot stopped")
    try:
        bot.send_telegram_message("YingYang Trading Bot stopped")
    except Exception as e:
        logging.error(f"Error sending stop message: {str(e)}")

if __name__ == "__main__":
    main()
