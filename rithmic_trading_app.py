#!/usr/bin/env python3

#################################################################
# LIBRARY IMPORTS                                               #
#################################################################

import asyncio
import threading
import time
import pandas as pd
import numpy as np
import sys
from time import sleep
from datetime import datetime as dt, timedelta
from hist_bars_ohlc import HistBarsApp
from pnl_pos_stream import PnlPosStreamApp
from md_stream import MdStreamApp
from place_orders import PlaceOrderApp

#################################################################
# VARIABLES                                                     #
#################################################################

# The url of the Rithmic server
uri              = 'wss://rprotocol.rithmic.com:443'

# The name of the Rithmic System. 
# For live trading, change to Rithmic 01.
system_name      = 'Rithmic Paper Trading'

# The username used to log into RTrader.
user_id          = sys.argv[1]

# The password used to log into RTrader. 
# Passed as a command line argument.
password         = sys.argv[2]

# The exchange used for trading: CBOT, NYMEX, or CME. 
# Passed as a command line argument.
exchange         = sys.argv[3]

# The contract to be traded. Examples: UBM4, CLH4.  
# Passed as a command line argument.
symbol           = sys.argv[4]

# The number of contracts to be traded. 
# Passed as a command line argument and converted to int.
contract_qty     = int(sys.argv[5])

# The futures commission merchant or clearing firm.
fcm_id           = sys.argv[6]

# The introducing broker.
ib_id            = sys.argv[7]

# The username used to log into RTrader
account_id       = sys.argv[1]

# The order action. Initialized as empty string. 
# Can be set (B or S) when submitting order.
order_side       = ''

# Strategy run times. Initialized as empty strings.
# Variables are set by set_times() function.
start_time = ''
end_time   = ''
start_date = ''
end_date   = ''
time_now   = ''

# Timestamps. Initialized as None. 
# Populated by refresh_session() function.
timestamps = None

# Frequencies
# Interval between time stamps
timestamp_freq = '5T'

# Resample frequency when resampling ticks.
timeframe = '5min'

# Current time
current_time = ''

# Time format. Used to formate timestamps.
tf = '%Y-%m-%d %H:%M:%S'

# Create threading event variable
shutdown_event = threading.Event()

# Initialize last processed index.
# Used to track the last index processed in the md_stream DataFrame.
last_processed_index = -1

# Initialize the last printed time to a value far in the past.
# Used to print PnL every 15 seconds.
last_print_time = time.time() - 15

#################################################################
# FUNCTIONS                                                     #
#################################################################

def md_thread_function():
    '''
    Start market data streaming in a separate daemon thread.
    '''

    # Since the market data streaming app uses asyncio,
    # a new event loop must be set for this thread.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # Start the event loop and run the 
        # market data streaming application.
        md_stream_app.run()
    except asyncio.CancelledError:
        # Handle task cancellation, which might not be 
        # needed unless you cancel tasks explicitly.
        print("md_thread: Asyncio tasks were cancelled.")
    except Exception as e:
        # This will handle any other exceptions that might occur.
        print("md_thread: An error occurred:", e)
    finally:
        # Perform cleanup
        print("\nmd_thread: Cleaning up resources...")
        loop.close()

def pnl_thread_function():
    '''
    Start PnL and position data streaming in a separate daemon thread.
    '''

    # Since the data streaming app uses asyncio,
    # a new event loop for this thread must be set.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # Start the event loop and run the application
        pnl_pos_app.run()
    except asyncio.CancelledError:
        # Handle task cancellation, which might not be 
        # needed unless you cancel tasks explicitly
        print("pnl_thread: Asyncio tasks were cancelled.")
    except Exception as e:
        # This will handle any other exceptions that might occur
        print("pnl_thread: An error occurred:", e)
    finally:
        # Perform cleanup
        print("\npnl_thread: Cleaning up resources...")
        loop.close()

def print_pnl_pos():
    '''
    Print PnL updates.
    '''

    # Lock the following variables to prevent pnl_pos_app() access.
    # We are locking variable so this app can access them
    # without risking a race condition or data corruption.
    with pnl_pos_app.lock:
        current_position = pnl_pos_app.current_position
        open_position_pnl = pnl_pos_app.open_position_pnl
        daily_pnl = pnl_pos_app.daily_pnl
    
    # After unlocking variables, print pnl and position data.
    print(f'\nPosition PnL: {open_position_pnl}')
    print(f'Daily PnL: {daily_pnl}')
    print(f'Current Position: {current_position}')

def get_hist_bars():
    '''
    Download historical bars and write to a DataFrame.
    '''

    global uri
    global system_name
    global user_id
    global password
    global exchange
    global symbol

    # Create hist_bars object from HistBarsApp class.
    hist_sec_bars = HistBarsApp(
        uri,
        system_name,
        user_id,
        password,
        exchange,
        symbol)

    # Run the hist_bars.run() method to download historical bars.
    hist_sec_bars.run()

    # After DataFrame has been created, 
    # make a copy of it and return it.
    return hist_sec_bars.ohlc_df.copy()

def set_times():
    ''' 
    Set the start and end times for the algo
    '''
    
    global end_date
    global end_time
    global start_date
    global start_time
    global time_now
    
    time_now = dt.now().strftime('%H:%M:%S')
    
    # Set times if current time is between 6:00 p.m. and 11:59 p.m.
    if '18:00:00' <= time_now <= '23:59:59':
        # Get start date
        start_date = dt.now().strftime('%Y-%m-%d')

        # Get end date by adding 1 day to current date
        end_date = dt.now() + timedelta(days=1)

        # Convert end_date to a string
        end_date = dt.strftime(end_date, '%Y-%m-%d')

        # Get start times by concatenating start_date and time
        start_time = start_date + ' 18'+':'+'00'+':'+'00'
        
        # Get end time by concatenating end_date and time
        end_time = end_date + ' 17'+':'+'00'+':'+'00'

    # Set time if current time is outside of window of 6:00 p.m. - 11:59 p.m.
    else:
        # Get start date by subtracting 1 day from current date
        start_date = dt.now() - timedelta(days=1)

        # Convert the start_date to a string
        start_date = dt.strftime(start_date, '%Y-%m-%d')

        # Get end date
        end_date = dt.now().strftime('%Y-%m-%d')

        # Get start time by concatenating start_date and time
        start_time = start_date + ' 18'+':'+'00'+':'+'00'
        
        # Get end time by concatenating end_date and time
        end_time = end_date + ' 17'+':'+'00'+':'+'00'

    print('\nSTART and END SESSION TIMES:')
    print(f'Start Date: {start_date}')
    print(f'Start Datetime: {start_time}')
    print(f'End Date: {end_date}')
    print(f'End Datetime: {end_time}')
    print(f'Time Now: {time_now}')

def refresh_session():
    '''
    Regenerate timestamps for new futures session.
    '''
        
    # Generate timestamps for the trading period
    global timestamps
    global start_time
    global end_time
    global timestamp_freq
    global tf

    # Set the beginning and ending times for the trading algorithm
    set_times()

    timestamps = pd.date_range(start=start_time, end=end_time, freq=timestamp_freq).strftime(tf)
                
def submit_order(side, qty):
    '''
    Submit order to exchange.

    :param: side: str
        The side of contract to enter, B (Buy) or S (Sell).

    :param: qty: int
        The number of contracts to buy or sell.
    '''

    global uri
    global system_name
    global user_id
    global password
    global exchange
    global symbol

    # Create a PlaceOrderApp object.
    place_order_app = PlaceOrderApp(
        uri,
        system_name,
        user_id,
        password,
        exchange,
        symbol
    )

    # Call the object's run method to submit order.
    place_order_app.run(side, qty)

def run_strategy():
    '''
    Place any strategy logic here.
    '''

    print('\nPut your trading and execution logic here...')
    print('*' * 44)

def update_ohlc(new_data, df, timeframe):
    '''
    Update existing OHLC DataFrame with new OHLC data.
    
    :param new_data: DataFrame
        A DataFrame with new_tick data
    
    :param df: DataFrame
        A DataFrame with existing OHLC data

    :param timeframe: string 
        The timeframe to use to resample the new tick data
    
    :return param df: DataFrame
        A DataFrame with updated OHLC data
    '''

    # Ensure new_data has datetime index for resampling
    new_data.index = pd.to_datetime(new_data['Date'])

    # Resample the tick data to the given timeframe
    ohlc = new_data['Close'].resample(timeframe, label='right').ohlc()

    # Choose the most recent entry for each timestamp
    ohlc = ohlc.groupby(ohlc.index).tail(1)

    # Concatenate with the main DataFrame
    if not ohlc.empty:
        df = pd.concat([df, ohlc]).groupby(level=0).last()
    
    return df

#################################################################
# START OF PROGRAM                                              #
#################################################################

#------RUN ONE-TIME TASKS------#

# Create market data streaming object
md_stream_app = MdStreamApp(uri, system_name, user_id, exchange, password, symbol)
# Create thread for market data streaming
md_stream_thread = threading.Thread(target=md_thread_function, daemon=True)
# Start the md_stream_thread
md_stream_thread.start()
    
# Create Pnl streaming object
pnl_pos_app = PnlPosStreamApp(uri, system_name, user_id, password, fcm_id, ib_id, account_id)
# Create thread for Pnl and position streaming
pnl_pos_thread = threading.Thread(target=pnl_thread_function, daemon=True)
# Start the pnl_pos_thread
pnl_pos_thread.start()

# Download OHLC 1 sec data to a DataFrame
ohlc_df = get_hist_bars()

# Print historical DF
print('\nOHLC DF')
print(ohlc_df.tail())

# Intialize timestamps for session
refresh_session()

#################################################################
# MONITOR CONDITIONS AND EXECUTE STRATEGY                       #
#################################################################
try:

    while True:
        # Get current time
        current_time = dt.now()
        # Convert current_time to string
        current_time_str = current_time.strftime(tf)

        # Check if 15 seconds have passed since the last print
        if time.time() - last_print_time >= 15:
            print('\nDEBUG: Printing PNL and position,', current_time_str)
            print_pnl_pos()
            last_print_time = time.time()  # Update the last print time

        # Check if it's time to refresh the session and timestamps
        if current_time_str > end_time or current_time_str < start_time:
            refresh_session()
        
        # Extract the day of week
        day_integer = current_time.weekday()
        # Extract just the hour for the halt check
        current_hour = current_time.hour

        # Check for weekend closure or daily halt (between 17:00 and 18:00)
        if (day_integer == 4 and current_hour >= 17) or day_integer == 5 or (day_integer == 6 and current_hour < 18):
            print('Weekend: Market is closed...', current_time.strftime(tf))
            sleep(30)
            continue
        elif day_integer in range(0, 5) and current_hour == 17:  # Monday to Friday halt
            print('Daily halt: Market is closed...', current_time.strftime(tf))
            sleep(3600 - current_time.minute * 60 - current_time.second)  # Sleep until 18:00
            continue

        # Convert current_time to a string for comparison
        #current_time_str = current_time.strftime(tf) # NOT SURE THIS IS NEEDED.  IT MAY BE REDUNDANT. CHECK ABOVE.

        # Check if current time is before the strategy start time or after the end time
        if current_time < dt.strptime(start_time, tf) or current_time > dt.strptime(end_time, tf):
            print('Outside trading hours: Market is closed...', current_time_str)
            sleep(30)
            continue

        # If current time is within strategy run times, execute the algorithm
        if current_time_str in timestamps:
            
            # Initialize new_data as an empty DataFrame at the start of each loop.
            new_data = pd.DataFrame(columns=['Date', 'Close'])
            # Convert type of Date to DateTime. 
            new_data['Date'] = pd.to_datetime(new_data['Date'])
            # In order to add data later, the data type of the Close column must be set.
            new_data['Close'] = new_data['Close'].astype(float)

            # Lock the DataFrame for thread-safe operation 
            with md_stream_app.lock:
                # Retrieve new data beyond the last processed index
                if last_processed_index + 1 < len(md_stream_app.tick_df):
                    new_data = md_stream_app.tick_df.iloc[last_processed_index + 1:]
                    last_processed_index = md_stream_app.tick_df.index[-1]

            # If new_data has new data, then process it.
            if not new_data.empty:
                # Process the new data
                print('\nDEBUG: New data detected...\n')
                print('')
                print(new_data)
                print('')
                # Update ohlc_df
                ohlc_df = update_ohlc(new_data, ohlc_df, timeframe)
                # Display update
                print('\nOHLC Update')
                print(ohlc_df)
                print('')
                # Run strategy
                print('*' * 44)
                print('Running strategy:', current_time_str)
                run_strategy()
            else:
                print('*' * 44)
                print('DEBUG: No new data detected...\n')
                print('No need to run strategy', current_time_str)
                print('*' * 44)

            sleep(1)
            
except KeyboardInterrupt:
    print("\nKeyboardInterrupt received, shutting down threads...")
    md_stream_app.cleanup()
    shutdown_event.set()
    md_stream_thread.join()
    pnl_pos_app.cleanup()
    shutdown_event.set()
    pnl_pos_thread.join()

finally:
    # Uncomment if ohlc_df needs to be exported.
    # ohlc_df.to_csv('./ohlc_df.csv')
    print("Main thread exiting...")