# Python Rithmic Trading App

* This is a minimum viable product (MVP).
* It provides access the following:
    * Tick-level market data.
    * Historical data.
    * PnL and position status.
    * Order execution.
* You must provide your own trading strategy and execution logic. Place it in `run_strategy()` in `rithmic_trading_app.py`.
* The app is configured to place market orders only.
* The app is configured to execute the strategy placeholder every 5-minutes.

## Prequisites

* You must have either a Rithmic Paper Trading or Rithmic Live account, NOT A TEST ACCOUNT.
* You must pass Rithmic's conformance testing.
* After passing conformance testing, Rithmic will send you a four-character prefix. 
* In `md_stream.py`, `pos_pnl_stream.py`, `hist_bars_ohlc.py`, and `place_orders.py`, find the following variable:
    * `rq.app_name`
    * Update it's value by replacing `CHANGE_ME` with the prefix issued by Rithmic.
    
## Installation

Download the repo to your hard drive.

## Start App

After downloading the app, `cd` to `python_rithmic_trading_app`.

Run the following command:


python rithmic_trading_app.py [username] [password] [exchange] [contract symbol] [contact qty] [FCM] [IB]


For example, if my Rithmic credentials are 00000000-DEMO/password123 and I want the trading app to monitor the Crude Oil market and to trade only one December 2024 contract, then I would run the following command:

```
python rithmic_trading_app.py 00000000-DEMO password123 NYMEX CLZ4 1 Ironbeam Ironbeam
``` 

After starting the trading app, you see a series of DEBUG messages indicticating that the app is receiving tick data, monitoring PnL and position data, and is executing the strategy placeholder.

     


