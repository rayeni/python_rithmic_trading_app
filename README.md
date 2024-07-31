# Python Rithmic API Trading App

* This is a minimum viable product (MVP).
* It provides access to the following:
    * Tick-level market data.
    * Historical data.
    * PnL and position status.
    * Order execution.
* You must provide your own trading strategy and execution logic. You can place it in the `run_strategy()` function in `rithmic_trading_app.py`.
* The app is configured to place market orders only.
* The app is configured to execute the strategy placeholder every 5-minutes.

## Prequisites

* You must have either a Rithmic Paper Trading or Rithmic Live account, **NOT A TEST ACCOUNT**.
* You must pass Rithmic's conformance testing.
* After passing conformance testing, Rithmic will send you a four-character prefix. 
* In `md_stream.py`, `pos_pnl_stream.py`, `hist_bars_ohlc.py`, and `place_orders.py`, find the following variable:
    * `rq.app_name`
    * Update its value by replacing `CHANGE_ME` with the prefix issued by Rithmic.
    
## Installation

Download the repo to your hard drive.

## Start App

After downloading the repo, `cd` to `python_rithmic_trading_app`.

Run the following command:


python rithmic_trading_app.py [username] [password] [exchange] [contract symbol] [contact qty] [FCM] [IB]


For example, if your Rithmic credentials are **00000000-DEMO/password123** and you want the trading app to monitor the **Crude Oil** market and to trade only **one quantity** of the **December 2024** contract, through your **IB (e.g., Ironbeam) and FCM (e.g., Ironbeam)**, then you would run the following command:

```
python rithmic_trading_app.py 00000000-DEMO password123 NYMEX CLZ4 1 Ironbeam Ironbeam
``` 

After starting the trading app, you will see a series of **DEBUG** messages indicating that the app is receiving tick data, monitoring PnL and position data, and is executing the strategy placeholder every five minutes.

## Stop App

To stop the app, issue a KeyboardInterrupt, `Ctrl+C`.

## Live Trading

After thoroughly testing your trading strategy and execution logic, if you wish to trade live, do the following:

* In `rithmic_trading_app.py`, find the variable `system_name`.
* Change its value from `Rithmic Paper Trading` to `Rithmic 01`.
     


