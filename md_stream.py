#!/usr/bin/env python3

#################################################################
# LIBRARY IMPORTS                                               #
#################################################################

import asyncio
import threading
import random
import traceback
import google.protobuf.message
import pathlib
import ssl
import websockets
import pandas as pd
from datetime import datetime as dt, timedelta

from protobuf import base_pb2
from protobuf import request_heartbeat_pb2
from protobuf import request_rithmic_system_info_pb2
from protobuf import response_rithmic_system_info_pb2
from protobuf import request_login_pb2
from protobuf import response_login_pb2
from protobuf import request_logout_pb2
from protobuf import request_market_data_update_pb2
from protobuf import response_market_data_update_pb2
from protobuf import last_trade_pb2

class MdStreamApp:
    def __init__(self, uri, system_name, user_id, exchange, password, symbol):
        self.uri = uri
        self.system_name = system_name
        self.user_id = user_id
        self.exchange = exchange
        self.password = password
        self.symbol = symbol
        self.unique_user_id = ''
        self.heartbeat_task = None
        self.exit_consume = False
        self.ssl_context = None
        self.tick_df = pd.DataFrame(columns=['Date', 'Close'])
        self.tick_df['Date'] = pd.to_datetime(self.tick_df['Date'])
        self.tick_df['Close'] = self.tick_df['Close'].astype(float)
        self.lock = threading.Lock()
    
    def cleanup(self):
        '''
        Clean up processes that are run by an 
        external thread that has been stopped.
        '''

        # Code to gracefully shutdown connections and tasks
        self.exit_consume = True

        print("md_stream consumed() stopped...")

    async def reestablish_state(self, ws):
        '''
        Re-establish any necessary state here 
        (e.g., log in, subscribe to symbols)
        '''
        
        await self.rithmic_login(ws, request_login_pb2.RequestLogin.SysInfraType.TICKER_PLANT)
        
        await self.resubscribe_to_symbols(ws)
    
    async def reconnect(self):
        '''
        Reconnect to the specified URI and return the websocket
        connection object.
        '''

        backoff = 1
        # Maximum backoff time in seconds
        max_backoff = 32

        while True:
            try:
                print(f"Attempting to reconnect...")
                ws = await websockets.connect(self.uri, ssl=self.ssl_context)
                print(f"Reconnected.")
                return ws
            except (asyncio.TimeoutError, websockets.exceptions.WebSocketException) as e:
                print(f"Reconnection failed: {e}")
                await asyncio.sleep(backoff)
                # Exponential backoff with jitter
                backoff = min(backoff * 2, max_backoff) + random.uniform(0, 1)    

    async def resubscribe_to_symbols(self, ws):
        '''
        Resubscribe to symbol after reconnecting to Rithmic.
        '''

        # Subscribe to symbol
        await self.subscribe(ws)

    async def connect_to_rithmic(self):
        '''
        Connect to the specified URI and return the websocket
        connection object.
        '''

        # disable ping keep-alive mechanism
        ws = await websockets.connect(self.uri, ssl=self.ssl_context, ping_interval=3)
        print(f"connected to {self.uri}")
        return (ws)

    async def send_heartbeat(self, ws):
        '''
        Send a heartbeat request.  

        :param ws: websocket
            Used to send message to Rithmic.
        '''

        rq = request_heartbeat_pb2.RequestHeartbeat()

        rq.template_id = 18

        serialized = rq.SerializeToString()
        length     = len(serialized)
            
        # length into bytes (4 bytes, big/little, true/false)
        buf  = bytearray()
        buf  = length.to_bytes(4, byteorder='big', signed=True)
        buf += serialized

        await ws.send(buf)
        #print(f"sent heartbeat request")

    async def list_systems(self, ws):
        '''
        Request the list of available Rithmic systems, and wait for
        the response from the server.  After this request is processed by the
        server, the server will initiate the closing of the websocket connection.

        :param ws: websocket
            Used to send and receive message to and from Rithmic.
        '''

        rq = request_rithmic_system_info_pb2.RequestRithmicSystemInfo()

        rq.template_id = 16
        rq.user_msg.append("hello");
        rq.user_msg.append("world");

        serialized = rq.SerializeToString()
        length     = len(serialized)
            
        # length into bytes (4 bytes, big/little, true/false)
        buf  = bytearray()
        buf  = length.to_bytes(4, byteorder='big', signed=True)
        buf += serialized

        await ws.send(buf)
        print(f"sent list_systems request")

        rp_buf = bytearray()
        rp_buf = await ws.recv()

        # get length from first four bytes from rp_buf
        rp_length = int.from_bytes(rp_buf[0:3], byteorder='big', signed=True)

        rp = response_rithmic_system_info_pb2.ResponseRithmicSystemInfo()
        rp.ParseFromString(rp_buf[4:])

        # an rp code of "0" indicates that the request was completed successfully
        if rp.rp_code[0] == "0":
            print(f" Available Systems :")
            print(f" ===================")
            for sys_name in rp.system_name:
                print(f"{sys_name}")
        else:
            print(f" error retrieving system list :")
            print(f" template_id : {rp.template_id}")
            print(f"    user_msg : {rp.user_msg}")
            print(f"     rp code : {rp.rp_code}")
            print(f" system_name : {rp.system_name}")

    async def consume(self, ws):
        '''
        Read data off the wire, occassionally send heartbeats if
        there is no traffic.

        :param ws: websocket
            Used to receive messages from Rithmic.  
        '''

        last_traded_price = 0.0

        # Local variable to hold timestamp
        dt_marker = ''

        # Local variable to hold new tick price
        new_price = 0.0

        # Local variable/dict to hold new tick data {Date, Close}
        new_data = {}

        # Local variable/DataFrame used to convert new_data dict to DataFrame
        temp_df = pd.DataFrame()

        # send a heartbeat immediately, just in case
        await self.send_heartbeat(ws)

        # After 100000 messages are read or time reaches 4:59 p.m., this routine will exit
        while self.exit_consume == False:
            msg_buf = bytearray()

            waiting_for_msg = True
            
            while waiting_for_msg:
                try:
                    # Uncomment to DEBUG
                    #print(f"\nwaiting for msg ...")
                    msg_buf = await asyncio.wait_for(ws.recv(), timeout=5)
                    waiting_for_msg = False
                except asyncio.TimeoutError:
                    if not ws.open:
                        print(f"connection appears to be closed. attempting to reconnect...")
                        ws = await self.reconnect()
                        await self.reestablish_state(ws)
                        # Cancel existing heartbeat task and start a new one
                        if self.heartbeat_task:
                            self.heartbeat_task.cancel()
                        self.heartbeat_task = asyncio.create_task(self.heartbeat_loop(ws))
                except (websockets.exceptions.ConnectionClosed, 
                        websockets.exceptions.ConnectionClosedError) as e:
                    print(f"Connection closed with error: {e}. Attempting to reconnect...")
                    ws = await self.reconnect()
                    await self.reestablish_state(ws)
                    # Similar handling for heartbeat_task as above
                    if self.heartbeat_task:
                        self.heartbeat_task.cancel()
                    self.heartbeat_task = asyncio.create_task(self.heartbeat_loop(ws))
                
            # get length from first four bytes from msg_buf
            msg_length = int.from_bytes(msg_buf[0:3], byteorder='big', signed=True)

            # parse into base class just to get a template id
            base = base_pb2.Base()
            base.ParseFromString(msg_buf[4:])

            # route msg based on template id
            if base.template_id == 13:
                msg_type = "logout response"
                print(f" consumed msg : {msg_type} ({base.template_id})")
                
            elif base.template_id == 19:
                msg_type = "heartbeat response"
                #print(f" consumed msg : {msg_type} ({base.template_id})")
                
            elif base.template_id == 101:
                msg = response_market_data_update_pb2.ResponseMarketDataUpdate()
                msg.ParseFromString(msg_buf[4:])
                print(f"")
                print(f" ResponseMarketDataUpdate : ")
                print(f"                 user_msg : {msg.user_msg}")
                print(f"                  rp_code : {msg.rp_code}")
        
            elif base.template_id == 150: # last_trade

                msg = last_trade_pb2.LastTrade()
                msg.ParseFromString(msg_buf[4:])
                
                is_last_trade     = True if msg.presence_bits & last_trade_pb2.LastTrade.PresenceBits.LAST_TRADE else False

                # Capture last traded price
                if msg.trade_price > 0 and is_last_trade == True:

                    # Convert epoch to datetime_object, then convert to datetime string
                    dt_marker = dt.fromtimestamp(msg.ssboe).strftime('%Y-%m-%d %H:%M:%S')

                    # Capture new tick price
                    new_price = msg.trade_price

                    if new_price != last_traded_price:
                        print(f"\nDEBUG: md_stream: Printing last trade info...")
                        print(f"\nLastTrade : ")
                        print(f"symbol    : {msg.symbol}")
                        print(f"new_price : {new_price}")
                        print(f"Date      : {dt_marker}")
                        with self.lock:
                            # Create a new dict
                            new_data = {'Date': dt_marker, 'Close': new_price}
                            # Create temp df with 'Date' as the index immediately
                            temp_df = pd.DataFrame([new_data])
                            # Convert 'Date' to datetime
                            temp_df['Date'] = pd.to_datetime(temp_df['Date'])
                            # Set Close to float
                            temp_df['Close'] = temp_df['Close'].astype(float)
                            # Concatenate temp_df to tick_df
                            self.tick_df = pd.concat([self.tick_df, temp_df], ignore_index=True)
                            # Update last_traded_price with latest price in self.tick_df
                            last_traded_price = new_price

    async def rithmic_login(self, ws, infra_type):
        '''
        Log into the specified Rithmic system using the specified
        credentials.  It will also wait for the login response.

        :param ws: websocket
            Used to send and receive messages to and from Rithmic.
        
        :param infra_type: SysInfraType
            The plant to which you are requesting login.
        '''

        rq = request_login_pb2.RequestLogin()

        rq.template_id      = 10;
        rq.template_version = "3.9"
        rq.user_msg.append("hello")

        rq.user        = self.user_id
        rq.password    = self.password
        rq.app_name    = "CHANGE_ME:md_stream.py"
        rq.app_version = "1.0.0"
        rq.system_name = self.system_name
        rq.infra_type  = infra_type

        serialized = rq.SerializeToString()
        length     = len(serialized)

        buf  = bytearray()
        buf  = length.to_bytes(4, byteorder = 'big', signed=True)
        buf += serialized

        await ws.send(buf)

        rp_buf = bytearray()
        rp_buf = await ws.recv()

        # get length from first four bytes from rp_buf
        rp_length = int.from_bytes(rp_buf[0:3], byteorder='big', signed=True)

        rp = response_login_pb2.ResponseLogin()
        rp.ParseFromString(rp_buf[4:])

        print(f"      ResponseLogin :")
        print(f"      ===============")
        print(f"        template_id : {rp.template_id}")
        print(f"   template_version : {rp.template_version}")
        print(f"           user_msg : {rp.user_msg}")
        print(f"            rp code : {rp.rp_code}")
        print(f"             fcm_id : {rp.fcm_id}")
        print(f"             ib_id  : {rp.ib_id}")
        print(f"       country_code : {rp.country_code}")
        print(f"         state_code : {rp.state_code}")
        print(f" heartbeat_interval : {rp.heartbeat_interval}")
        print(f"     unique_user_id : {rp.unique_user_id}")

        self.unique_user_id = rp.unique_user_id
    
    async def subscribe(self, ws):
        '''
        Subscribe to best bid/offer or trade market data for the
        specified instrument.  Any received messages from this 
        subscription request are handled elsewhere (see the consume() routine)
        '''

        rq = request_market_data_update_pb2.RequestMarketDataUpdate()

        rq.template_id      = 100;
        rq.user_msg.append("hello")

        rq.symbol      = self.symbol
        rq.exchange    = self.exchange
        rq.request     = request_market_data_update_pb2.RequestMarketDataUpdate.Request.SUBSCRIBE
        rq.update_bits = request_market_data_update_pb2.RequestMarketDataUpdate.UpdateBits.LAST_TRADE

        serialized = rq.SerializeToString()
        length     = len(serialized)

        buf  = bytearray()
        buf  = length.to_bytes(4, byteorder = 'big', signed=True)
        buf += serialized

        await ws.send(buf)
    
    async def heartbeat_loop(self, ws):
        '''
        Start and continue process to send heartbeat to Rithmic.
        '''
        while True:
            try:
                await asyncio.sleep(4)  # Send heartbeat every 30 seconds, adjust as needed
                await self.send_heartbeat(ws)
            except asyncio.CancelledError:
                print("Heartbeat task cancelled")
                break
            except Exception as e:
                print(f"Error in heartbeat loop: {e}")
                # Decide how to handle unexpected errors, e.g., break or continue

    async def unsubscribe(self, ws):
        '''
        Unsubscribe from best bid/offer and trade market data for the
        specified instrument.
        '''

        rq = request_market_data_update_pb2.RequestMarketDataUpdate()

        rq.template_id      = 100;
        rq.user_msg.append("hello")

        rq.symbol      = self.symbol
        rq.exchange    = self.exchange
        rq.request     = request_market_data_update_pb2.RequestMarketDataUpdate.Request.UNSUBSCRIBE
        rq.update_bits = request_market_data_update_pb2.RequestMarketDataUpdate.UpdateBits.LAST_TRADE

        serialized = rq.SerializeToString()
        length     = len(serialized)

        buf  = bytearray()
        buf  = length.to_bytes(4, byteorder = 'big', signed=True)
        buf += serialized

        await ws.send(buf)

    async def rithmic_logout(self, ws):
        '''
        Sends a logout request. Do not wait for a response.
        '''
        
        rq = request_logout_pb2.RequestLogout()

        rq.template_id      = 12;
        rq.user_msg.append("hello")

        serialized = rq.SerializeToString()
        length     = len(serialized)

        buf = bytearray()
        buf = length.to_bytes(4, byteorder = 'big', signed=True)
        buf += serialized

        await ws.send(buf)

    async def disconnect_from_rithmic(self, ws):
        '''
        Closes the websocket connection.  The status code is
        hard-coded to 1000, indicating a normal closure.
        '''
        await ws.close(1000, "see you tomorrow")   

    def run(self):

        loop = asyncio.get_event_loop()

        # check if we should use ssl/tls
        #ssl_context = None
        if "wss://" in self.uri:
            # Set up the ssl context.  One can also use an alternate SSL/TLS cert file
            # or database
            self.ssl_context   = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            localhost_pem = pathlib.Path(__file__).with_name("rithmic_ssl_cert_auth_params")
            self.ssl_context.load_verify_locations(localhost_pem)

        ws = loop.run_until_complete(self.connect_to_rithmic())
                
        loop.run_until_complete(self.rithmic_login(ws, request_login_pb2.RequestLogin.SysInfraType.TICKER_PLANT,))

        loop.run_until_complete(self.subscribe(ws))

        # Start the heartbeat task
        self.heartbeat_task = loop.create_task(self.heartbeat_loop(ws))

        try:
            loop.run_until_complete(self.consume(ws))
        except Exception as e:
            print(traceback.print_exc())
            print(e)
            pass
        finally:
            # If the loop is interrupted or finishes, ensure the heartbeat task is cancelled
            self.heartbeat_task.cancel()
            try:
                # Await the task to handle its cancellation
                loop.run_until_complete(self.heartbeat_task)
            except asyncio.CancelledError:
                # Expect the CancelledError and ignore it, as it's a normal part of cancelling a task
                pass

            if ws.open:
                print(f"Unsubscribing from market data...")
                loop.run_until_complete(self.unsubscribe(ws))
                print(f"Logging out from Ticker Plant...")
                loop.run_until_complete(self.rithmic_logout(ws))
                print(f"Disconnecting from Ticker Plant...")
                loop.run_until_complete(self.disconnect_from_rithmic(ws))
                #print('Exporting tick data to csv...')
                #self.tick_df.to_csv('./tick_data.csv')
                print(f"Done!")
                print("")
            else:
                print(f"Connection appears to be closed. Exiting app.")