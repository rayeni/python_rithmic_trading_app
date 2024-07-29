#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#################################################################
# LIBRARY IMPORTS                                               #
#################################################################

import asyncio
import pandas as pd
import google.protobuf.message
import pathlib
import ssl
import websockets
from datetime import datetime as dt, timedelta

from protobuf import base_pb2
from protobuf import request_heartbeat_pb2
from protobuf import request_login_pb2
from protobuf import response_login_pb2
from protobuf import request_logout_pb2
from protobuf import request_time_bar_replay_pb2
from protobuf import response_time_bar_replay_pb2


class HistBarsApp:

    def __init__(self, uri, system_name, user_id, password, exchange, symbol):
        self.uri = uri
        self.system_name = system_name
        self.user_id = user_id
        self.password = password
        self.exchange = exchange
        self.symbol = symbol
        self.g_rp_is_done = False
        self.bar_data = {}
        self.ohlc_df = None

    async def response_time_bar_replay_cb(self, msg_buf):
        '''
        Receive historical bars from Rithmic, 203.

        :param msg_buf: bytes
            A binary message sent from Rithmic's History Plant.
        '''
        
        
        msg = response_time_bar_replay_pb2.ResponseTimeBarReplay()
        msg.ParseFromString(msg_buf[4:])

        bar_type_to_string = {response_time_bar_replay_pb2.ResponseTimeBarReplay.BarType.SECOND_BAR   : "SECOND_BAR",
                              response_time_bar_replay_pb2.ResponseTimeBarReplay.BarType.MINUTE_BAR   : "MINUTE_BAR",
                              response_time_bar_replay_pb2.ResponseTimeBarReplay.BarType.DAILY_BAR    : "DAILY_BAR"}
        
        print(f"")
        print(f"         ResponseTimeBarReplay : ")
        print(f"                   template_id : {msg.template_id}")
        print(f"                      user_msg : {msg.user_msg}")
        print(f"            rq_handler_rp_code : {msg.rq_handler_rp_code}")
        print(f"                       rp_code : {msg.rp_code}")

        print(f"                        symbol : {msg.symbol}")
        print(f"                      exchange : {msg.exchange}")
    
        print(f"                          type : {bar_type_to_string[msg.type]} ({msg.type})")
        print(f"                        period : {msg.period}")
        print(f"                        marker : {msg.marker}")

        print(f"                    num_trades : {msg.num_trades}")
        print(f"                        volume : {msg.volume}")
        print(f"                    bid_volume : {msg.bid_volume}")
        print(f"                    ask_volume : {msg.ask_volume}")

        print(f"                    open_price : {msg.open_price}")
        print(f"                   close_price : {msg.close_price}")
        print(f"                    high_price : {msg.high_price}")
        print(f"                     low_price : {msg.low_price}")
        print(f"              settlement_price : {msg.settlement_price}")
        print(f"          has_settlement_price : {msg.has_settlement_price}")
        print(f"   must_clear_settlement_price : {msg.must_clear_settlement_price}")
        print(f"")

        # Local variable to hold timestamp
        dt_marker = ''

        # Collect bar data
        if (len(msg.rq_handler_rp_code) > 0 and
            len(msg.rp_code) == 0 and
            msg.close_price > 0):

            # Convert epoch to datetime_object, then convert to datetime string
            dt_marker = dt.fromtimestamp(msg.marker).strftime('%Y-%m-%d %H:%M:%S')

            # Add bar data to dictionary
            if msg.symbol not in self.bar_data:
                # Add new list to dict
                self.bar_data[msg.symbol] = [{'date': dt_marker, 
                                              'open': msg.open_price,
                                              'high': msg.high_price,
                                              'low': msg.low_price,
                                              'close': msg.close_price}]
            else:
                # Append new entry to existing list in dict
                self.bar_data[msg.symbol].append({'date': dt_marker,
                                                  'open': msg.open_price,
                                                  'high': msg.high_price,
                                                  'low': msg.low_price,
                                                  'close': msg.close_price})

        if len(msg.rq_handler_rp_code) == 0 and \
               len(msg.rp_code) > 0:
            
            # After all the bars have been stored in dict, convert dict to DataFrame
            self.ohlc_df = pd.DataFrame(self.bar_data[self.symbol])
            # Set the date column to the Index
            self.ohlc_df.set_index('date', inplace=True)
            # Change Index to DatetimeIndex
            self.ohlc_df.index = pd.to_datetime(self.ohlc_df.index)
            
            # Sort the DataFrame in ascending order
            self.ohlc_df.sort_index(ascending=True, inplace=True)
            print('')

            print(f"Time bar responses are done.")

            self.g_rp_is_done = True

    async def connect_to_rithmic(self, uri, ssl_context):
        '''
        Connect to the specified URI and return the websocket
        connection object.
        '''
        
        ws = await websockets.connect(uri, ssl=ssl_context, ping_interval=3)
        print(f"connected to {uri}")
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
        print(f"sent heartbeat request")

    #async def list_systems(self, ws):
        # Implementation of list_systems...

    async def consume(self, ws):
        '''
        Read data off the wire, occassionally send heartbeats if
        there is no traffic.

        :param ws: websocket
            Used to receive messages from Rithmic.  
        '''

        # send a heartbeat immediately, just in case
        await self.send_heartbeat(ws)

        max_num_msgs = 100000
        num_msgs = 0

        # After <max_num_msgs>  messages are read or the tick bar response is done,
        # this routine will exit
        while num_msgs < max_num_msgs and not self.g_rp_is_done :
            msg_buf = bytearray()

            waiting_for_msg = True
        
            while waiting_for_msg:
                try:
                    print(f"waiting for msg ...\n")
                    msg_buf = await asyncio.wait_for(ws.recv(), timeout=5)
                    waiting_for_msg = False
                except asyncio.TimeoutError:
                    if ws.open:
                        print(f"sending heartbeat ...\n")
                        await self.send_heartbeat(ws)
                    else:
                        print(f"connection appears to be closed.  exiting consume()")
                        return;

            num_msgs += 1

            print(f"received msg {num_msgs} of {max_num_msgs}")

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
                print(f" consumed msg : {msg_type} ({base.template_id})")
            
            # elif base.template_id == 101:
            #     msg_type = "market data update response"
            #     print(f" consumed msg : {msg_type} ({base.template_id})")

            # elif base.template_id == 151: # best_bid_offer
            #     msg_type = "best_bid_offer"
            #     print(f" consumed msg : {msg_type} ({base.template_id})")
            
            # elif base.template_id == 150: # last_trade
            #     msg_type = "last_trade"
            #     print(f" consumed msg : {msg_type} ({base.template_id})")

            elif base.template_id == 203:
                msg_type = "time bar replay response"
                print(f" consumed msg : {msg_type} ({base.template_id})")
                await self.response_time_bar_replay_cb(msg_buf)
    
            # elif base.template_id == 251:
            #     msg_type = "tick bar"
            #     print(f" consumed msg : {msg_type} ({base.template_id})")

            else:
                msg_type = "unrecognized template id"
                print(f" consumed msg : {msg_type} ({base.template_id})")

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
        rq.app_name    = "CHANGE_ME:hist_bars_ohlc.py"
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

        print(f"")
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
        print(f"")        

    async def replay_time_bars(self, ws):
        '''
        Request the replay of time bars.

        :param ws: websocket
            Used to send and receive Rithmic messages.
        '''

        current_time = dt.now()
        one_hour_prior = current_time - timedelta(hours=1)
        two_hours_prior = current_time - timedelta(hours=2)
        three_hours_prior = current_time - timedelta(hours=3)
        four_hours_prior = current_time - timedelta(hours=4)
        seventytwo_hours_prior = current_time - timedelta(hours=72)
        ninetysix_hours_prior = current_time - timedelta(hours=96)
        twosixteen_hours_prior = current_time - timedelta(hours=216)
        epoch_now = int(current_time.timestamp())
        epoch_1prior = int(one_hour_prior.timestamp())
        epoch_2prior = int(two_hours_prior.timestamp())
        epoch_3prior = int(three_hours_prior.timestamp())
        epoch_4prior = int(four_hours_prior.timestamp())
        epoch_72prior = int(seventytwo_hours_prior.timestamp())
        epoch_96prior = int(ninetysix_hours_prior.timestamp())
        epoch_216prior = int(twosixteen_hours_prior.timestamp())

        rq = request_time_bar_replay_pb2.RequestTimeBarReplay()

        rq.template_id      = 202;
        rq.user_msg.append("hello")

        rq.symbol       = self.symbol
        rq.exchange     = self.exchange
        
        rq.bar_type     = 2 # 1=SECOND_BAR, 2=MINUTE_BAR, 3=DAILY_BAR, 4=WEEKLY_BAR
        rq.bar_type_period = 5
    
        rq.start_index  = epoch_216prior
        rq.finish_index = epoch_now

        rq.direction = 2
        rq.time_order = 2
        rq.resume_bars = True

        serialized = rq.SerializeToString()
        length     = len(serialized)

        buf  = bytearray()
        buf  = length.to_bytes(4, byteorder = 'big', signed=True)
        buf += serialized

        await ws.send(buf)

    async def rithmic_logout(self, ws):
        '''
        Log out from History Plant.

        :param ws: websocket
            Used to send message to Rithmic.
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
        Disconnect from History Plant.

        :param ws: websocket
            Used to close connection.
        '''

        await ws.close(1000, "see you tomorrow")

    def run(self):
        '''
        Start loop to replay time bars.
        '''

        loop = asyncio.get_event_loop()

        # check if we should use ssl/tls 
        ssl_context = None
        if "wss://" in self.uri:
            # Set up the ssl context. One can also use an alternate SSL/TLS cert file or database
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            localhost_pem = pathlib.Path(__file__).with_name("rithmic_ssl_cert_auth_params")
            ssl_context.load_verify_locations(localhost_pem)

        ws = loop.run_until_complete(self.connect_to_rithmic(self.uri, ssl_context))

        loop.run_until_complete(self.rithmic_login(ws, request_login_pb2.RequestLogin.SysInfraType.HISTORY_PLANT))

        loop.run_until_complete(self.replay_time_bars(ws))

        loop.run_until_complete(self.consume(ws))

        if ws.open:
            print(f"Logging out from History Plant...")
            loop.run_until_complete(self.rithmic_logout(ws))
            print(f"Disconnecting from History Plant...")
            loop.run_until_complete(self.disconnect_from_rithmic(ws))
            print(f"Done!")
        else:
            print(f"Connection appears to be closed.  Exiting app.")