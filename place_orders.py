#!/usr/bin/env python3
'''
Created on August 4, 2024

Author: Rufus Ayeni

Contact: https://github.com/rayeni/python_rithmic_trading_app/discussions
'''

#################################################################
# LIBRARY IMPORTS                                               #
#################################################################

import asyncio
import google.protobuf.message
import pathlib
import ssl
import websockets

from protobuf import base_pb2
from protobuf import request_account_list_pb2
from protobuf import response_account_list_pb2
from protobuf import request_heartbeat_pb2
from protobuf import request_rithmic_system_info_pb2
from protobuf import response_rithmic_system_info_pb2
from protobuf import request_login_pb2
from protobuf import response_login_pb2
from protobuf import request_login_info_pb2
from protobuf import response_login_info_pb2
from protobuf import request_logout_pb2
from protobuf import request_trade_routes_pb2
from protobuf import response_trade_routes_pb2
from protobuf import request_subscribe_for_order_updates_pb2
from protobuf import request_new_order_pb2
from protobuf import exchange_order_notification_pb2
from protobuf import rithmic_order_notification_pb2

class PlaceOrderApp:
    def __init__(self, uri, system_name, user_id, password, exchange, symbol):
        self.uri = uri
        self.system_name = system_name
        self.user_id = user_id
        self.password = password
        self.g_exchange = exchange
        self.g_symbol = symbol
        self.g_rcvd_account = False
        self.g_fcm_id = ""
        self.g_ib_id = ""
        self.g_account_id = ""
        self.g_rcvd_trade_route = False
        self.g_trade_route = ""
        self.g_order_is_complete = False
        self.ssl_context = None

    async def rithmic_order_notification_cb(self, msg_buf):
        '''
        Interpret the msg_buf as a RithmicOrderNotification, 351.

        :param msg_buf: bytes
            A binary message sent from Rithmic's order plant.
        '''
        
        msg = rithmic_order_notification_pb2.RithmicOrderNotification()
        msg.ParseFromString(msg_buf[4:])

        print(f"")
        print(f" RithmicOrderNotification : ")
        print(f"                   status : {msg.status}")

        if msg.status == "complete" or \
           msg.notify_type == rithmic_order_notification_pb2.RithmicOrderNotification.COMPLETE:
            self.g_order_is_complete = True
        
    async def exchange_order_notification_cb(self, msg_buf):
        '''
        Interpret the msg_buf as a ExchangeOrderNotification, 352.

        :param msg_buf: bytes
            A binary message sent from Rithmic's order plant.
        '''
        
        msg = exchange_order_notification_pb2.ExchangeOrderNotification()
        msg.ParseFromString(msg_buf[4:])

        print(f"")
        print(f" ExchangeOrderNotification : ")
        print(f"                   status : {msg.status}")

    async def connect_to_rithmic(self):
        '''
        Connect to the specified URI and return a 
        websocket connection object.
        '''
        
        # disable ping keep-alive mechanism
        ws = await websockets.connect(self.uri, ssl=self.ssl_context, ping_interval=3)
        print(f"connected to {self.uri}")
        return (ws)

    async def send_heartbeat(self, ws):
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

    async def list_systems(self, ws):
        '''
        Request the list of available Rithmic systems from the server.
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
        Read data off the wire. Occassionally send heartbeats if
        there is no traffic.
        '''
        # send a heartbeat immediately, just in case
        await self.send_heartbeat(ws)

        max_num_msgs = 200000
        num_msgs = 0

        # After 100 messages are read, this routine will exit
        while num_msgs < max_num_msgs and self.g_order_is_complete == False:
            msg_buf = bytearray()

            waiting_for_msg = True

            while waiting_for_msg:
                try:
                    #print(f"waiting for msg ...")
                    msg_buf = await asyncio.wait_for(ws.recv(), timeout=5)
                    waiting_for_msg = False
                except asyncio.TimeoutError:
                    if ws.open:
                        print(f"sending heartbeat ...")
                        await self.send_heartbeat(ws)
                    else:
                        print(f"connection appears to be closed.  exiting consume()")
                        return;

            num_msgs += 1

            # get length from first four bytes from msg_buf
            msg_length = int.from_bytes(msg_buf[0:3], byteorder='big', signed=True)

            # parse into base class just to get a template id
            base = base_pb2.Base()
            base.ParseFromString(msg_buf[4:])

            # route msg based on template id
            if base.template_id == 13:
                msg_type = "logout response"
                #print(f" consumed msg : {msg_type} ({base.template_id})")

            elif base.template_id == 19:
                msg_type = "heartbeat response"
                #print(f" consumed msg : {msg_type} ({base.template_id})")

            elif base.template_id == 309: # response to subscribe_for_order_updates
                msg_type = "response_subscribe_for_order_updates"
                #print(f" consumed msg : {msg_type} ({base.template_id})")

            elif base.template_id == 313: # response to new_order
                msg_type = "response_new_order"
                #print(f" consumed msg : {msg_type} ({base.template_id})")

            elif base.template_id == 351: # rithmic_order_notification
                msg_type = "rithmic_order_notification"
                #print(f" consumed msg : {msg_type} ({base.template_id})")

                await self.rithmic_order_notification_cb(msg_buf)

            elif base.template_id == 352: # exchange_order_notification
                msg_type = "exchange_order_notification"
                #print(f" consumed msg : {msg_type} ({base.template_id})")

                await self.exchange_order_notification_cb(msg_buf)

        if self.g_order_is_complete == True:
            print(f"order is complete ...")

    async def rithmic_login(self, ws, infra_type):
        '''
        Log into the specified Rithmic system using the specified
        credentials and wait for the login response.
        '''

        rq = request_login_pb2.RequestLogin()

        rq.template_id      = 10;
        rq.template_version = "3.9"
        rq.user_msg.append("hello")

        rq.user        = self.user_id
        rq.password    = self.password
        rq.app_name    = "CHANGE_ME:place_orders.py"
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

    async def login_info(self, ws):
        '''
        Retrieve additional info about the currently logged in user.
        '''
    
        rq = request_login_info_pb2.RequestLoginInfo()
    
        rq.template_id = 300;
        rq.user_msg.append("hello")
    
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
    
        rp = response_login_info_pb2.ResponseLoginInfo()
        rp.ParseFromString(rp_buf[4:])
    
        if rp.rp_code[0] == '0':
            print(f"retrieving account list ...")
            await self.list_accounts(ws, rp.fcm_id, rp.ib_id, rp.user_type)
    
            print(f"retrieving trade routes ...")
            await self.list_trade_routes(ws)

    async def list_accounts(self, ws, fcm_id, ib_id, user_type):
        '''
        Retrieve the list of accounts that the currently logged in
        user has permission to trade on.
        '''

        rq = request_account_list_pb2.RequestAccountList()

        rq.template_id = 302;
        rq.user_msg.append("hello")
        rq.fcm_id      = fcm_id
        rq.ib_id       = ib_id
        rq.user_type   = user_type

        serialized = rq.SerializeToString()
        length     = len(serialized)

        buf  = bytearray()
        buf  = length.to_bytes(4, byteorder = 'big', signed=True)
        buf += serialized

        rp_is_done = False

        await ws.send(buf)

        rp_buf = bytearray()

        while rp_is_done == False:
            rp_buf = await ws.recv()

            # get length from first four bytes from rp_buf
            rp_length = int.from_bytes(rp_buf[0:3], byteorder='big', signed=True)

            rp = response_account_list_pb2.ResponseAccountList()
            rp.ParseFromString(rp_buf[4:])

            # store the first acount we get for placing the order
            if self.g_rcvd_account == False    and \
               len(rp.rq_handler_rp_code) > 0  and \
               rp.rq_handler_rp_code[0] == "0" and \
               len(rp.fcm_id) > 0              and \
               len(rp.ib_id) > 0               and \
               len(rp.account_id) > 0:
                self.g_fcm_id       = rp.fcm_id
                self.g_ib_id        = rp.ib_id
                self.g_account_id   = rp.account_id
                self.g_rcvd_account = True

            if len(rp.rp_code) > 0:
                rp_is_done = True

    async def list_trade_routes(self, ws):
        '''
        Retrieves the list of trade routes from the order plant.
        '''
        
        rq = request_trade_routes_pb2.RequestTradeRoutes()

        rq.template_id = 310;
        rq.user_msg.append("hello")
        rq.subscribe_for_updates = False

        serialized = rq.SerializeToString()
        length     = len(serialized)

        buf  = bytearray()
        buf  = length.to_bytes(4, byteorder = 'big', signed=True)
        buf += serialized

        rp_is_done = False

        await ws.send(buf)

        rp_buf = bytearray()

        while rp_is_done == False:
            rp_buf = await ws.recv()

            # get length from first four bytes from rp_buf
            rp_length = int.from_bytes(rp_buf[0:3], byteorder='big', signed=True)

            rp = response_trade_routes_pb2.ResponseTradeRoutes()
            rp.ParseFromString(rp_buf[4:])

            # store the first applicable trade route we get
            if self.g_rcvd_trade_route == False and \
               len(rp.rq_handler_rp_code) > 0   and \
               rp.rq_handler_rp_code[0] == "0"  and \
               self.g_fcm_id   == rp.fcm_id     and \
               self.g_ib_id    == rp.ib_id      and \
               self.g_exchange == rp.exchange:
                self.g_trade_route      = rp.trade_route
                self.g_rcvd_trade_route = True

            if len(rp.rp_code) > 0:
                rp_is_done = True

    async def subscribe_for_order_updates(self, ws):
        '''
        Subscribe to updates on any orders on the specified fcm, ib
        and account.
        '''

        rq = request_subscribe_for_order_updates_pb2.RequestSubscribeForOrderUpdates()

        rq.template_id      = 308;
        rq.user_msg.append("hello")

        rq.fcm_id     = self.g_fcm_id
        rq.ib_id      = self.g_ib_id
        rq.account_id = self.g_account_id

        serialized = rq.SerializeToString()
        length     = len(serialized)

        buf  = bytearray()
        buf  = length.to_bytes(4, byteorder = 'big', signed=True)
        buf += serialized

        await ws.send(buf)

    async def new_order(self, ws, side, qty):
        '''
        Submit a request for a new order.
        '''

        rq = request_new_order_pb2.RequestNewOrder()

        rq.template_id      = 312;
        rq.user_msg.append("hello")

        rq.fcm_id     = self.g_fcm_id
        rq.ib_id      = self.g_ib_id
        rq.account_id = self.g_account_id

        rq.exchange   = self.g_exchange
        rq.symbol     = self.g_symbol

        rq.quantity   = qty

        if side == "B" or side == "b":
            rq.transaction_type = request_new_order_pb2.RequestNewOrder.TransactionType.BUY
        else:
            rq.transaction_type = request_new_order_pb2.RequestNewOrder.TransactionType.SELL
        rq.duration         = request_new_order_pb2.RequestNewOrder.Duration.DAY
        rq.price_type       = request_new_order_pb2.RequestNewOrder.PriceType.MARKET
        rq.manual_or_auto   = request_new_order_pb2.RequestNewOrder.OrderPlacement.AUTO

        rq.trade_route = self.g_trade_route

        serialized = rq.SerializeToString()
        length     = len(serialized)

        buf  = bytearray()
        buf  = length.to_bytes(4, byteorder = 'big', signed=True)
        buf += serialized

        await ws.send(buf)

    async def rithmic_logout(self, ws):
        '''
        Send a logout request
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
        Close the websocket connection.
        '''

        await ws.close(1000, "see you tomorrow")

    def run(self, side, qty):
        '''
        Start task to submit order.
        '''

        loop = asyncio.get_event_loop()

        #ssl_context = None
        if "wss://" in self.uri:
            # Set up the ssl context.  
            self.ssl_context   = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            localhost_pem = pathlib.Path(__file__).with_name("rithmic_ssl_cert_auth_params")
            self.ssl_context.load_verify_locations(localhost_pem)

        ws = loop.run_until_complete(self.connect_to_rithmic())

        loop.run_until_complete(self.rithmic_login(ws,request_login_pb2.RequestLogin.SysInfraType.ORDER_PLANT))

        loop.run_until_complete(self.login_info(ws))

        if self.g_rcvd_account and self.g_rcvd_trade_route:

            loop.run_until_complete(self.subscribe_for_order_updates(ws))
            print('Entering new_order')
            loop.run_until_complete(self.new_order(ws, side, qty))
            print('Exiting new_order')
            loop.run_until_complete(self.consume(ws))

        if ws.open:
            print(f"Logging out from Order Plant...")
            loop.run_until_complete(self.rithmic_logout(ws))
            print(f"Disconnecting from Order Plant...")
            loop.run_until_complete(self.disconnect_from_rithmic(ws))
            print(f"Done!")
        else:
            print(f"Connection appears to be closed.  Exiting app.")
