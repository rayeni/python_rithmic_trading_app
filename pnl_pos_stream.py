#!/usr/bin/env python3

#################################################################
# LIBRARY IMPORTS                                               #
#################################################################

import asyncio
import threading
import random
import google.protobuf.message
import pathlib
import ssl
import websockets
import traceback
from datetime import datetime as dt, timedelta

from protobuf import base_pb2
from protobuf import request_heartbeat_pb2
from protobuf import request_rithmic_system_info_pb2
from protobuf import response_rithmic_system_info_pb2
from protobuf import request_login_pb2
from protobuf import response_login_pb2
from protobuf import request_logout_pb2
from protobuf import request_pnl_position_updates_pb2
from protobuf import response_pnl_position_updates_pb2
from protobuf import instrument_pnl_position_update_pb2
from protobuf import account_pnl_position_update_pb2

class PnlPosStreamApp:
    def __init__(self, uri, system_name, user_id, password, fcm_id, ib_id, account_id):
        self.uri = uri
        self.system_name = system_name
        self.user_id = user_id
        self.password = password
        self.fcm_id = fcm_id
        self.ib_id = ib_id
        self.account_id = account_id
        self.daily_pnl = 0.0
        self.open_position_pnl = 0.0
        self.close_position_pnl = 0.0
        self.rp_is_done = False
        self.current_position = 0
        self.heartbeat_task = None
        self.exit_consume = False
        self.ssl_context = None
        self.lock = threading.Lock()

    def cleanup(self):
        '''
        Clean up processes that are run by 
        an external thread has been stopped.
        '''

        self.exit_consume = True

        print("pnl_pos_stream consumed() stopped...")
    
    async def reestablish_state(self, ws):
        '''
        Reestablish the state by logging 
        in again after reconnect.
        '''
             
        await self.rithmic_login(ws, request_login_pb2.RequestLogin.SysInfraType.PNL_PLANT)
        
        await self.resubscribe_to_pnl(ws)

    async def resubscribe_to_pnl(self, ws):
        '''
        Resubscribe to PnL after reconnecting to Rithmic.
        '''

        await self.subscribe(ws)

    # This routine 
    async def reconnect(self):
        '''
        Reconnect to the specified URI and 
        returns the websocket connection object.
        '''

        backoff = 1
        max_backoff = 32  # Maximum backoff time in seconds
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
 
    async def account_pnl_position_update(self, msg_buf):
        '''
        Interpret msg_buf as a AccountPnLPositionUpdate, 451.

        :param msg_buf: bytes
            A binary message sent from Rithmic's PnL plant.
        '''
        
        msg = account_pnl_position_update_pb2.AccountPnLPositionUpdate()
        msg.ParseFromString(msg_buf[4:])

        with self.lock:
            # Update Open position/trade pnl
            self.open_position_pnl = float(msg.open_position_pnl)
            # print(f'INFO: Trade PnL: {self.open_position_pnl}')
            
            # Update close position pnl
            self.close_position_pnl = float(msg.closed_position_pnl)

            # Update daily pnl
            self.daily_pnl = self.open_position_pnl + self.close_position_pnl
            # print(f'INFO: Daily PnL: {self.daily_pnl}')

            # Update current position
            self.current_position = int(msg.net_quantity)
            # print(f'INFO: Current position: {self.current_position}')

    async def instrument_pnl_pos_update(self, msg_buf):
        '''
        Interpret msg_buf as a InstrumentPnLPositionUpdate, 450.

        :param msg_buf: bytes
            A binary message sent from Rithmic's PnL plant.
        '''
        
        msg = instrument_pnl_position_update_pb2.InstrumentPnLPositionUpdate()
        msg.ParseFromString(msg_buf[4:])
            
    async def connect_to_rithmic(self):
        '''
        Connects to the specified URI and returns 
        the websocket connection object.
        '''

        ws = await websockets.connect(self.uri, ssl=self.ssl_context, ping_interval=3)

        print(f"connected to {self.uri}")

        return (ws)

    async def send_heartbeat(self, ws):
        '''
        Send a heartbeat request.
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
    
    async def list_systems(self, ws):
        '''
        Request the list of available Rithmic systems.
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

        while self.exit_consume == False:
            
            msg_buf = bytearray()

            waiting_for_msg = True
                
            while waiting_for_msg:
                try:
                    # Uncomment to DEBUG
                    #print(f"Waiting for PnL msg ...")
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
                #print(f" consumed msg : {msg_type} ({base.template_id})")
            
            elif base.template_id == 19:
                msg_type = "heartbeat response"
                #print(f" consumed msg : {msg_type} ({base.template_id})")
            
            elif base.template_id == 401:
                msg_type = "pnl position updates response"
                #print(f" consumed msg : {msg_type} ({base.template_id})")
                msg = response_pnl_position_updates_pb2.ResponsePnLPositionUpdates()
                msg.ParseFromString(msg_buf[4:])
                
            elif base.template_id == 450:
                msg_type = "instrument pnl position update"
                #print(f" consumed msg : {msg_type} ({base.template_id})")
                await self.instrument_pnl_pos_update(msg_buf)

            elif base.template_id == 451:
                msg_type = "account pnl position update"
                #print(f" consumed msg : {msg_type} ({base.template_id})")
                await self.account_pnl_position_update(msg_buf)
    
            else:
                msg_type = "unrecognized template id"
                #print(f" consumed msg : {msg_type} ({base.template_id})")

    async def rithmic_login(self, ws, infra_type):
        '''
        Log into the specified Rithmic system
        using the specified credentials.
        '''

        rq = request_login_pb2.RequestLogin()

        rq.template_id      = 10;
        rq.template_version = "3.9"
        rq.user_msg.append("hello")

        rq.user        = self.user_id
        rq.password    = self.password
        rq.app_name    = "CHANGE_ME:pnl_pos_stream.py"
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
  
    async def subscribe(self, ws):
        '''
        Subscribe to position and pnl data 
        for the specified instrument.
        '''
        
        rq = request_pnl_position_updates_pb2.RequestPnLPositionUpdates()

        rq.template_id      = 400;
        rq.user_msg.append("hello")
        rq.request    = request_pnl_position_updates_pb2.RequestPnLPositionUpdates.Request.SUBSCRIBE

        rq.fcm_id     = self.fcm_id
        rq.ib_id      = self.ib_id
        rq.account_id = self.account_id

        serialized = rq.SerializeToString()
        length     = len(serialized)

        buf  = bytearray()
        buf  = length.to_bytes(4, byteorder='big', signed=True)
        buf += serialized

        await ws.send(buf)

    async def heartbeat_loop(self, ws):
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
        Unsubscribe from Pnl and position updates.
        '''

        rq = request_pnl_position_updates_pb2.RequestPnLPositionUpdates()

        rq.template_id      = 400;
        rq.user_msg.append("hello")
        rq.request    = request_pnl_position_updates_pb2.RequestPnLPositionUpdates.Request.UNSUBSCRIBE

        rq.fcm_id     = self.fcm_id
        rq.ib_id      = self.ib_id
        rq.account_id = self.account_id

        serialized = rq.SerializeToString()
        length     = len(serialized)

        buf  = bytearray()
        buf  = length.to_bytes(4, byteorder='big', signed=True)
        buf += serialized

        await ws.send(buf)

    async def rithmic_logout(self, ws):
        '''
        Send a logout request.
        '''

        rq = request_logout_pb2.RequestLogout()

        rq.template_id      = 12;
        rq.user_msg.append("hello")

        serialized = rq.SerializeToString()
        length     = len(serialized)

        buf = bytearray()
        buf = length.to_bytes(4, byteorder='big', signed=True)
        buf += serialized

        await ws.send(buf)

    async def disconnect_from_rithmic(self, ws):
        '''
        Close the websocket connection.
        '''
        
        await ws.close(1000, "see you tomorrow")

    def run(self):
        '''
        Start task to start PnL and position stream.
        '''

        loop = asyncio.get_event_loop()

        if "wss://" in self.uri:
            self.ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            localhost_pem = pathlib.Path(__file__).with_name("rithmic_ssl_cert_auth_params")
            self.ssl_context.load_verify_locations(localhost_pem)

        ws = loop.run_until_complete(self.connect_to_rithmic())

        loop.run_until_complete(self.rithmic_login(ws, request_login_pb2.RequestLogin.SysInfraType.PNL_PLANT))

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
            # If the loop is interrupted or finishes, ensure the heartbeat taks is cancelled.
            self.heartbeat_task.cancel()
            try:
                # Await the task to handle its cancellation
                loop.run_until_complete(self.heartbeat_task)
            except asyncio.CancelledError:
                # Expect the CancelledError and ignore it, as it's a normal part of cancelling a task
                pass

            if ws.open:
                print(f"Unsubscribing from PnL and position data...")
                loop.run_until_complete(self.unsubscribe(ws))
                print(f"Logging out from PNL Plant...")
                loop.run_until_complete(self.rithmic_logout(ws))
                print(f"Disconnecting from PNL Plant...")
                loop.run_until_complete(self.disconnect_from_rithmic(ws))
                print(f"Done!")
            else:
                print(f"Connection appears to be closed. Exiting app.")
