# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: response_time_bar_replay.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1eresponse_time_bar_replay.proto\x12\x03rti\"\xe1\x04\n\x15ResponseTimeBarReplay\x12\x15\n\x0btemplate_id\x18\xe3\xb6\t \x02(\x05\x12\x15\n\x0brequest_key\x18\x96\x8d\x08 \x01(\t\x12\x12\n\x08user_msg\x18\x98\x8d\x08 \x03(\t\x12\x1c\n\x12rq_handler_rp_code\x18\x9c\x8d\x08 \x03(\t\x12\x11\n\x07rp_code\x18\x9e\x8d\x08 \x03(\t\x12\x10\n\x06symbol\x18\x94\xdc\x06 \x01(\t\x12\x12\n\x08\x65xchange\x18\x95\xdc\x06 \x01(\t\x12\x32\n\x04type\x18\xa0\xa3\x07 \x01(\x0e\x32\".rti.ResponseTimeBarReplay.BarType\x12\x10\n\x06period\x18\xc8\xa2\x07 \x01(\t\x12\x10\n\x06marker\x18\xbc\xa2\x07 \x01(\x05\x12\x14\n\nnum_trades\x18\xc5\xa2\x07 \x01(\x04\x12\x10\n\x06volume\x18\xc6\xa2\x07 \x01(\x04\x12\x14\n\nbid_volume\x18\xcd\xa2\x07 \x01(\x04\x12\x14\n\nask_volume\x18\xce\xa2\x07 \x01(\x04\x12\x14\n\nopen_price\x18\xb3\x8d\x06 \x01(\x01\x12\x15\n\x0b\x63lose_price\x18\xb5\x8d\x06 \x01(\x01\x12\x14\n\nhigh_price\x18\xac\x8d\x06 \x01(\x01\x12\x13\n\tlow_price\x18\xad\x8d\x06 \x01(\x01\x12\x1a\n\x10settlement_price\x18\xe6\x8d\x06 \x01(\x01\x12\x1e\n\x14has_settlement_price\x18\x92\x8d\t \x01(\x08\x12%\n\x1bmust_clear_settlement_price\x18\xcb\xb7\t \x01(\x08\"H\n\x07\x42\x61rType\x12\x0e\n\nSECOND_BAR\x10\x01\x12\x0e\n\nMINUTE_BAR\x10\x02\x12\r\n\tDAILY_BAR\x10\x03\x12\x0e\n\nWEEKLY_BAR\x10\x04')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'response_time_bar_replay_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _RESPONSETIMEBARREPLAY._serialized_start=40
  _RESPONSETIMEBARREPLAY._serialized_end=649
  _RESPONSETIMEBARREPLAY_BARTYPE._serialized_start=577
  _RESPONSETIMEBARREPLAY_BARTYPE._serialized_end=649
# @@protoc_insertion_point(module_scope)
