# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: grpc_service.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x12grpc_service.proto\"5\n\x07Request\x12\x12\n\nauth_token\x18\x01 \x01(\t\x12\x16\n\x0e\x64\x61ta_source_id\x18\x02 \x01(\x05\"(\n\x08Response\x12\x0e\n\x06status\x18\x01 \x01(\x05\x12\x0c\n\x04\x62ody\x18\x02 \x01(\t25\n\x0bGrpcService\x12&\n\rFetchDataSets\x12\x08.Request\x1a\t.Response\"\x00\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'grpc_service_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _REQUEST._serialized_start=22
  _REQUEST._serialized_end=75
  _RESPONSE._serialized_start=77
  _RESPONSE._serialized_end=117
  _GRPCSERVICE._serialized_start=119
  _GRPCSERVICE._serialized_end=172
# @@protoc_insertion_point(module_scope)