# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: apache/rocketmq/v2/admin.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1e\x61pache/rocketmq/v2/admin.proto\x12\x12\x61pache.rocketmq.v2\"\x95\x01\n\x15\x43hangeLogLevelRequest\x12>\n\x05level\x18\x01 \x01(\x0e\x32/.apache.rocketmq.v2.ChangeLogLevelRequest.Level\"<\n\x05Level\x12\t\n\x05TRACE\x10\x00\x12\t\n\x05\x44\x45\x42UG\x10\x01\x12\x08\n\x04INFO\x10\x02\x12\x08\n\x04WARN\x10\x03\x12\t\n\x05\x45RROR\x10\x04\"(\n\x16\x43hangeLogLevelResponse\x12\x0e\n\x06remark\x18\x01 \x01(\t2r\n\x05\x41\x64min\x12i\n\x0e\x43hangeLogLevel\x12).apache.rocketmq.v2.ChangeLogLevelRequest\x1a*.apache.rocketmq.v2.ChangeLogLevelResponse\"\x00\x42=\n\x12\x61pache.rocketmq.v2B\x07MQAdminP\x01\xa0\x01\x01\xd8\x01\x01\xf8\x01\x01\xaa\x02\x12\x41pache.Rocketmq.V2b\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'apache.rocketmq.v2.admin_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\n\022apache.rocketmq.v2B\007MQAdminP\001\240\001\001\330\001\001\370\001\001\252\002\022Apache.Rocketmq.V2'
  _CHANGELOGLEVELREQUEST._serialized_start=55
  _CHANGELOGLEVELREQUEST._serialized_end=204
  _CHANGELOGLEVELREQUEST_LEVEL._serialized_start=144
  _CHANGELOGLEVELREQUEST_LEVEL._serialized_end=204
  _CHANGELOGLEVELRESPONSE._serialized_start=206
  _CHANGELOGLEVELRESPONSE._serialized_end=246
  _ADMIN._serialized_start=248
  _ADMIN._serialized_end=362
# @@protoc_insertion_point(module_scope)