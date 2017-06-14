var protobuf = require('protocol-buffers')
var messages = protobuf(`
  message Feed {
    required bytes key = 1;
    optional bool owner = 2;
    optional bool writer = 3;
  }

  message Header {
    required string type = 1;
    required uint64 version = 2;
  }

  message Pointers {
    repeated Pointer _pointers = 1;
  }

  message Pointer {
    required uint64 feed = 0;
    required uint64 seq = 1;
    optional uint64 target = 2;
  }

  message Node {
    optional uint64 pointer = 1;
    repeated Feed feeds = 2;
    repeated uint64 heads = 3;
    repeated Pointers pointers = 4;
    optional string key = 5;
    optional bytes value = 6;
  }

  // IGNORE THIS MESSAGE, A PERF HACK TO ADD SOME EXTRA PROPS
  message NodeWrap {
    optional uint64 pointer = 1;
    repeated Feed feeds = 2;
    repeated uint64 heads = 3;
    repeated Pointers pointers = 4;
    optional string key = 5;
    optional bytes value = 6;

    optional uint64 seq = 100;
    optional uint64 feed = 101;
    optional bytes hash = 102;
  }
`)

module.exports = messages
