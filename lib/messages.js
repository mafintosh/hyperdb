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

  message Node {
    optional uint64 pointer = 1;
    repeated Feed feeds = 2;
    repeated uint64 heads = 3;
    optional string key = 4;
    optional bytes value = 5;
  }

  // IGNORE THIS MESSAGE, A PERF HACK TO ADD SOME EXTRA PROPS
  message NodeWrap {
    optional uint64 pointer = 1;
    repeated Feed feeds = 2;
    repeated uint64 heads = 3;
    optional string key = 4;
    optional bytes value = 5;

    optional uint64 seq = 100;
  }
`)

module.exports = messages
