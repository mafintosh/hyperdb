var protobuf = require('protocol-buffers')

module.exports = protobuf(`
  message Node {
    message Feed {
      required bytes key = 1;
      optional bool owner = 2;
    }

    optional string key = 1;
    optional bytes value = 2;
    repeated uint64 clock = 3;
    optional bytes trie = 4;
    repeated Feed feeds = 5;
    optional uint64 feedSeq = 6; // TODO remove and merge into index (trie+feedSeq)
  }
`)
