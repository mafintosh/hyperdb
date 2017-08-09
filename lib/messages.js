var protobuf = require('protocol-buffers')

module.exports = protobuf(`
  message Node {
    required string key = 1;
    optional bytes value = 2;
    repeated uint64 heads = 3;
    optional bytes trie = 4;
  }
`)
