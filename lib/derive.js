var sodium = require('sodium-universal')

var CONTEXT = Buffer.from('hyperdb1') // hyperdb v1

module.exports = deriveKeyPair

function deriveKeyPair (secretKey) {
  var seed = Buffer.alloc(sodium.crypto_sign_SEEDBYTES)
  var keyPair = {
    publicKey: Buffer.alloc(sodium.crypto_sign_PUBLICKEYBYTES),
    secretKey: Buffer.alloc(sodium.crypto_sign_SECRETKEYBYTES)
  }

  sodium.crypto_kdf_derive_from_key(seed, 1, CONTEXT, secretKey)
  sodium.crypto_sign_seed_keypair(keyPair.publicKey, keyPair.secretKey, seed)
  seed.fill(0)

  return keyPair
}
