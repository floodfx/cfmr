var mapper = require('cfmr').mapper

console.log('starting mapper function')

function map(key, value, emitter) {
  console.log("mapping:", "key", key, "value", value)
  return value.toString().split(/\s+/).map((word) => {
    emitter.emit(word, 1)
    return [word, 1]
  })
}

exports.handle = mapper.connect(map)
