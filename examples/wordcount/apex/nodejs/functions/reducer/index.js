var reducer = require('cfmr').reducer;

console.log('starting reduce function')

function reduce(key, values, emitter) {
  console.log("reducing:", "key", key, "values", values)
  var total = values.reduce((sum, data) => {
    return sum + data
  }, 0)
  emitter.emit(key, total)
  return total
}

exports.handle = reducer.connect(reduce)
