var s3helper = require('../common/s3helper');
var Emitter = require('../common/emitter');

exports.connect = function(reducerFunction) {

  const wrapper = (e, ctx, cb) => {
    console.log('reduce event: %j', e, ctx)

    var emitter = new Emitter(
      ctx.clientContext.custom.outputBucket,
      ctx.clientContext.custom.outputPrefix,
      ctx.clientContext.custom.jobId,
      Emitter.TYPE_REDUCER
    )

    // get items for this partition
    var reducerKey = e.key
    var dataGetters = e.value.map((path) => {
      console.log("getting", path)
      return s3helper.getItem(ctx.clientContext.custom.outputBucket, path)
      .then((data) => {
        return JSON.parse(data.Body.toString())
      })
      .catch((err) => {
        console.log("Error")
        cb(err)
      })
    })

    Promise.all(dataGetters)
    .then((allData) => {
      // decode data
      var values = allData.map((data) => {
        if(data.valueIsBase64) {
          return new Buffer(data.value, 'base64')
        } else {
          return data.value
        }
      })
      var key = allData[0].key
      if(allData[0].keyIsBase64) {
        key = new Buffer(key, 'base64')
      }
      console.log("reducing with:", "key", key, "values", values)
      var reduceResult = reducerFunction(key, values, emitter)
      console.log("reduceResult", reduceResult)
      // flush emits
      emitter.flushEmits().then((flushes) => {
        console.log("completed", flushes)
        cb(null, {reduceResult: reduceResult})
      })
    })
    .catch((err) => {
      console.err(err)
      cb(err)
    })
  }

  return wrapper
}
