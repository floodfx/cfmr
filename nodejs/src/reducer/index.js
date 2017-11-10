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
      console.log("allData", allData)
      var allValues = allData.reduce((values, data) => {
        console.log("data", data, "values", values)
        data.values.forEach((v) => {
          console.log("v", v)
          if(v.valueIsBase64) {
            values.push(new Buffer(v.value, 'base64'))
          } else {
            values.push(v.value)
          }
        })
        return values
      }, [])
      console.log("allValues", allValues)
      var key = allData[0].key
      if(allData[0].keyIsBase64) {
        key = new Buffer(key, 'base64')
      }
      console.log("reducing with:", "key", key, "values", allValues)
      var reduceResult = reducerFunction(key, allValues, emitter)
      console.log("reduceResult", reduceResult)
      // flush emits
      emitter.flushEmits().then((flushes) => {
        console.log("completed", flushes)
        cb(null, {reduceResult: reduceResult})
      })
    })
    .catch((err) => {
      console.log(err)
      cb(err)
    })
  }

  return wrapper
}
