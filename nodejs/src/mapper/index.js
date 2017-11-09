var s3helper = require('../common/s3helper');
var Emitter = require('../common/emitter');

exports.connect = function(mapperFunction) {

  const wrapper = (e, ctx, cb) => {
    console.log('map event: %j', e, ctx)

    var mapKey = e.key;

    var emitter = new Emitter(
      ctx.clientContext.custom.outputBucket,
      ctx.clientContext.custom.outputPrefix,
      ctx.clientContext.custom.jobId,
      Emitter.TYPE_MAPPER
    )

    // get data
    s3helper.getItem(ctx.clientContext.custom.inputBucket, mapKey)
    .then((data) => {
      console.log("key", mapKey, "data", data)
      // call map
      var mapResult = mapperFunction(mapKey, data.Body, emitter)
      console.log("mapResult", mapResult)
      // flush emits
      emitter.flushEmits().then((flushes) => {
        console.log("completed", flushes)
        cb(null, {mapResult: mapResult})
      })
    })
    .catch((err) => {
      console.error(err)
      cb(err)
    })
  }

  return wrapper
}
