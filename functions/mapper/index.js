var crypto = require('crypto');
var AWS = require('aws-sdk');
var s3 = new AWS.S3()

console.log('starting mapper function')

var CLIENT_CONTEXT = {}
const setCFMRContext = (clientContext) => {
  CLIENT_CONTEXT = clientContext
}

var EMIT_BUFFER = []
const flushEmits = () => {
  var emits = [...EMIT_BUFFER]
  EMIT_BUFFER = []
  return Promise.all(emits.map((emit) => {
    return flushEmit(...emit)
  }))
}

const flushEmit = (key, value) => {
  // TODO do not allow keys or values to be undefined?
  console.log("key", key, "value", value)

  var keyIsBuffer = Buffer.isBuffer(key)
  var outputKey = keyIsBuffer ? key.toString('base64') : key

  var valueIsBuffer = Buffer.isBuffer(value)
  var outputValue = valueIsBuffer ? value.toString('base64') : value

  // for S3 keys
  var partionKey = crypto.createHash('sha256').update(key).digest("hex")
  var partKey = crypto.randomBytes(16).toString("hex")

  var body = {
    key: outputKey,
    keyIsBase64: keyIsBuffer,
    value: outputValue,
    valueIsBase64: valueIsBuffer,
  }
  var params = {
    Body: JSON.stringify(body),
    Bucket: CLIENT_CONTEXT.outputBucket,
    Key: `${CLIENT_CONTEXT.outputPrefix}/${CLIENT_CONTEXT.jobId}/mapper/${partionKey}/${partKey}`
  };
  return new Promise((resolve, reject) => {
    s3.putObject(params, function(err, data) {
       if (err) {
         console.log("error emitting", err, err.stack); // an error occurred
         reject(err)
       }
       else {
         console.log("success emitting", data);           // successful response
         resolve(data)
       }
    });
  })
}

const emit = (key, value) => {
  EMIT_BUFFER.push([key, value])
}

const getData = (bucket, key) => {
  var params = {
    Bucket: bucket,
    Key: key
   };
   return new Promise((resolve, reject) => {
     s3.getObject(params, function(err, data) {
       if (err) {
         console.log(err, err.stack); // an error occurred
         reject(err)
       }
       else {
         console.log(data);           // successful response
         resolve(data)
       }
     });
   })
}

function map(key, buffer) {
  return buffer.toString().split(/\s+/).map((word) => {
    emit(word, 1)
    return [word, 1]
  })
}

exports.handle = function(e, ctx, cb) {
  console.log('map event: %j', e, ctx)

  setCFMRContext(ctx.clientContext)
  // get data
  getData(ctx.clientContext.inputBucket, e.key)
  .then((data) => {
    console.log("key", e.key, "data", data)
    // call map
    var mapResult = map(e.key, data.Body)
    console.log("mapResult", mapResult)
    // flush emits
    flushEmits().then((flushes) => {
      console.log("completed", flushes)
      cb(null, {mapResult: mapResult})
    })
  })
  .catch((err) => {
    console.error(err)
    cb(err)
  })

}
