console.log('starting reduce function')
var crypto = require('crypto');
var AWS = require('aws-sdk');
var s3 = new AWS.S3()

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
    Key: `${CLIENT_CONTEXT.outputPrefix}/${CLIENT_CONTEXT.jobId}/reducer/${partionKey}/${partKey}`
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

const old_emit = (key, value, ctx) => {
  // write to s3://outputPrefix/{key}/{uuid}
  console.log("emitting", key, value)
  var hashedKey = crypto.createHash('sha256').update(key).digest("hex")
  var objectKey = crypto.randomBytes(16).toString("hex")
  var params = {
    Body: JSON.stringify({key, value}),
    Bucket: ctx.clientContext.outputBucket,
    Key: `${ctx.clientContext.outputPrefix}/${ctx.clientContext.jobId}/reducer/${hashedKey}/${objectKey}`
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


function reduce(key, values) {
  console.log("reduce", "key", key, "values", values)
  var total = values.reduce((sum, data) => {
    return sum + data
  }, 0)
  emit(key, total)
  return total
}

exports.handle = function(e, ctx, cb) {
  console.log('reduce event: %j', e, ctx)

  setCFMRContext(ctx.clientContext)

  var partitionKey = e.key
  var dataGetters = e.value.map((path) => {
    console.log("getting", path)
    return getData(ctx.clientContext.outputBucket, path)
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
    var values = allData.map((data) => {
      if(data.isValueBase64) {
        return new Buffer(data.value, 'base64')
      } else {
        return data.value
      }
    })
    var key = allData[0].key
    if(allData[0].isKeyBase64) {
      key = new Buffer(key, 'base64')
    }
    console.log("reducing with:", "key", key, "values", values)
    var reduceResult = reduce(key, values)
    console.log("reduceResult", reduceResult)
    // flush emits
    flushEmits().then((flushes) => {
      console.log("completed", flushes)
      cb(null, {reduceResult: reduceResult})
    })
  })
  .catch((err) => {
    console.err(err)
    cb(err)
  })
}
