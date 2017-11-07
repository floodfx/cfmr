console.log('starting reduce function')
var crypto = require('crypto');
var AWS = require('aws-sdk');
var s3 = new AWS.S3()

const emit = (key, value, ctx) => {
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

exports.handle = function(e, ctx, cb) {
  console.log('reduce event: %j', e, ctx)
  var partitionKey = e.key
  var total = 0;
  console.log("here")
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
  Promise.all(dataGetters).then((allData) => {
    total = allData.reduce((sum, data) => {
      return sum + data.value
    }, 0)
    emit(allData[0].key, total, ctx)
    .then(() => {
      cb(null, { status: 'done' })
    })
    .catch((err) => {
      cb(err)
    })
  })
}
