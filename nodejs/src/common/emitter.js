var crypto = require('crypto');
var AWS = require('aws-sdk');
var s3 = new AWS.S3()

class Emitter {
  constructor(outputBucket, outputPrefix, jobId, emitterType) {
    this.outputBucket = outputBucket;
    this.outputPrefix = outputPrefix;
    this.jobId = jobId;
    this.emitterType = emitterType;
    this.emitBuffer = [];
  }

  emit(key, value) {
    this.emitBuffer.push([key, value])
  }

  flushEmits() {
    var emits = [...this.emitBuffer]
    this.emitBuffer = []
    return Promise.all(emits.map((emit) => {
      return this.flushEmit(...emit)
    }))
  }

  flushEmit(key, value) {
    // TODO do not allow keys or values to be undefined?

    var keyIsBuffer = Buffer.isBuffer(key)
    var outputKey = keyIsBuffer ? key.toString('base64') : key

    var valueIsBuffer = Buffer.isBuffer(value)
    var outputValue = valueIsBuffer ? value.toString('base64') : value

    // for S3 keys
    var partionKey = crypto.createHash('sha256').update(key).digest("hex")
    var partKey = crypto.randomBytes(16).toString("hex")

    var flushKey = `${this.outputPrefix}/${this.jobId}/${this.emitterType}/${partionKey}/${partKey}`

    console.log("Flushing key:", key, "value:", value, ` to s3://${this.outputBucket}/${flushKey}`)

    var body = {
      key: outputKey,
      keyIsBase64: keyIsBuffer,
      value: outputValue,
      valueIsBase64: valueIsBuffer,
    }
    var params = {
      Body: JSON.stringify(body),
      Bucket: this.outputBucket,
      Key: flushKey
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

}

Emitter.TYPE_MAPPER = 'mapper';
Emitter.TYPE_REDUCER = 'reducer';

module.exports = Emitter
