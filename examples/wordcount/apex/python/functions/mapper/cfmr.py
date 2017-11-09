import os
import boto3
import logging
import json
import codecs
import hashlib
import binascii
import inspect

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.resource('s3')

class Emitter(object):

    def __init__(self, outputBucket, outputPrefix, jobId, emitterType):
        self.emitBuffer = []
        self.outputBucket = outputBucket
        self.outputPrefix = outputPrefix
        self.jobId = jobId
        self.emitterType = emitterType

    def emit(self, key, value):
        self.emitBuffer.append([key, value])

    def flushEmits(self):
        for x in self.emitBuffer:
            self.flushEmit(x[0], x[1])

    def flushEmit(self, key, value):
        logger.info(f"flushing {key}, {value}")
        keyIsBuffer = type(key).__name__ is 'bytes'
        outputKey = codecs.encode(key, 'base64') if keyIsBuffer else key

        valueIsBuffer = type(value).__name__ is 'bytes'
        outputValue = codecs.encode(value, 'base64') if valueIsBuffer else value

        # for S3 keys
        h = hashlib.sha256()
        if(keyIsBuffer):
            h.update(key)
        else:
            h.update(key.encode('utf-8'))
        partitionKey = h.hexdigest()
        partKey = str(binascii.hexlify(os.urandom(16)), 'ascii')

        flushKey = f"{self.outputPrefix}/{self.jobId}/{self.emitterType}/{partitionKey}/{partKey}"

        logger.info(f"Flushing key: {key}, value: {value}, to s3://{self.outputBucket}/{flushKey}")

        body = {
          'key': outputKey,
          'keyIsBase64': keyIsBuffer,
          'value': outputValue,
          'valueIsBase64': valueIsBuffer,
        }
        obj = s3.Object(self.outputBucket, flushKey)
        obj.put(Body=json.dumps(body).encode('utf-8'))


def mapper(event, ctx, mapperFunction):
    logger.info('got event{}'.format(event))
    logger.info('got client context{}'.format(ctx.client_context))

    logger.info(inspect.getmembers(ctx.client_context))

    #emitter = Emitter()
    emitter = Emitter(
        ctx.client_context.custom['outputBucket'],
        ctx.client_context.custom['outputPrefix'],
        ctx.client_context.custom['jobId'],
        "mapper"
    )

    mapKey = event['key']
    inputBucket = ctx.client_context.custom['inputBucket']

    obj = s3.Object(inputBucket, mapKey)
    dataObj = obj.get()['Body'].read()
    logger.info('dataObjType{}'.format(dataObj))
    mapperResult = mapperFunction(mapKey, dataObj, emitter)
    logger.info('mapperResult{}'.format(mapperResult))
    emitter.flushEmits()

    return {
        'mapperResult': mapperResult
    }
