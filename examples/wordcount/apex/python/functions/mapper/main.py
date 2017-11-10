import logging
from cfmr import mapper

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def map(key, value, emitter):
    logger.debug(f"map: key:{key}, val:{value}")
    mapResult = []
    for word in value.decode('utf-8').split():
        emitter.emit(word, 1)
        mapResult.append([word, 1])
    logger.debug(f"mapResult:{mapResult}")
    return mapResult

def handle(event, ctx):
    logger.debug(f"event:{event}")
    mapper.handle(event, ctx, map)
