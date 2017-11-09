import logging
import cfmr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def map(key, value, emitter):
    mapResult = []
    for word in value.decode('utf-8').split():
        emitter.emit(word, 1)
        mapResult.append([word, 1])
    return mapResult

def handle(event, ctx):
    cfmr.mapper(event, ctx, map)
