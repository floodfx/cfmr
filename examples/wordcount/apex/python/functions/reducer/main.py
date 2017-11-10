import logging
from cfmr import reducer

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def reduce(key, values, emitter):
    logger.debug(f"map: key:{key}, vals:{values}")
    total = 0
    for value in values:
        total += value
    emitter.emit(key, total)
    logger.debug(f"reduceResult:{total}")
    return total

def handle(event, ctx):
    logger.debug(f"event:{event}")
    logger.debug(f"ctx:{ctx}")
    reducer.handle(event, ctx, reduce)
