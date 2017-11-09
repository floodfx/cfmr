import logging
import cfmr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def reduce(key, values, emitter):
    total = 0
    for value in values:
        total += value
    emitter.emit(key, total)
    return total

def handle(event, ctx):
    cfmr.reducer(event, ctx, reduce)
