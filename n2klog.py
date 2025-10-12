import time
import asyncio
import logging

tasks={}
def setTaskName(task,name):
    tasks[task]=name
    asyncio.create_task(removeTaskName(task))

async def removeTaskName(task):
    await task
    n=tasks.pop(task,None)
    if n:
        print(f"task {n} finished")
    else:
        print("no task name!")

def getTaskName(task):
    return tasks.get(task) or "?"

def GetCurTaskName():
    try:
        return getTaskName(asyncio.current_task())
    except:
        return "None"

logging.VERBOSE=logging.DEBUG-1
logging.addLevelName(logging.VERBOSE,"Verbose")
logging_orig_set=logging.LogRecord.set
msecs_start=time.ticks_ms()
def Logging_Record_Set(self,name,level,msg):
    logging_orig_set(self,name,level,msg)
    self.msecs=time.ticks_diff(time.ticks_ms(),msecs_start)
    self.task=GetCurTaskName()
logging.LogRecord.set=Logging_Record_Set
logging.basicConfig()

class Formatter:
    def __init__(self,fmt):
        self.fmt=fmt
    def format(self,record):
        return self.fmt % record.__dict__

logging.getLogger().handlers[0].setFormatter(Formatter("%(msecs)s:%(levelname)s:%(task)s:%(name)s: %(message)s"))

def setLevel(level):
    logger=logging.getLogger()
    logger.setLevel(level)
    logger.handlers[0].setLevel(level)

logging.setLevel=setLevel

def test():
    log=logging.getLogger("test")
    log.warning("eik %(a)s", {"a":"hello"})
if __name__ == '__main__':
    test()

