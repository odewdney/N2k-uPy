import asyncio
import logging
from n2klog import setTaskName

#logging.setLevel(logging.VERBOSE)

#import esp32can as can
#import functools
#from asyncio import Queue
from primitives.queue import Queue
import aiorepl
from ucontextlib import contextmanager
import esp32

#import mip
#mip.install("logging")
#mip.install("aiorepl")
#mip.install("github:peterhinch/micropython-async/v3/primitives")
#mip.install("ucontextlib")

from n2kmsgs import *

LOG_VERBOSE=logging.VERBOSE
LOG_DEBUG=logging.DEBUG
LOG_NORMAL=logging.INFO
LOG_WARN=logging.WARNING
LOG_ERROR=logging.ERROR

logger=logging.getLogger("n2k")
log=logger.log

class NmeaAckException(Exception):
    def __init__(msg,pgn,pgnErr,priErr,params=None):
        super().__init__(msg)
        self.pgn=pgn
        self.pgnErr=pgnErr
        self.priErr=priErr
        self.params=params

class MsgHandler:
    def __init__(self,dev,msg=None,pgn=None,offset=None,interval=None):
        self.dev=dev
        self.log=dev.log
        self.msg=msg
        if msg:
            self.pgn=msg.pgn
            p=self.getSetting("p")
            if p: msg.priority=p
        else:
            self.pgn=pgn
            self.priority=self.getSetting("p")
        self.task=None
        self.ev=asyncio.Event()
        self.defInt=interval
        self.interval=self.getSetting("i",interval)
        self.offset=self.getSetting("o",offset)
    def settingKey(self,key):
        return f"{self.pgn:x}.{key}"
    def getSetting(self,key,default=None):
        return self.dev.getSetting(self.settingKey(key),default)
    def setSetting(self,key,value):
        return self.dev.getSetting(self.settingKey(key),value)
    def start(self):
        self.task=asyncio.create_task(self.runner())
        setTaskName(self.task,f"dev-{self.dev.inst}:{self.pgn}")
    def stop(self):
        self.task.cancel()
        self.task=None
    async def runner(self):
        cnt=0
        while True:
            self.ev.clear()
            if self.interval:
                await self.sendRepeatMsg(cnt)
                cnt+=1
                try:
                    await asyncio.wait_for(self.ev.wait(),self.interval if cnt>0 else self.offset)
                    cnt=0
                except asyncio.core.TimeoutError:
                    continue
            else:
                await self.ev.wait()
    async def sendRepeatMsg(self,cnt):
        self.log.debug(f"sending {self.pgn:x} {cnt}")
    def createAck(self,pgnErr=0,priErr=0,params=None):
        msg=n2kNMEAAckGroupMsg()
        msg.PGN=self.pgn
        msg.ErrorCode=pgnErr
        msg.ErrorCode2=priErr
        return msg
    async def GetISOMessage(self):
        if not self.msg: raise NmeaAckException("no msg of iso",self.pgn,0,0)
        return self.msg.clone()
    def NmeaRequest(self,interval,offset):
        if not isinstance(offset,n2kNAValue):
            if offset==0: offset=None
            self.offset=offset
            self.setSetting("o",offset)
        if isinstance(interval,n2kNAValue) and interval.GetValue()==0xfffffffe:
            interval=self.defInt
        if not isinstance(interval,n2kNAValue):
            if interval==0: interval=None
            self.interval=interval
            self.setSetting("i",interval)
        elif isinstance(offset,n2kNAValue):
            return self.msg.clone()
        self.ev.set()
        return self.createAck()
    def NmeaCommand(self,priority):
        if pri>=10: #reseved
            return self.createAck(0,4)
        if pri!=8: # no change
            if pri==9: #default
                pri=self.msg.__class__.priority if self.msg else None
            if self.msg:
                self.msg.priority=priority
            else:
                self.priority=priority
            self.setSetting("p",priority if pri!=9 else None)
        return self.createAck()
    def NmeaRead(self,data):
        pass
    def NmeaWrite(self,data):
        pass

    
class Heartbeat(MsgHandler):
    def __init__(self,dev,msg):
        super().__init__(dev,msg,offset=1,interval=10)
        msg.Sequence=0
    def update(self):
        msg=self.msg
        msg.Offset=self.interval
        msg.Sequence = msg.Sequence+1 if msg.Sequence < 252 else 0
        msg.EquipmentState=EquipStatusEnum.Operational
    async def sendRepeatMsg(self,cnt):
        self.log.info(f"sending {self.pgn:x} {cnt}")
        self.update()
        await self.dev.sendMessage(self.msg)
    def GetISOMessage(self):
        raise NmeaAckException("GetIso not allowed",self.pgn,0,0)
    def NmeaCommand(self,priority):
        raise NmeaAckException("NmeaCmd not allowed",self.pgn,4,4)
    def NmeaRequest(self,interval,offset):
        if interval<1 or interval>60:
            raise NmeaAckException("NmeaReq bad int",self.pgn,0,1)
        if isinstance(interval,n2kmsgs.n2kNAValue) and isinstance(offset,n2kmsgs.n2kNAValue):
            raise NmeaAckException("Nmea cant req hb",self.pgn,3,1)
        return super().NmeaRequest(interval,offset)

class PGNListMsgHandler(MsgHandler):
    def __init__(self,dev,tx):
        super().__init__(dev,pgn=n2kNMEAPGNList.pgn)
        self.tx=tx
        #if hasattr(msg,"ISO"):
        #    m.ISO=True
        #await self.sendMessage(m)
        #await domsgs(n2kNMEAPGNList.FunctionCodes.TX,self.txmsgs)
        #await domsgs(n2kNMEAPGNList.FunctionCodes.RX,self.rxmsgs)
    def GetISOMessage(self):
        msgs=self.dev.txmsgs if self.tx else self.dev.rxmsgs
        m=n2kNMEAPGNListTx() if self.tx else n2kNMEAPGNListRx()
        if self.priority: m.priority=self.priority
        distinct=[0]
        for mm in msgs.keys():
            pgn=mm.pgn
            if not pgn in distinct:
                n=m.PGNs.add()
                m.PGNs[n]=pgn
                distinct.append(pgn)
        return m

class n2kDevice:
    def __init__(self,bus,inst):
        self.bus=bus
        self.inst=inst
        self.log=logging.getLogger(f"n2kdev={inst}")
        self.nvs=esp32.NVS(f"dev-{inst}")
        
        self.addr = N2K_ADDR_NULL
        self.task=None
        self.name = n2kISOAddressClaimMsg()
        self.name.UniqueNumber = 0x1234
        self.name.ManufacturerCode = 2046
        self.name.DeviceFunction = 130 # pc gateway
        self.name.DeviceClass = 25 # internetwork
        # DeviceInstance
        self.name.DeviceInstance=inst
        # Systeminstance
        self.addr_taken = asyncio.Event()
        
        self.pi = n2kProductInformationMsg()
        self.pi.ModelID = "test"
        
        self.conf = n2kConfigurationInformationMsg()
        self.conf.InstallationDescription1 = self.getSetting("desc1","desc1")
        self.conf.InstallationDescription2 = "desc2"
        self.conf.ManufacturerDescription = "mdesc"
        
        self.heartbeat = n2kHeartbeatMsg()
        
        self.txmsgs = {n2kISOAcknowledgementMsg:None,
                     n2kISORequestMsg:None,
                     n2kISOTransportProtocolConnectionManagementBase:None,
                     n2kISOTransportProtocolDataTransferMsg:None,
                     # n2k group, txrxlist
                     type(self.name):self.name,
                     type(self.pi):MsgHandler(self,self.pi),
                     type(self.conf):MsgHandler(self,self.conf),
                     type(self.heartbeat):Heartbeat(self,self.heartbeat),
#                     n2kNMEAPGNList:PGNListMsgHandler(self)
                     n2kNMEAPGNListTx:PGNListMsgHandler(self,tx=True),
                     n2kNMEAPGNListRx:PGNListMsgHandler(self,tx=False)
                     }
        
        self.rxmsgs = {n2kNmeaFastBase:self.processFastMsg,
                       n2kISOAcknowledgementMsg:True,
                       n2kISORequestMsg:self.processISORequest,
                       n2kISOTransportProtocolConnectionManagementBase:self.processISOTPMsg,
                       n2kISOTransportProtocolDataTransferMsg:None,
                       n2kISOAddressClaimMsg:self.processISOAddressClaim,
                       n2kISOCommandedAddressMsg:self.processISOCommandedAddress,
                       n2kNMEAGroupFunctionBase:self.processNMEAGroupFunctionMsg}
        self.msgProcessors = {}
        self.msgSenders = {}
    def getSetting(self,key,default=None):
        try:
            return self.nvs.get_i32(key)
        except:
            return default
    def setSetting(self,key,value):
        if value is None:
            self.nvs.erase_key(key)
        else:
            self.nvs.set_i32(key,value)
            self.nvs.commit()
    settingBuffer=bytearray(50)
    def getSettingStr(self,key,default):
        try:
            l=self.nvs.get_blob(key,settingBuffer)
            return settingBuffer[:l].decode()
        except OSError as err:
            # not found or but too samll
            return default
    def setSettingStr(self,key,value):
        self.nvs.set_blob(key,value)

    async def sendMessage(self,msg):
        if self.addr==N2K_ADDR_NULL:
            log(LOG_ERROR,"Cant send with no addr")
            return
        msg.src=self.addr
        if (hasattr(msg,"ISO") and len(msg.data)>8) or msg.fast:
            # multi msg
            print("queuing")
            dst=msg.dst
            queue = self.msgSenders.get(dst)
            if not queue:
                print("creating q")
                queue=Queue()
                self.msgSenders[dst]=queue
                task=asyncio.create_task(self.sendMessageQueue(dst,queue))
                setTaskName(task,f"sendQ-{self.addr}-{msg.dst}")
            await queue.put(msg)
        else:
            await self.bus.sendMessage(self,msg)
    async def sendMessageQueue(self,dst,queue):
        print("qproc start")
        while not queue.empty():
            msg=await queue.get()
            await self.sendMessageImpl(msg)
        del self.msgSenders[dst]
        print("qproc end")
    async def sendMessageImpl(self,msg):
        if hasattr(msg,"ISO") and len(msg.data)>8:
            if msg.dst==N2K_ADDR_BROADCAST:
                await self.sendISOBAMMessage(msg)
            else:
                await self.sendISOMessage(msg)
        elif msg.fast:
            print("sending fast")
            await self.bus.sendMessage(self,msg)
        else:
            await self.bus.sendMessage(self,msg)

    async def sendISOBAMMessage(self,msg):
        l=len(msg.data)
        p=(l+6)//7
        bam=n2kISOTransportProtocolConnectionBAMMsg()
        bam.priority=msg.priority
        bam.dst=msg.dst
        bam.src=self.addr
        bam.PGN=msg.pgn
        bam.MessageSize=len(msg.data)
        bam.Packets=p
        await self.bus.sendMessage(self,msg)
        for n in range(p):
            await asyncio.sleep_ms(50)
            m=n2kISOTransportProtocolDataTransferMsg()
            m.priority=msg.priority
            m.dst=msg.dst
            m.src=self.addr
            m.Sequence=n+1
            m.Data=msg.data[n*7:(n+1)*7]
            await self.bus.sendMessage(self,m)

    async def sendISOMessage(self,msg):
        l=len(msg.data)
        p=(l+6)//7
        rts=n2kISOTransportProtocolConnectionRTSMsg()
        rts.priority=msg.priority
        rts.dst=msg.dst
        rts.src=self.addr
        rts.PGN=msg.pgn
        rts.MessageSize=l
        rts.Packets=p
        rts.PacketsReply=p
        msgQueue=Queue()
        task=asyncio.cur_task
        processor=n2kDevice.ISOTPProcessor(task,msgQueue,msg.pgn)
        regCtx=self.registerMessageProcessor(msg.dst,processor)
        with regCtx():
            await self.bus.sendMessage(self,rts)
            while True:
                try:
                    cts = await asyncio.wait_for_ms(msgQueue.get(),2000)
                except asyncio.core.TimeoutError:
                    self.log.warning("sendISO timeout")
                    abort=n2kISOTransportProtocolConnectionAbortMsg(dst=msg.dst,src=self.addr)
                    abort.PGN=msg.pgn
                    abort.Reason=3 #timeout
                    await self.bus.sendMessage(self,abort)
                    return
                if isinstance(cts,n2kISOTransportProtocolConnectionEOMMsg):
                    return
                if not isinstance(cts,n2kISOTransportProtocolConnectionCTSMsg):
                    self.log.warning("unexpected TP msg %s",cts)
                    return
                last=cts.NextSID+cts.MaxPackets
                if last>(p+1):
                    self.log.error("bad nSID n:%d m:%d p:%d",cts.NextSID,cts.MaxPackets,p)
                    last=p+1
                for s in range(cts.NextSID, last):
                    m=n2kISOTransportProtocolDataTransferMsg()
                    m.priority=msg.priority
                    m.dst=msg.dst
                    m.src=self.addr
                    m.Sequence=s
                    m.Data=msg.data[(s-1)*7:s*7]
                    await self.bus.sendMessage(self,m)

    def start(self):
        if self.task is not None:
            raise Exception("already started")
        self.task=asyncio.create_task(self.run_addr_claim())
        setTaskName(self.task,f"addr({self.inst})")
        for m in self.txmsgs.items():
            if isinstance(m[1],MsgHandler):
                m[1].start()
    def stop(self):
        if self.task is not None:
            self.task.cancel()
            self.task=None
        for m in self.txmsgs.items():
            if isinstance(m[1],MsgHandler):
                m[1].stop()
    async def run_addr_claim(self):
        addr = 0
        loop = False
        while True:
            # random initial wait
            await asyncio.sleep_ms(100)
            # send claim
            self.name.src = addr
            log(LOG_VERBOSE,"send claim inst:%s addr:%x",self.name.DeviceInstance,addr)
            await self.bus.sendMessage(self,self.name)
            self.addr_taken.clear()
            try:
                # wait for response
                await asyncio.wait_for_ms(self.addr_taken.wait(),250)
            except asyncio.core.TimeoutError:
                # claim successful
                log(LOG_VERBOSE,"claimed inst:%d addr:%x",self.name.DeviceInstance,addr) 
                self.addr = addr
                loop = False
                while True:
                    try:
                        await asyncio.wait_for(self.addr_taken.wait(),60)
                        break
                    except asyncio.core.TimeoutError:
                        log(LOG_VERBOSE,"send poll inst:%d addr:%x",self.name.DeviceInstance,addr)
                        await self.bus.sendMessage(self,self.name)
                        continue
                    await asyncio.sleep(1)
                        
            if self.name.src != N2K_ADDR_NULL:
                if self.name.src!=addr:
                    # new addr - make claim
                    addr = self.name.src
                    self.addr = N2K_ADDR_NULL
                continue
            
            self.addr = N2K_ADDR_NULL
            # someone else has claimed
            addr+=1
            if addr>252:
                if loop:
                    print("No address available")
                    self.name.src = N2K_ADDR_NULL
                    await self.bus.sendMessage(self,self.name)
                    return
                addr = 0
                loop = True
    
    async def processISOAddressClaim(self,msg):
        if msg.src==self.name.src:
            msgInt = msg.GetNameInt()
            selfInt = self.name.GetNameInt()
            if msgInt < selfInt:
                log(LOG_VERBOSE,"inst %d has addr %x",msg.DeviceInstance,msg.src)
                self.name.src = N2K_ADDR_NULL
                self.addr_taken.set()
            elif msgInt > selfInt:
                log(LOG_VERBOSE,"inst %d assert my addr",self.name.DeviceInstance)
                self.addr_taken.set()
#                await self.bus.sendMessage(self,self.name)
    
    async def processISOCommandedAddress(self,msg):
        if msg.GetNameInt() == self.name.GetNameInt():
            self.log.debug("addr: %d=>%d",self.name.src,msg.NewSourceAddress)
            self.name.src = msg.NewSourceAddress
            self.addr_taken.set()
        else:
            self.log.error("command addr not for us")
            
    async def processISORequest(self,msg):
        self.log.warning("Request msg %x from %x",msg.PGN,msg.src)
        dst=msg.src if msg.PGN!=n2kISOAddressClaimMsg.pgn else N2K_ADDR_BROADCAST
        processed=False
        try:
            for e in self.txmsgs.items():
                entryType = e[0]
                entryValue = e[1]
                if entryType.pgn!=msg.PGN: continue
                processed=True
                if callable(entryValue):
                    await entryValue(msg)
                elif isinstance(entryValue, MsgHandler):
                    m=entryValue.GetISOMessage()
                    m.dst=dst
                    m.ISO=True
                    await self.sendMessage(m)
                else:
                    for e in (entryValue if isinstance(entryValue,list) else [entryValue]):
                        m=entryValue.clone()
                        m.dst=dst
                        m.ISO=True
                        await self.sendMessage(m)
        except NmeaAckException as exc:
            self.log.error(exc)
            processed=False
        if not processed and msg.dst!=N2K_ADDR_BROADCAST:
            m=n2kISOAcknowledgementMsg(dst=msg.src)
            m.Control = 1# NACK
            m.GroupFunction = 0xff
            m.PGN = msg.PGN
            m.ISO=True
            await self.sendMessage(m)
            self.log.error("isoreg sent %s",m)    

    def ISOTPProcessor(self,msgQueue,pgn):
        f=False
        while True:
            msg2=yield f
            if isinstance(msg2, n2kISOTransportProtocolConnectionManagementBase) and msg2.PGN==pgn:
                self.log.debug("got TP")
                msgQueue.put_nowait(msg2)
                f=True
            elif isinstance(msg2, n2kISOTransportProtocolDataTransferMsg):
                self.log.debug("got data")
                msgQueue.put_nowait(msg2)
                f=True
            else:
                self.log.debug("no processed")
                f=False

    async def processRTS(self,msg):
        pgn=msg.PGN
        src=msg.src
        task=None
        msgQueue=Queue()
        async def run(regCtx):
            nonlocal msg,src,pgn,self
            pkts=msg.Packets
            msgs=pkts*[None]
            sid=1
            with regCtx():
                while sid<=pkts:
                    #send cts
                    npkts=min(2,msg.PacketsReply,1+pkts-sid)
                    cts=n2kISOTransportProtocolConnectionCTSMsg()
                    cts.dst=src
                    cts.NextSID=sid
                    cts.MaxPackets=npkts
                    cts.PGN=pgn
                    await self.sendMessage(cts)
                    # read messages
                    for n in range(npkts):
                        m = await asyncio.wait_for_ms(msgQueue.get(),2000)
                        if not isinstance(m,n2kISOTransportProtocolDataTransferMsg):
                            log(LOG_ERROR,"not data")
                            return
                        if m.Sequence!=sid:
                            log(LOG_ERROR,"bad sid")
                            return
                        msgs[sid-1]=bytes(m.Data)
                        sid+=1
            data=(b''.join(msgs))[:msg.MessageSize]
            eom=n2kISOTransportProtocolConnectionEOMMsg()
            eom.dst=src
            eom.TotalPackets=len(msgs)
            eom.TotalMessageSize=msg.MessageSize
            eom.PGN=pgn
            await self.sendMessage(eom)
            m=CreateMsgObject(msg.PGN,msg.priority,src,self.addr,data)
            m.ISO=True
            await self.bus.sendMessage(None,m)

 #       entries=self.msgProcessors.get(msg.src)
 #       if entries:
 #           for entry in entries:
 #               entry.cancel()
        processor=self.ISOTPProcessor(msgQueue,pgn)
        task=asyncio.create_task(run(self.registerMessageProcessor(msg.src,processor)))
        setTaskName(task,f"procRTS({self.inst})")
        return task
    
    async def processISOTPMsg(self,msg):
        if isinstance(msg,n2kISOTransportProtocolConnectionRTSMsg):
            await self.processRTS(msg)
    
    def FastProcessor(self,pgn,seq,queue):
        try:
            f=False
            while True:
                msg2=yield f
                if isinstance(msg2,n2kNmeaFastBase) and msg2.Seq==seq:
                    queue.put_nowait(msg2)
                    msg2.fp=True
                    f=True
                else:
                    f=False
        except GeneratorExit:
            queue.put(None)

    async def processFastMsg(self,msg):
        if hasattr(msg,"fp"): return
        msgQueue=Queue()
        pkts=[]
        target=0xffffffff
        actual=0
        pktlen=0
        def update(msg):
            f=msg.Frame
            actual|=1<<f
            if f==0:
                pktlen=msg.PacketLength
                pkts=1+pktlen//7
                target=(1<<pkts)-1
                data=msg.data[2:]
            else:
                data=msg.data[1:]
            if f<=len(pkts): pkts.extend([None for x in range(1+f-len(pkts))])
            pkts[f]=data
            return actual==target
        async def run(regCtx):
            with regCtx():
                while True:
                    m = await asyncio.wait_for_ms(msgQueue.get(),2000)
                    if not m: return
                    if update(msg):
                         break
            data=(b''.join(pkts))[:pktlen]
            mm=CreateMsgObject(msg.pgn,msg.priority,self.addr,data,fast=False)
            self.bus.sendMessage(None,mm)
        update(msg)
        processor=FastProcessor(msg.pgn,msg.Seq,msgQueue)
        task=asyncio.create_task(run(self.RegisterMessageProcessor(msg.src,processor)))
        setTaskName(task,f"fastProc{msg.pgn}")
    
    async def processNMEAGroupFunctionMsg(self,msg):
        if isinstance(msg,n2kNMEAAckGroupMsg):
            return
        
        if isinstance(msg,n2kNMEARequestGroupMsg):
            await self.processNMEARequest(msg)
            
    async def processMessage(self,msg):
        log(LOG_DEBUG,f"proc msg {self.inst}: {msg}")
        if self.addr==N2K_ADDR_NULL:
            if type(msg) is not n2kISOAddressClaimMsg and type(msg) is not n2kISOCommandedAddressMsg:
                log(LOG_WARN,"no addr")
                return
        processed = False
        entries = self.msgProcessors.get(msg.src)
        if entries:
            for entry in entries:
                log(LOG_DEBUG,f"{self.addr}: sending to {entry}")
                processed |= entry.send(msg) or False
#        entry = self.rxmsgs.get(type(msg))
        entry=None
        for e in self.rxmsgs.items():
            if isinstance(msg,e[0]):
                entry=e
                break
        if entry:
            func=entry[1]
            if callable(func):
                return await func(msg)
            processed |= func or False
        if not processed and msg.dst!=N2K_ADDR_BROADCAST:
            m=n2kISOAcknowledgementMsg(dst=msg.src)
            m.Control = 1# NACK
            m.GroupFunction = 0xff
            m.PGN = msg.pgn
            await self.sendMessage(m)

    def registerMessageProcessor(self,src,proc):
        @contextmanger
        def msgReg():
            nonlocal self,src,proc
            try:
                yield
            finally:
                procs=self.msgProcessors.get(src)
                try:
                    self.log.debug("remove proc %s l:%d",proc,len(procs))
                    procs.remove(proc)
                    if not procs:
                        self.log.debug("no more procs")
                        self.msgProcessors.pop(src,None)
                    proc.exit()
                except ValueError:
                    self.log.error("unexpected proc")
        proc.send(None)
        procs=self.msgProcessors.get(src,None)
        if procs is None: procs=self.msgProcessors[src]=[]
        procs.append(proc)
        self.log.debug("procs added %s len %d",proc,len(procs))
        return msgReg
        
class n2kBus:
    def __init__(self):
        self.devices=[]
        self.can=None
        self.task=None
        self.queue = Queue()
        self.bamMsgs={}
        self.log=logging.getLogger("n2kbus")
        
    def AddDevice(self,dev):
        self.devices.append(dev)
        if self.task is not None:
            dev.start()
    
    def SetCan(self,can):
        self.can = can
        
    def start(self):
        for dev in self.devices:
            dev.start()
        self.task=asyncio.create_task(self.run())
        setTaskName(self.task,"bus")
    
    def stop(self):
        if self.task is not None:
            self.task.cancel()
            for dev in self.devices:
                dev.stop()
            self.task=None
    
    async def run(self):
        while True:
            (dev,msg)=await self.queue.get()
            self.log.debug("got msg %s from %x",type(msg),msg.src)
            await self.processMessage(dev,msg)
    
    async def sendMessage(self,src,msg):
        self.log.log(LOG_VERBOSE,f"sm: %d %d %d %s",msg.pgn,msg.src,msg.dst,msg.data)
        if not isinstance(msg.data,bytes): msg=msg.clone()
        await self.queue.put((src,msg))
        
    def BAMMessageRegister(self,src,processor):
        @contextmanager
        def BAMMessageRegisterCtx():
            try:
                yield
            finally:
                nonlocal self,src,processor
                if self.bamMsgs.get(src)==processor:
                    self.bamMsgs.pop(src,None)
                else:
                    log(LOG_WARN,"not me as proc")
        entry = self.bamMsgs.get(src)
        if entry:
            self.log.warning("canceling bam")
            entry.close()
        self.bamMsgs[src] = processor
        processor.send(None)
        return BAMMessageRegisterCtx
    
    async def processTPBAMMsg(self,src,msg):
        task = None
        msgQueue = Queue()
        BAM=msg
        processor=None
        def BAMProcessor():
            try:
                while True:
                    (src,msg2)=yield True
                    msgQueue.put_nowait(msg2)
            except GeneratorExit:
                task.cancel()
        async def run(regMsg):
            nonlocal BAM,msgQueue,src,processor
            msgs=BAM.Packets*[None]
            npkts=0
            with regMsg():
                while npkts<BAM.Packets:
                    try:
                        msg=await asyncio.wait_for_ms(msgQueue.get(),2000)
                    except asyncio.core.TimeoutError:
                        self.log.warning("bam timeout")
                        return
                    if isinstance(msg,n2kISOTransportProtocolConnectionAbortMsg):
                        self.log.info("bam abort")
                        return
                    if not isinstance(msg,n2kISOTransportProtocolDataTransferMsg):
                        self.log.error("Unexpected BAM:%s",msg)
                        return
                    if msg.Sequence<1 or msg.Sequence>BAM.Packets:
                        self.log.error("bam err seq")
                        return
                    if msgs[msg.Sequence-1] is None:
                        npkts+=1
                    msgs[msg.Sequence-1]=bytes(msg.Data)
            data=(b''.join(msgs))[:BAM.MessageSize]
            #print(data)
            m=CreateMsgObject(BAM.PGN,BAM.priority,BAM.src,N2K_ADDR_BROADCAST,data)
            await self.sendMessage(src,m)

        processor=BAMProcessor()
        task=asyncio.create_task(run(self.BAMMessageRegister(msg.src,processor)))
        setTaskName(task,f"procBAM({msg.src})")
        
    async def processTPMsg(self,src,msg):
        entry = self.bamMsgs.get(msg.src)
        if entry:
            entry.send((src,msg))
        else:
            self.log.log(LOG_ERROR,"no bam proc")
        
    async def processMessage(self,src,msg):
        self.log.warning("pm: %d s:%d d:%d %s", msg.pgn, msg.src,msg.dst,msg.data)
        log(LOG_DEBUG,f"{src.addr if src else None}: {msg}")
        if msg.dst==N2K_ADDR_BROADCAST:
            if isinstance(msg,n2kISOTransportProtocolConnectionBAMMsg):
                return await self.processTPBAMMsg(src,msg)
            if isinstance(msg,n2kISOTransportProtocolDataTransferMsg) or isinstance(msg,n2kISOTransportProtocolConnectionAbortMsg):
                return await self.processTPMsg(src,msg)
        dst = msg.dst
        for dev in self.devices:
            if dev is not src: # dont send back to self
                if dst==N2K_ADDR_BROADCAST or dst==dev.addr:
                    await dev.processMessage(msg)
        if self.can is not None and src is not None and src is not self.can:
            await self.sendCanMessage(msg)
    
    async def sendCanMessage(self,msg):
        if self.can:
            await self.can.sendMessage(msg)

class n2kDebugDevice(n2kDevice):
    def __init__(self,bus):
        super().__init__(bus,0x1234)
    async def setAddr(self,d,addr=10):
        m=n2kISOCommandedAddressMsg()
        m.dst=d.addr
        m.data=bytearray(d.name.data)
        m.NewSourceAddress=addr
        await self.sendMessage(m)

    async def getISORequestResponse(self,pgn,addr,timeout=5000):
        response=None
        ev=asyncio.Event()
        def ISOResponseProcessor():
            nonlocal response,ev
            try:
                f=False
                while True:
                    msg=yield f
                    log(LOG_DEBUG,"proc")
                    print(msg)
                    if isinstance(msg,n2kISOAcknowledgementMsg) and msg.PGN==pgn:
                        response = msg
                        ev.set()
                        f=True
                    elif msg.pgn==pgn:
                        response = msg
                        ev.set()
                        f=True
                    else:
                        f=False
            except GeneratorExit:
                ev.set()
        processor=ISOResponseProcessor()
        with self.registerMessageProcessor(addr,processor)():
            await asyncio.wait_for_ms(ev.wait(),timeout)
        return response

    async def reqPGN(self,d,p):
        m=n2kISORequestMsg()
        m.dst=d.addr
        m.PGN = p
        await self.sendMessage(m)
        resp = await self.getISORequestResponse(p,d.addr)
        return resp
    
    async def testBAM(self):
        m=n2kISOTransportProtocolConnectionBAMMsg()
        m.dst=N2K_ADDR_BROADCAST
        m.MessageSize=3
        m.Packets=1
        m.PGN=59904
        await self.sendMessage(m)
        await asyncio.sleep_ms(50)
        m=n2kISOTransportProtocolDataTransferMsg()#data=bytearray((60928*0x100).to_bytes(4,"little")))
        m.Data=data=bytearray((60928).to_bytes(4,"little"))
        m.dst=N2K_ADDR_BROADCAST
        m.Sequence=1
        await self.sendMessage(m)
    
    async def testISO(self,d):
        m=n2kISOTransportProtocolConnectionRTSMsg()
        m.dst=d.addr
        m.MessageSize=3
        m.Packets=1
        m.PacketsReply=1
        m.PGN=59904
        await self.sendMessage(m)
        m=await self.getISORequestResponse(60416,d.addr)
        if not isinstance(m,n2kISOTransportProtocolConnectionCTSMsg):
            print("not cts")
            return
        m=n2kISOTransportProtocolDataTransferMsg(data=bytearray((60928*0x100).to_bytes(4,"little")))
        m.dst=d.addr
        m.Sequence=1
        await self.sendMessage(m)
        m=await self.getISORequestResponse(60416,d.addr)
        if not isinstance(m,n2kISOTransportProtocolConnectionEOMMsg):
            print("not eom")
            return
        print("done")
        

n=n2kBus()
d=n2kDevice(n,1)
n.AddDevice(d)

d1=n2kDevice(n,2)
n.AddDevice(d1)

dd=n2kDebugDevice(n)
n.AddDevice(dd)

import CanEth
config=esp32.NVS("config")
host_buf=bytearray(20)
host_len=config.get_blob("host",host_buf)
host=host_buf[:host_len].decode()
CanEth.setupWifi()
c=CanEth.CanEth(host)
n.SetCan(c)

async def waiter():
    n.start()
    repl=asyncio.create_task(aiorepl.task())
    setTaskName(repl,"repl")
    log(LOG_DEBUG,"starting")
    #await asyncio.gather(asyncio.sleep(100),repl)
    try:
        await asyncio.wait_for(repl,100)
    except asyncio.core.TimeoutError:
        print("Time out")
    print("stopping")
    n.stop()
    await asyncio.sleep_ms(100)

async def test():
    m=n2kISOCommandedAddressMsg()
    m.dst=0
    m.data=d.name.data
    m.NewSourceAddress=10
    await n.sendMessage(None,m)

def run():
    asyncio.run(waiter())

print("run")
run()

#asyncio.run_until_complete(waiter())
#asyncio.create_task(waiter())
#asyncio.run_until_complete()

