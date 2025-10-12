import asyncio
import logging
from n2klog import setTaskName

#logging.setLevel(logging.VERBOSE)

#import esp32can as can
#import functools
#from asyncio import Queue
from primitives.queue import Queue
import aiorepl

#import mip
#mip.install("logging")
#mip.install("aiorepl")
#mip.install("github:peterhinch/micropython-async/v3/primitives")

from n2kmsgs import *

LOG_VERBOSE=logging.VERBOSE
LOG_DEBUG=logging.DEBUG
LOG_NORMAL=logging.INFO
LOG_WARN=logging.WARNING
LOG_ERROR=logging.ERROR

logger=logging.getLogger("n2k")
log=logger.log

class n2kDevice:
    def __init__(self,bus,inst):
        self.bus=bus
        self.inst=inst
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
        self.conf.InstallationDescription1 = "desc1"
        self.conf.InstallationDescription2 = "desc2"
        self.conf.ManufacturerDescription = "mdesc"
        
        self.heartbeat = n2kHeartbeatMsg()
        
        self.txmsgs = {n2kISOAcknowledgementMsg:None,n2kISORequestMsg:None,
                     n2kISOTransportProtocolConnectionManagementBase:None, n2kISOTransportProtocolDataTransferMsg:None,
                     # n2k group, txrxlist
                     type(self.name):self.name,type(self.pi):self.pi,type(self.conf):self.conf,type(self.heartbeat):self.heartbeat,
                     n2kNMEAPGNList:self.processPGNList}
        
        self.rxmsgs = {n2kISOAcknowledgementMsg:None,n2kISORequestMsg:self.processISORequest,
                       n2kISOTransportProtocolConnectionManagementBase:self.processISOTPMsg,n2kISOTransportProtocolDataTransferMsg:None,
                       n2kISOAddressClaimMsg:self.processISOAddressClaim,n2kISOCommandedAddressMsg:self.processISOCommandedAddress,
                       n2kNMEAGroupFunctionBase:self.processNMEAGroupFunctionMsg}
        self.msgProcessors = {}

    async def sendMessage(self,msg):
        if self.addr==N2K_ADDR_NULL:
            log(LOG_ERROR,"Cant send with no addr")
            return
        if hasattr(msg,"ISO") and len(msg.data)>8:
            if msg.dst==N2K_ADDR_BROADCAST:
                task=asyncio.create_task(self.sendISOBAMMessage(msg))
                setTaskName(task,f"sendBAM({self.inst})")
            else:
                task=asyncio.create_task(self.sendISOMessage(msg))
                setTaskName(task,f"sendISO({self.inst})")
            return
        msg.src=self.addr
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
                    abort=n2kISOTransportProtocolConnectionAbortMsg(dst=msg.dst,src=self.addr)
                    abort.PGN=msg.pgn
                    abort.Reason=3 #timeout
                    await self.bus.sendMessage(self,abort)
                    return
                if isinstance(cts,n2kISOTransportProtocolConnectionEOMMsg):
                    return
                if not isinstance(cts,n2kISOTransportProtocolConnectionCTSMsg):
                    log(LOG_WARN,"unexpected TP msg")
                    return
                if cts.NextSID>p:
                    log(LOG_ERROR,"bad nSID")
                for s in range(cts.NextSID, min(p+1,cts.NextSID+cts.MaxPackets)):
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
    def stop(self):
        if self.task is not None:
            self.task.cancel()
            self.task=None
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
            log(LOG_DEBUG,f"addr: {self.name.src} => {msg.NewSourceAddress}")
            self.name.src = msg.NewSourceAddress
            self.addr_taken.set()
            
    async def processISORequest(self,msg):
        log(LOG_DEBUG,"Request msg %x from %x",msg.PGN,msg.src)
        entry=None
        for e in self.txmsgs.items():
            if e[0].pgn==msg.PGN:
                entry = e
        if entry:
            entryType = entry[0]
            entryValue = entry[1]
            if callable(entryValue):
                await entryValue(msg)
            else:
                for e in (entryValue if isinstance(entryValue,list) else [entryValue]):
                    m=entryType()
                    m.dst=msg.src if msg.PGN!=n2kISOAddressClaimMsg.pgn else N2K_ADDR_BROADCAST
                    m.data=bytes(e.data)
                    m.ISO=True
                    await self.sendMessage(m)
        elif msg.dst!=N2K_ADDR_BROADCAST:
            m=n2kISOAcknowledgementMsg(dst=msg.src)
            m.Control = 1# NACK
            m.GroupFunction = 0xff
            m.PGN = msg.PGN
            m.ISO=True
            await self.sendMessage(m)
            log(LOG_ERROR,"isoreg sent %s",m)    

    class ISOTPProcessor:
        def __init__(self,task,msgQueue,pgn):
            self.task=task
            self.msgQueue=msgQueue
            self.pgn=pgn
        def cancel(self):
            self.task.cancel()
        async def processMsg(self,msg2):
            if isinstance(msg2, n2kISOTransportProtocolConnectionManagementBase) and msg2.PGN==self.pgn:
                log(LOG_DEBUG,"got TP")
                await self.msgQueue.put(msg2)
                return True
            elif isinstance(msg2, n2kISOTransportProtocolDataTransferMsg):
                log(LOG_DEBUG,"got data")
                await self.msgQueue.put(msg2)
                return True
            else:
                log(LOG_DEBUG,"no processed")

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
        processor=n2kDevice.ISOTPProcessor(task,msgQueue,pgn)
        task=asyncio.create_task(run(self.registerMessageProcessor(msg.src,processor)))
        setTaskName(task,f"procRTS({self.inst})")
    
    async def processISOTPMsg(self,msg):
        if isinstance(msg,n2kISOTransportProtocolConnectionRTSMsg):
            await self.processRTS(msg)
        
    async def processPGNList(self,msg):
        async def domsgs(fc,msgs):
            m=n2kNMEAPGNList()
            m.dst=msg.src
            m.FunctionCode=fc
            for mm in msgs.keys():
                n=m.PGNs.add()
                m.PGNs[n]=mm.pgn
            if hasattr(msg,"ISO"):
                m.ISO=True
            await self.sendMessage(m)
        await domsgs(n2kNMEAPGNList.FunctionCodes.TX,self.txmsgs)
        await domsgs(n2kNMEAPGNList.FunctionCodes.RX,self.rxmsgs)
        
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
                processed |= await entry.processMsg(msg)
#        entry = self.rxmsgs.get(type(msg))
        entry=None
        for e in self.rxmsgs.items():
            if isinstance(msg,e[0]):
                entry=e[1]
                break
        if entry:
            return await entry(msg)
        if not processed and msg.dst!=N2K_ADDR_BROADCAST:
            m=n2kISOAcknowledgementMsg(dst=msg.src)
            m.Control = 1# NACK
            m.GroupFunction = 0xff
            m.PGN = msg.pgn
            await self.sendMessage(m)

    def registerMessageProcessor(self,src,proc):
        class msgReg:
            def __enter__(self2):
                pass
            def __exit__(self2, exc_type, exc_value, traceback):
                nonlocal self,src,proc
                procs=self.msgProcessors.get(src)
                if proc in procs:
                    log(LOG_DEBUG,f"{self.addr}: remove proc {proc} l:{len(procs)}")
                    procs.remove(proc)
                    if len(procs)==0:
                        log(LOG_DEBUG,"no more procs")
                        self.msgProcessors.pop(src,None)
                else:
                    log(LOG_ERROR,"unexpected proc")
        procs=self.msgProcessors.get(src,[])
        procs.append(proc)
        log(LOG_DEBUG,f"{self.addr}: procs added {proc} len {len(procs)}")
        self.msgProcessors[src]=procs
        return msgReg
        
class n2kBus:
    def __init__(self):
        self.devices=[]
        self.can=None
        self.task=None
        self.queue = Queue()
        self.bamMsgs={}
        
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
            #await asyncio.sleep(1)
            (dev,pri,src,dst,pgn,data) = await self.queue.get()
            print(f"rm: {pgn} {src} {dst} {data}")
            msg = CreateMsgObject(pgn,pri,src,dst,data)
            if msg is None:
                print("no message {p}".format(p=pgn))
                continue
            log(LOG_DEBUG,"got msg %s from %x",type(msg),src)
            await self.processMessage(dev,msg)
    
    async def sendMessageParts(self,src,priority,srcAddr,dst,pgn,data):
        if not isinstance(data,bytes): data=bytes(data)
        print(f"sm: {pgn} {srcAddr} {dst} {data}")
        await self.queue.put((src,priority,srcAddr,dst,pgn,data))
    async def sendMessage(self,src,msg):
        await self.sendMessageParts(src,msg.priority,msg.src,msg.dst,msg.pgn,bytes(msg.data))
        
    def BAMMessageRegister(self,src):
        class BAMMessageRegisterCtx:
            def __init__(self,processor):
                self.processor=processor
            def __enter__(self2):
                pass
            def __exit__(self2,exc_type, exc_value, traceback):
                nonlocal self,src
                if self.bamMsgs.get(src)==self2.processor:
                    self.bamMsgs.pop(src,None)
                else:
                    log(LOG_WARN,"not me as proc")
        return BAMMessageRegisterCtx
    
    async def processTPBAMMsg(self,src,msg):
        task = None
        msgQueue = Queue()
        BAM=msg
        processor=None
        class BAMProcessor:
            def cancel(self):
                nonlocal task
                task.cancel()
            async def processMsg(self,src,msg):
                nonlocal msgQueue
                await msgQueue.put(msg)
        async def run(regMsg):
            nonlocal BAM,msgQueue,src,processor
            msgs=BAM.Packets*[None]
            npkts=0
            with regMsg(processor):
                while npkts<BAM.Packets:
                    try:
                        msg=await asyncio.wait_for_ms(msgQueue.get(),2000)
                    except asyncio.core.TimeoutError:
                        log(LOG_WARN, "bam timeout")
                        return
                    if isinstance(msg,n2kISOTransportProtocolConnectionAbortMsg):
                        log(LOG_NORMAL,"bam abort")
                        return
                    if not isinstance(msg,n2kISOTransportProtocolDataTransferMsg):
                        log(LOG_ERROR,"Unexpected BAM:%s",msg)
                        return
                    if msg.Sequence<1 or msg.Sequence>BAM.Packets:
                        log(LOG_ERROR,"bam err seq")
                        return
                    if msgs[msg.Sequence-1] is None:
                        npkts+=1
                    msgs[msg.Sequence-1]=bytes(msg.Data)
            data=(b''.join(msgs))[:BAM.MessageSize]
            #print(data)
            await self.sendMessageParts(src,BAM.priority,BAM.src,N2K_ADDR_BROADCAST,BAM.PGN,data)

        entry = self.bamMsgs.get(msg.src)
        if entry:
            log(LOG_WARN,"canceling bam")
            entry.cancel()
        #self.bamMsgs[msg.src] = self.ISOBAMMsgProcessor(self,src,msg,self.BAMMessageRegister(msg.src))
        processor=BAMProcessor()
        
        self.bamMsgs[msg.src] = processor
        task=asyncio.create_task(run(self.BAMMessageRegister(msg.src)))
        setTaskName(task,f"procBAM({msg.src})")
        
    async def processTPMsg(self,src,msg):
        entry = self.bamMsgs.get(msg.src)
        if entry:
            await entry.processMsg(src,msg)
        else:
            log(LOG_ERROR,"no bam proc")
        
    async def processMessage(self,src,msg):
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
        #log(LOG_DEBUG,"send {m}",m=msg)
        # pri[3] res[1] DP[1] PF[8] PS[8] src[8]
        pass

class n2kDebugDevice(n2kDevice):
    def __init__(self,bus):
        super().__init__(bus,0x1234)
    async def setAddr(self,d,addr=10):
        m=n2kISOCommandedAddressMsg()
        m.dst=d.addr
        m.data=d.name.data
        m.NewSourceAddress=addr
        await self.sendMessage(self,m)

    async def getISORequestResponse(self,pgn,addr,timeout=5000):
        response=None
        ev=asyncio.Event()
        class ISOResponseProcessor:
            def cancel(self):
                ev.set()
            async def processMsg(self,msg):
                nonlocal response,ev
                log(LOG_DEBUG,"proc")
                if isinstance(msg,n2kISOAcknowledgementMsg):
                    response = msg
                    ev.set()
                    return True
                elif msg.pgn==pgn:
                    response = msg
                    ev.set()
                    return True
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

n.SetCan(1)

async def waiter():
    n.start()
    repl=asyncio.create_task(aiorepl.task())
    setTaskName(repl,"repl")
    log(LOG_DEBUG,"starting")
    #await asyncio.gather(asyncio.sleep(100),repl)
    await asyncio.wait_for(repl,100)
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

