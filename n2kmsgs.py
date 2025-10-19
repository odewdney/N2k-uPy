import logging

N2K_ADDR_BROADCAST=0xff
N2K_ADDR_NULL=0xfe

log=logging.getLogger("n2kmsgs")

msgs={}

def GetMsgClass(pgn,data=None):
    msgentry=msgs.get(pgn)
    if isinstance(msgentry,dict):
        cls=msgentry.get(None)
        for e in msgentry.items():
            if e[0] is not None and data[:len(e[0])]==e[0]:
                cls=e[1]
        msgentry=cls
    if msgentry is None:
        msgentry=lambda priority,src,dst,data:n2kUnknownMsg(pgn=pgn,priority=priority,src=src,dst=dst,data=data)
    return msgentry

def CreateMsgObject(pgn,priority,src,dst,data,fast=False):
    if fast and IsFast(pgn):
        msgentry=lambda priority,src,dst,data:(n2kNmeaFastMsg if data and len(data)>0 and (data[0]&0xf)!=0 else n2kNmeaFastFirstMsg)(pgn=pgn,priority=priority,src=src,dst=dst,data=data)
    else:
        msgentry=GetMsgClass(pgn,data)
    msg=msgentry(priority=priority,src=src,dst=dst,data=data)
    return msg

def IsManu(pgn):
    return pgn==0xef00 or (pgn>=0xff00 and pgn<=0xffff) or pgn==0x1ef00 or (pgn>=0x1ff00 and pgn<=0x1ffff)

def IsFast(pgn):
    if pgn>=0x1f000 and pgn<=0x1feff: return None # mixed
    return (pgn>=0x1ed00 and pgn<=0x1efff) or (pgn>=0x1ff00 and pgn<=0x1ffff)

def getPDU(pgn):
    if (pgn>=0xe800 and pgn<=0xefff) or (pgn>=0x1ed00 and pgn<=0x1efff) or pgn>=0x1ff00 and pgn<=0x1ffff:
        return 1
    if (pgn>=0xf000 and pgn<=0xffff) or (pgn>=0x1f000 and pgn<=0x1feff):
        return 2
    if pgn==0:
        return 1
    
def n2kMsgType(priority=6,pgn=None,fast=None,indexed=None,match=None):
    def decorator(klass):
        nonlocal fast
        #functools.wraps(klass)
        msgentry = msgs.get(pgn)
        if match is None and msgentry is None:
            msgs[pgn]=klass
        else:
            if msgentry is None:
                msgentry={}
                msgs[pgn]=msgentry
            elif not isinstance(msgentry,dict):
                msgentry = {None:msgentry}
                msgs[pgn] = msgentry
            msgentry[match]=klass
        klass.pgn=pgn
        klass.priority=priority
        pgnfast=IsFast(pgn)
        if pgnfast is None:
            if fast is None:
                log.warning("No fast for %x",pgn)
        elif fast is not None: # not in mixed range
            log.warning("Unexpected fast for pgn:%x",pgn)
        else:
            fast=pgnfast

        pdu=getPDU(pgn)
        if pdu is None:
            log.error("unexpected pdu:%x",pgn)
        if pdu==1 and (pgn&0xff)!=0:
            log.error("unexpected dst in pgn:%x",pgn)

        klass.pdu=pdu
        klass.manu=IsManu(pgn)
        klass.fast=fast
        if indexed: klass.indexed=indexed
        if match: klass.match=match
        return klass
    return decorator

class n2kValue:
    pass
class n2kNAValue(n2kValue):
    def __init__(self,value,bits):
        self.bits=bits
        if bits>=12: self.value=value
    def __repr__(self):
        if self.bits>=12 and self.value!=((1<<self.bits)-1):
            return f"NA:{self.value&((1<<(self.bits-8))-1)}"
        return "NA"
    def __bool__(self):
        return False
    def __int__(self):
        return self.getValue()
    def __index__(self):
        return self.getValue()
    def getValue(self):
        if self.bits<12: return (1<<self.bits)-1
        return self.value
    @staticmethod
    def isNAValue(value,bits):
        if bits<2: return False
        if bits<12:
            return value==((1<<bits)-1)
        return (value>>(bits-8))==0xff
    
class n2kErrorValue(n2kValue):
    def __init__(self,value,bits):
        self.bits=bits
        if bits>=12: self.value=value
    def __repr__(self):
        if self.bits>=12:
            return f"Err:{self.value&((1<<(self.bits-8))-1)}"
        return "Err"
    def __int__(self):
        return self.getValue()
    def __index__(self):
        return self.getValue()
    def getValue(self):
        if self.bits<12: return (1<<self.bits)-1
        return self.value
    @staticmethod
    def isErrValue(value,bits):
        if bits<2: return False
        if bits<12:
            return value==((1<<bits)-2)
        return (value>>(bits-8))==0xfe
    
class n2kField:
    def __init__(self,index,offset,default=None,scale=None,unit=None,repr=None,enum=None,nullable=True,errorable=True,name=None):
        self.index=index
        self.offset=offset
        self.default=default
        if repr is not None: self.repr=repr
        if enum: self.enum=enum
        self.nullable=nullable
        self.errorable=errorable
        if name: self.name=name
    def __set_name__(self,owner,name):
        self.name=name
        if self.index < 1: # virtual field
            log.debug(f"setting f2:{self} for {owner}")
            f2=getattr(owner,"fields2",[])
            if "fields2" not in owner.__dict__: owner.fields2=f2=f2.copy()
            f2.append(self)
            return
        f=getattr(owner,"fields",[])
        if "fields" not in owner.__dict__:
            f=f.copy()
            owner.fields=f
        if len(f)<self.index:
            f.extend((self.index-len(f)) * [None])
        if f[self.index-1] is not None:
            raise Exception("dup field")
        f[self.index-1]=self
    def clone(self,index,offset,**kwargs):
        return self.__class__(index,offset,name=self.name,**kwargs)
    def __get__(self,obj,objtype=None):
        raise Exception("not imp")
    def __set__(self,obj,value):
        raise Exception("not imp")
    def init(self,obj):
        if self.default is not None:
            if len(obj.data)<=self.GetOffset(obj) and self.GetFieldSize(obj)>0:
                setattr(obj,self.name,self.default)
    def GetValueSize(self,obj):
        return self.fieldsize
    def GetFieldSize(self,obj):
        return self.fieldsize
    def GetOffset(self,obj):
        if isinstance(self.offset, int):
            return self.offset
        return self.offset.GetEndOffset(obj)
    def GetEndOffset(self,obj):
        return self.GetOffset(obj)+self.GetFieldSize(obj)
    def GetSlice(self,obj,length): # length is min size
        return obj.GetSlice(length)
    def GetRangeSlice(self,obj,start,length):
        slice = self.GetSlice(obj,start+length)
        return slice[start:start+length]
    def GetFieldSlice(self,obj):
        offset = self.GetOffset(obj)
        length = self.GetFieldSize(obj)
        return self.GetRangeSlice(obj,offset,length)
    def GetResizedSlice(self,obj,length):
        offset = self.GetOffset(obj)
        curlen = self.GetFieldSize(obj)
        if length==curlen:
            s = self.GetRangeSlice(obj,offset+length)
        else:
            s=obj.GetResizedSlice(offset,curlen,length)
        return s
        
class n2kByteField(n2kField):
    def __init__(self,index,offset,bitoffset=0,bits=8,**kwargs):
        super().__init__(index,offset,**kwargs)
        self.bitoffset=bitoffset
        self.bits=bits
        self.fieldsize=1
    def clone(self,index,offset):
        return super().clone(index,offset,bitoffset=self.bitoffset,bits=self.bits)
    def __get__(self,obj,objtype=None):
        val=(self.GetFieldSlice(obj)[0] >> self.bitoffset) & ((1<<self.bits)-1)
        if self.nullable and n2kNAValue.isNAValue(val,self.bits):
            return n2kNAValue(val,self.bits)
        return val
    def __set__(self,obj,value):
        offset=self.GetOffset(obj)
        log.debug(f"setting {self.name} {obj} {offset} {value}")
        s=self.GetSlice(obj,offset+1)
        mask=((1<<self.bits)-1)
        value=(int(value)&mask)<<self.bitoffset
        mask=~(mask<<self.bitoffset)
        s[offset] = (s[offset] & mask) | value

class n2kUIntNField(n2kField):
    def __init__(self,index,offset,size,bits=None,bitoffset=0,order="little",scale=None,valueoffset=None,unit=None,**kwargs):
        super().__init__(index,offset,**kwargs)
        self.bitoffset=bitoffset
        self.bits=bits or size*8
        self.fieldsize=size
        self.order=order
        if scale is not None: self.scale=scale
        if valueoffset is not None: self.voffset=valueoffset
        if unit is not None: self.unit=unit
    def clone(self,index,offset):
        return super().clone(index,offset,bits=self.bits,bitoffset=self.bitoffset)
    def __get__(self,obj,objtype=None):
        b=self.GetFieldSlice(obj)
        value=int.from_bytes(b,self.order)
        value=(value >> self.bitoffset) & ((1<<self.bits)-1)
        if self.nullable and n2kNAValue.isNAValue(value,self.bits): return n2kNAValue(value,self.bits)
        if hasattr(self,"scale"): value*=self.scale
        if hasattr(self,"voffset"): value+=self.voffset
        return value
    def __set__(self,obj,value):
        if not isinstance(value,n2kValue):
            if hasattr(self,"voffset"): value-=self.voffset
            if hasattr(self,"scale"): value=round(value/self.scale)
        b=self.GetFieldSlice(obj)
        data=int.from_bytes(b,self.order)
        mask=((1<<self.bits)-1)
        value=(int(value)&mask)<<self.bitoffset
        mask=~(mask<<self.bitoffset)
        data=(data & mask) | value
        log.debug(f"setting {b.hex()} {self.fieldsize} {data.to_bytes(self.fieldsize,self.order).hex()}")
        b[0:self.fieldsize]=data.to_bytes(self.fieldsize,self.order)
    def GetValueSize(self):
        return (self.bits+7)//8

class n2kUInt8Field(n2kUIntNField):
    def __init__(self,index,offset,bitoffset=0,bits=8,**kwargs):
        super().__init__(index,offset,1,bitoffset=bitoffset,bits=bits,**kwargs)

class n2kUInt16Field(n2kUIntNField):
    def __init__(self,index,offset,bitoffset=0,bits=16,**kwargs):
        super().__init__(index,offset,2,bitoffset=bitoffset,bits=bits,**kwargs)

class n2kUInt24Field(n2kUIntNField):
    def __init__(self,index,offset,bitoffset=0,bits=24,**kwargs):
        super().__init__(index,offset,3,bitoffset=bitoffset,bits=bits,**kwargs)

class n2kUInt32Field(n2kUIntNField):
    def __init__(self,index,offset,bitoffset=0,bits=32,**kwargs):
        super().__init__(index,offset,4,bitoffset=bitoffset,bits=bits,**kwargs)

class n2kFixedString(n2kField):
    def __init__(self,index,offset,length, encoding='utf-8',**kwargs):
        super().__init__(index,offset, **kwargs)
        self.fieldsize=length
        self.encoding=encoding
    def clone(self,index,offset):
        return super().clone(index,offset,length=self.length,encoding=self.encoding)
    def __get__(self,obj,objtype=None):
        b = self.GetFieldSlice(obj)
        return str(b, self.encoding).split('\x00',1)[0]
    def __set__(self,obj,value):
        b = self.GetFieldSlice(obj)
        l = len(b)
        d = value.encode(self.encoding)
        ld = len(d)
        if l<=ld:
            b[0:l] = d[0:l]
        else:
            b[0:ld]=d
            b[ld:]=bytearray(l-ld)

class n2kLAStringField(n2kField):
    def __init__(self,index,offset,encoding='utf-8',**kwargs):
        super().__init__(index,offset, **kwargs)
        self.encoding=encoding
    def clone(self,index,offset):
        return super().clone(index,offset,encoding=self.encoding)
    def GetFieldSize(self,obj):
        offset = self.GetOffset(obj)
        b = self.GetRangeSlice(obj,offset,2)
        if b[0]==1: raise Exception("bad length")
        if b[0]<=2 or b[0]==0xff: return 2
        return b[0]
    def __get__(self,obj,objtype=None):
        len = self.GetFieldSize(obj)
        if len <= 2: return ""
        offset = self.GetOffset(obj)
        b = self.GetRangeSlice(obj,offset,len)
        if b[1]==0: #unicode
            s = str(b[2:], "utf-16") # not support
        else:
            s = str(b[2:], self.encoding)
        return s.split('\x00',1)[0]
    def __set__(self,obj,value):
        d = value.encode(self.encoding)
        dl=len(d)+2
        s = self.GetResizedSlice(obj,dl)
        s[0]=dl
        s[1]=1
        s[2:]=d

class n2kDataField(n2kField):
    def __init__(self,index,offset,size,**kwargs):
        super().__init__(index,offset,**kwargs)
        self.fieldsize=size
    def __get__(self,obj,objType=None):
        return self.GetFieldSlice(obj)
    def __set__(self,obj,value):
        s=self.GetFieldSlice(obj)
        s[0:len(value)]=value
    

class n2kMsg:
    def __init__(self,src=N2K_ADDR_NULL,dst=N2K_ADDR_BROADCAST,priority=None,pgn=None,data=None):
        if priority:
            self.priority=priority
        if pgn:
            self.pgn=pgn
        self.dst=dst
        self.src=src
        if data:
            self.data=data
        elif hasattr(self,"match"):
            self.data = bytearray(self.match)
        else:
            self.data=bytearray()
        self.dataview=memoryview(self.data)
        if hasattr(self,"fields"):
            for f in self.fields:
                f.init(self)
        if hasattr(self,"fields2"):
            for n in range(len(self.fields2)):
                self.fields2[n].init(self)
    def __repr__(self):
        s = "{name} PGN={pgn} src={src} dst={dst}:".format(name=type(self).__name__,pgn=self.pgn,src=self.src,dst=self.dst)
        for f in self.fields:
            if f is not None:
                name=f.name if not hasattr(f,"repr") else f.repr
                value=f.__get__(self,None) #getattr(self,f.name,None)
                #if hasattr(f,"scale"): value*=f.scale
                if isinstance(value,memoryview): value=value.hex()
                if hasattr(f,"enum"): value=f.enum.GetValue(value)
                if hasattr(f,"unit"): value=f"{value}{f.unit}"
                s += " {name}:{value}".format(name=name,value=value)
        return s
    def GetSlice(self,length):
        obj=self
        if len(obj.dataview) < length:
            data=obj.data
            data.extend((length-len(data))*b'\xff')
            obj.data=data
            obj.dataview=memoryview(data)
        return obj.dataview
    def GetResizedSlice(self,offset,curlen,length):
        data=self.data
        if length>curlen:
            ext=length-curlen
            data.extend(ext*b'\xff')
            data[offset+length:]=data[offset+curlen:len(data)-ext]
        elif length<curlen:
            ext=curlen-length
            data[offset+length:len(data)-ext]=data[offset+curlen:]
            data=data[:len(data)-ext]
        self.data=data
        self.dataview=memoryview(data)
        return self.dataview[offset:offset+length]

@n2kMsgType(pgn=0)
class n2kUnknownMsg(n2kMsg):
    body=n2kByteField(1,0)
    
@n2kMsgType(pgn=0)
class n2kNmeaFastBase(n2kMsg):
    Seq=n2kByteField(1,0,bitoffset=4,bits=4)
    Frame=n2kByteField(2,0,bitoffset=0,bits=4)

class n2kNmeaFastFirstMsg(n2kNmeaFastBase):
    PacketLength=n2kByteField(3,1)
    Data6=n2kDataField(5,2,6)
    
class n2kNmeaFastMsg(n2kNmeaFastBase):
    Data7=n2kDataField(3,1,7)

@n2kMsgType(pgn=59392) # 0xe800
class n2kISOAcknowledgementMsg(n2kMsg):
    Control=n2kByteField(1,0)
    GroupFunction=n2kByteField(2,1)
    Reserved=n2kUInt24Field(3,2)
    PGN=n2kUInt24Field(4,5)

#0xe900

@n2kMsgType(pgn=59904) # 0xea00
class n2kISORequestMsg(n2kMsg):
    PGN=n2kUInt24Field(1,0)

@n2kMsgType(pgn=60160,priority=7) # 0xeb00
class n2kISOTransportProtocolDataTransferMsg(n2kMsg):
    Sequence = n2kByteField(1,0)
    Data = n2kDataField(2,1,7)

class n2kEnum:
    @classmethod
    def GetValue(cls,value):
        v=[x for x in dir(cls) if not x.startswith("__") and getattr(cls,x)==value]
        if len(v)>0: return v[0]
        return value

class n2kISOTransportProtocolConnectionFunctionEnum(n2kEnum):
    RTS=0x10
    CTS=0x11
    EOM=0x12
    BAM=0x20
    Abort=0xff

@n2kMsgType(pgn=60416) # 0xec00
class n2kISOTransportProtocolConnectionManagementBase(n2kMsg):
    GroupFunction = n2kByteField(1,0,enum=n2kISOTransportProtocolConnectionFunctionEnum) # 16=RTS, 17=CTS, 19-EOM, 32=Broadcast, 255=Abort
    PGN = n2kUInt24Field(5,5) # common
    # variable
    # RTS

@n2kMsgType(pgn=60416,priority=7,match=b'\x10') # 0xec00
class n2kISOTransportProtocolConnectionRTSMsg(n2kISOTransportProtocolConnectionManagementBase):
    MessageSize = n2kUInt16Field(2,1)    
    Packets = n2kByteField(3,3)
    PacketsReply = n2kByteField(4,4)    

@n2kMsgType(pgn=60416,priority=7,match=b'\x11') # 0xec00
class n2kISOTransportProtocolConnectionCTSMsg(n2kISOTransportProtocolConnectionManagementBase):
    MaxPackets = n2kByteField(2,1)
    NextSID = n2kByteField(3,2)    
    Reserved = n2kUInt16Field(4,3)    

@n2kMsgType(pgn=60416,priority=7,match=b'\x12') # 0xec00
class n2kISOTransportProtocolConnectionEOMMsg(n2kISOTransportProtocolConnectionManagementBase):
    TotalMessageSize = n2kUInt16Field(2,1)    
    TotalPackets = n2kByteField(3,3)
    Reserved = n2kByteField(4,4)    

@n2kMsgType(pgn=60416,priority=7,match=b'\x20') # 0xec00
class n2kISOTransportProtocolConnectionBAMMsg(n2kISOTransportProtocolConnectionManagementBase):
    MessageSize = n2kUInt16Field(2,1)    
    Packets = n2kByteField(3,3)
    Reserved = n2kByteField(4,4)    

@n2kMsgType(pgn=60416,priority=7,match=b'\xff') # 0xec00
class n2kISOTransportProtocolConnectionAbortMsg(n2kISOTransportProtocolConnectionManagementBase):
    Reason = n2kByteField(2,1)
    Reserved1 = n2kByteField(3,2)
    Reserved2 = n2kUInt16Field(4,3)    

#0xed00

class n2kIndustryCode(n2kEnum):
    Global=0
    Highway=1
    Agriculture=2
    Construction=3
    Marine=4
    Industrial=5

class n2kDeviceClass(n2kEnum):
    Reserved=0
    SystemTools=10
    SafetySystems=20
    InternetworkDevice=25
    ElectricalDistribution=30
    ElectricalGeneration=35
    SteeringAndControlSurfaces=40
    Propulsion=50
    Navigation=60
    Communication=70
    SensorCommunicationInterface=75
    InstrumentationSystems=80
    ExternalEnvironment=85
    InternalEnvironment=90
    DeckSystems=100
    HumanInterface=110
    Display=120
    Entertainment=125

@n2kMsgType(pgn=60928) # 0xee00
class n2kISOAddressClaimMsg(n2kMsg):
    UniqueNumber = n2kUInt32Field(1,0,bits=21)
    ManufacturerCode = n2kUInt32Field(2,0,bitoffset=21,bits=11,repr='mcode')
    DeviceInstance = n2kByteField(3,4,repr='di')
    DeviceFunction = n2kByteField(4,5,repr='df')
    res1= n2kByteField(5,6,bits=1)
    DeviceClass = n2kByteField(6,6,bits=7,bitoffset=1,repr='cls',enum=n2kDeviceClass)
    SystemInstance = n2kByteField(7,7,bits=4,repr="si")
    IndustryGroup = n2kByteField(8,7,bits=3,bitoffset=4,default=4,repr='ig',enum=n2kIndustryCode) # marine
    ArbitaryAddress =n2kByteField(9,7,bits=1,bitoffset=7,default=1,repr='ArbAddr')

    def GetNameInt(self):
        return int.from_bytes(self.data[0:8],"little")

@n2kMsgType(pgn=61184) # 0xef00 - addressed
class n2kManufacturerProprietaryAddrMsg(n2kMsg):
    ManufacturerCode = n2kUInt16Field(1,0,bits=11)
    IndustryGroup = n2kUInt16Field(3,0,bitoffset=13,bits=3,default=4,enum=n2kIndustryCode)
    PropertyId = n2kByteField(4,2)
    # data
    
@n2kMsgType(pgn=65240) # 0xFED8 - multi pkt BAM
class n2kISOCommandedAddressMsg(n2kISOAddressClaimMsg):
    NewSourceAddress=n2kByteField(10,8)

@n2kMsgType(pgn=65280,priority=2) # 0xff00-0xffff - Non-addressed
class n2kManufacturerProprietaryMsg(n2kMsg):
    ManufacturerCode = n2kUInt16Field(1,0,bits=11)
    IndustryGroup = n2kUInt16Field(3,0,bitoffset=13,bits=3,default=4,enum=n2kIndustryCode)
    PropertyId = n2kByteField(4,2)
    # data
    
#126208
@n2kMsgType(pgn=126208, priority=3) # 0x1ED00
class n2kNMEAGroupFunctionBase(n2kMsg):
    FunctionCode = n2kByteField(1,0)
    PGN = n2kUInt24Field(2,1)

class n2kParamField(n2kField):
    def GetField(self,obj):
        if not isinstance(obj,n2kParamObj): obj=self.obj
        pgn=obj.obj.PGN
        index=obj.Index
        if isinstance(index,n2kNAValue): return None
        #print(f"Lookup {pgn} {index}")
        cls=msgs.get(pgn)
        if not cls or index==0xff:
            return None
        field=cls.fields[index-1]
        return field.clone(self.index,self.offset)
    def __get__(self,obj,objtype):
        f=self.GetField(obj)
        if f is None: return "N/A"
        return f.__get__(obj,None)
    def __set__(self,obj,value):
        #print(f"set val:{obj} {obj.obj}")
        f=self.GetField(obj)
        f.__set__(obj,value)
    def GetFieldSize(self,obj):
        f=self.GetField(obj)
        if f is None: return 1
        return f.GetFieldSize(obj)

class n2kCompoundField(n2kField):
    def __init__(self,obj,index,offset):
        self.obj=obj
        super().__init__(index,offset)
        self.fields=self.fields.copy()
        log.debug(f"setting fields on {obj} {index} {offset}")
        for n in range(len(self.fields)):
            f=self.fields[n].clone(index+n,offset)
            f.obj=self
            offset=f
            self.fields[n]=f
            #setattr(self,f.name,f)
            #object.__setattr__(self,f.name,f)
            log.debug(f"set name:{f.name} {f.index} {f.offset}")
    def __set_name__(self,owner,name):
        for f in self.fields:
            f.__set_name__(owner,f"{name}.{f.name}")
    def GetSlice(self,length):
        offset=self.GetOffset(self.obj)
        return self.obj.GetSlice(offset+length)[offset:]
    def GetFieldSize(self,obj):
        sz=0
        for f in self.fields:
            sz+=f.GetFieldSize(self.obj)
        return sz
    def GetResizedSlice(self,offset,curlen,length):
        o=self.GetOffset(self.obj)
        return self.obj.GetResizedSlice(offset+o,curlen,length)

class n2kParamObj(n2kCompoundField):
    Index=n2kByteField(1,0)
    Value=n2kParamField(2,1)

class n2kArrayFieldImpl:
    def __init__(self,obj,index,offset,type,count,name):
        self.obj=obj
        self.index=index
        self.offset=offset
        self.count=count
        self.name=name
        self.type=type
        self.entries=[]
        if not hasattr(obj,"data"):
            self._SetCount()
    def __getitem__(self,key):
        ret=self.entries[key]
        if isinstance(ret,n2kCompoundField):
            return ret
        return ret.__get__(self.obj,None)
    def __setitem__(self,key,value):
        ret=self.entries[key]
        if isinstance(ret,n2kCompoundField):
            raise Exception("cant set field")
        ret.__set__(self.obj,value)
    def __len__(self):
        return len(self.entries)
    def _SetCount(self):
        if self.count: self.count.__set__(self.obj,len(self.entries))
    def _GetCount(self):
        return self.count.__get__(self.obj,None)
    def GetOffset(self,obj):
        if isinstance(self.offset, int):
            return self.offset
        return self.offset.GetEndOffset(obj)
    def GetFieldSize(self,obj):
        sz=0
        for e in self.entries:
            sz+=e.GetFieldSize(self.obj)
        return sz
    def _add(self):
        l=len(self.entries)
        offset = self.GetOffset(self.obj)
        if l>0:
            last=self.entries[l-1]
            offset = last#.GetEndOffset(self.obj)
        if issubclass(self.type,n2kCompoundField):
            num = len(self.type.fields) if hasattr(self.type,"fields") else 1
            val=self.type(self.obj,self.index+l*num,offset)
        else:
            val=self.type(self.index+l,offset)
        val.__set_name__(self.obj,f"{self.name}[{l}]")
        self.entries.append(val)
        return l
    def add(self):
        l=self._add()
        self._SetCount()
        return l
    def init(self,obj):
        log.debug(f"init {obj}")
        if not hasattr(obj,"data"): return
        data=obj.data
        offset=self.GetOffset(obj)
        if len(data)>offset:
            if self.count:
                for n in range(self._GetCount()):
                    self._add()
            elif len(data)>0:
                while offset<len(data):
                    n=self._add()
                    offset=self.entries[n].GetEndOffset(obj)
                if offset!=len(data):
                    raise Exception("bad array data")

class n2kArrayField(n2kField):
    def __init__(self,index,offset,type,count=None):
        super().__init__(0,offset)
        self.count=count
        self.aindex=index
        self.type=type
    def __get__(self,obj,objtype=None):
        val=getattr(obj,"_"+self.name,None)
        if not val:
            val=n2kArrayFieldImpl(obj,self.aindex,self.offset,self.type,self.count,self.name)
            setattr(obj,"_"+self.name,val)
        return val
    def GetFieldSize(self,obj):
        val=getattr(obj,"_"+self.name,None)
        return val.GetFieldSize(obj) if val else 0
    def init(self,obj):
        if not hasattr(obj,"data"): return
        self.__get__(obj).init(obj)

@n2kMsgType(pgn=126208, priority=3, match=b'\x00') # 0x1ED00
class n2kNMEARequestGroupMsg(n2kNMEAGroupFunctionBase):
    TransmissionInterval = n2kUInt32Field(3,4,scale=0.001,unit="s")
    TransmissionOffset = n2kUInt16Field(4,8,scale=0.01,unit="s")
    ParameterCount = n2kByteField(5,10)
    Parameters = n2kArrayField(6,11,type=n2kParamObj,count=ParameterCount)

@n2kMsgType(pgn=126208, priority=3, match=b'\x01') # 0x1ED00
class n2kNMEACommandGroupMsg(n2kNMEAGroupFunctionBase):
    Priority = n2kByteField(3,4,bits=4)
    Reserved = n2kByteField(4,4,bits=4,bitoffset=4)
    ParameterCount = n2kByteField(5,5)
    Parameters = n2kArrayField(6,6,type=n2kParamObj,count=ParameterCount)

@n2kMsgType(pgn=126208, priority=3, match=b'\x02') # 0x1ED00
class n2kNMEAAckGroupMsg(n2kNMEAGroupFunctionBase):
    ErrorCode = n2kByteField(3,4,bits=4)
    ErrorCode2 = n2kByteField(4,4,bits=4,bitoffset=4)
    ParameterCount = n2kByteField(5,5)
    Parameters = n2kArrayField(6,6,type=n2kByteField,count=ParameterCount)

class n2kUInt16FieldX(n2kUInt16Field):
    def GetFieldSize(self,obj):
        if IsManu(obj.PGN):
            return super().GetFieldSize(obj)
        return 0

class n2kNMEAFieldsGroupBase(n2kNMEAGroupFunctionBase):
    ManufacturerCode = n2kUInt16FieldX(3,4,bitoffset=0,bits=11,repr='mcode')
    reserved = n2kUInt16FieldX(4,4,bitoffset=11,bits=2,repr='mcode')
    IndustryGroup = n2kUInt16FieldX(5,4,bits=3,bitoffset=13,default=4,repr='ig',enum=n2kIndustryCode) # marine
    UniqueID = n2kByteField(6,IndustryGroup)
    SelectionCount = n2kByteField(7,UniqueID)
    ParameterCount = n2kByteField(8,SelectionCount)
    Selections = n2kArrayField(9,ParameterCount,type=n2kParamObj,count=SelectionCount)

@n2kMsgType(pgn=126208, priority=3, match=b'\x03') # 0x1ED00
class n2kNMEAReadFieldsGroupMsg(n2kNMEAFieldsGroupBase):
    Parameters = n2kArrayField(10,n2kNMEAFieldsGroupBase.Selections,type=n2kByteField,count=n2kNMEAFieldsGroupBase.ParameterCount)

@n2kMsgType(pgn=126208, priority=3, match=b'\x04') # 0x1ED00
class n2kNMEAReadFieldsReplyGroupMsg(n2kNMEAFieldsGroupBase):
    Parameters = n2kArrayField(10,n2kNMEAFieldsGroupBase.Selections,type=n2kParamObj,count=n2kNMEAFieldsGroupBase.ParameterCount)

@n2kMsgType(pgn=126208, priority=3, match=b'\x05') # 0x1ED00
class n2kNMEAWriteFieldsGroupMsg(n2kNMEAFieldsGroupBase):
    Parameters = n2kArrayField(10,n2kNMEAFieldsGroupBase.Selections,type=n2kParamObj,count=n2kNMEAFieldsGroupBase.ParameterCount)

@n2kMsgType(pgn=126208, priority=3, match=b'\x06') # 0x1ED00
class n2kNMEAWriteFieldsReplyGroupMsg(n2kNMEAFieldsGroupBase):
    Parameters = n2kArrayField(10,n2kNMEAFieldsGroupBase.Selections,type=n2kParamObj,count=n2kNMEAFieldsGroupBase.ParameterCount)

@n2kMsgType(pgn=126464) # 0x1EE00
class n2kNMEAPGNList(n2kMsg):
    FunctionCode=n2kByteField(1,0)
    PGNs=n2kArrayField(2,1,n2kUInt24Field)
    class FunctionCodes:
        TX=0
        RX=1

class CtrlEnum(n2kEnum):
    ErrorActive=0
    ErrorPassive=1
    BusOff=2
class EquipStatusEnum(n2kEnum):
    Operational=0
    Fault=1

@n2kMsgType(pgn=126993, priority=7, fast=False) # 0x1F011
class n2kHeartbeatMsg(n2kMsg):
    Offset = n2kUInt16Field(1,0,unit="s",scale=0.01)
    Sequence = n2kByteField(2,2)
    ControllerState1 = n2kByteField(3,3,bits=2,bitoffset=0,default=3,enum=CtrlEnum)
    ControllerState2 = n2kByteField(4,3,bits=2,bitoffset=2,default=3,enum=CtrlEnum)
    EquipmentState = n2kByteField(5,3,bits=2,bitoffset=4,default=0,enum=EquipStatusEnum)
    reserved1 = n2kByteField(6,3,bits=2,bitoffset=6)
    reserved2 = n2kUInt32Field(7,4)
        
@n2kMsgType(pgn=126996, fast=True ) # 0x1F014
class n2kProductInformationMsg(n2kMsg):
    Version = n2kUInt16Field(1,0)
    ProductCode = n2kUInt16Field(2,2)
    ModelID = n2kFixedString(3,ProductCode,32)
    SoftwareVersion = n2kFixedString(4,ModelID,32)
    ModelVersion = n2kFixedString(5,68,32)
    ModelSerialCode = n2kFixedString(6,100,32)
    CertLevel = n2kByteField(7,132)
    LoadEquiv = n2kByteField(8,133)

@n2kMsgType(pgn=126998, fast=True) #0x1F016
class n2kConfigurationInformationMsg(n2kMsg):
    InstallationDescription1 = n2kLAStringField(1,0)
    InstallationDescription2 = n2kLAStringField(2,InstallationDescription1)
    ManufacturerDescription = n2kLAStringField(3,InstallationDescription2)

x=None
def test():
    global x
    d=b'\x12\x34\x56\x78\x12\x34\x56'
    #d=bytearray(d)
    x=n2kNMEAPGNList(data=d)
    #x.PGNs.add()
    #x.PGNs.add()
    print(x.PGNs)
    print(x.PGNs[0])
    print(hex(x.PGNs[0]))
    print(hex(x.PGNs[1]))

m=None
def t():
    global m
#    p=True
    print("create")
    m=n2kNMEARequestGroupMsg()
    print("set pgn")
    m.PGN=126998 #126993
    print("add")
    m.Parameters.add()
    print("index")
    m.Parameters[0].Index=1
    print("value")
    m.Parameters[0].Value="hello"
    
if __name__ == '__main__':
    level=logging.DEBUG
    logger=logging.getLogger()
    logger.setLevel(level)
    logger.handlers[0].setLevel(level)
    test()
