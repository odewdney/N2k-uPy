import network
import esp32
import socket
import time

def configWifi(ssid,pwd):
    nvs=esp32.NVS("wifi")
    nvs.set_i32("enabled",1)
    nvs.set_blob("ssid",ssid)
    nvs.set_blob("pass",pwd)
    nvs.commit()

def setupWifi():
    nvs=esp32.NVS("wifi")
    try:
        enabled=nvs.get_i32("enabled")
        if enabled:
            buf=bytearray(100)
            def getStr(key):
                l=nvs.get_blob(key,buf)
                return buf[:l].decode()
            w=network.WLAN()
            if w.status()==network.STAT_GOT_IP: return
            w.active(True)
            w.connect(getStr("ssid"),getStr("pass"))
            
            # Wait for connect or fail
            max_wait = 10
            while max_wait > 0:
                status=w.status()
                if status not in [network.STAT_IDLE,network.STAT_CONNECTING]:
                    if status!=network.STAT_GOT_IP:
                        print(f"connection status...{status}")
                    break
                max_wait -= 1
                print(f'waiting for connection...{status}')
                time.sleep(1)
    except Exception as e:
        print(f"setup wifi failed {e}")

#https://www.proconx.com/assets/files/products/caneth/canframe.pdf
class CanEth:
    def __init__(self,addr):
        self.addr=addr
        self.skt=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.dst=(addr,11898)
        self.skt.connect(self.dst)
    @staticmethod
    def getAddr(msg):
        ret=msg.priority<<26
        ret|=msg.pgn<<8
        if (msg.pgn&0xff00)<0xf000:ret|=(msg.dst&0xff)<<8
        ret|=msg.src&0xff
        return ret
    async def sendMessage(self,msg):
        buf=bytearray(25)
        buf[:8]=b'ISO11898'
        buf[8]=1
        buf[9]=1
        pgn=self.getAddr(msg)
        buf[10:14]=pgn.to_bytes(4,"little")
        l=len(msg.data)
        if l>8:l=8
        buf[14]=l
        buf[15:15+l]=msg.data[0:l]
        buf[23]=1 #ext
        buf[24]=0 #rtr
        
        self.skt.send(buf)
        
if __name__ == '__main__':
    class TestMsg:
        def __init__(self):
            self.priority=6
            self.pgn=126993  #0xef00
            self.src=0x43
            self.dst=0x87
            self.pdu=1
            self.data=b'helloxxx'


    async def test():
        config=esp32.NVS("config")
        host_buf=bytearray(20)
        host_len=config.get_blob("host",host_buf)
        host=host_buf[:host_len].decode()
        can = CanEth(host)
        msg=TestMsg()
        await can.sendMessage(msg)

    import asyncio
    
    setupWifi()
    asyncio.run(test())
    