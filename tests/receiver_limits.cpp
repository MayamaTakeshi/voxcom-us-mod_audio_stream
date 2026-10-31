#include "WebSocketReceiver.h"
#include <cassert>
#include <iostream>
struct Sink : IWebSocketSinks {
 int error=0; bool rxCompressionEnabled() const override { return true; }
 bool rxIsTerminating() const override { return error != 0; }
 void onRxProtocolError(uint16_t c,std::string&&) override {error=c;}
 void onRxPong(std::vector<uint8_t>&&) override {}
 void onRxPing(std::vector<uint8_t>&&) override {}
 void onRxClose(uint16_t,std::string&&) override {}
 void onRxText(std::string&&) override {}
 void onRxBinary(std::vector<uint8_t>&&) override {}
};
int main() {
 { Sink sink; WebSocketReceiver r(sink); auto* b=evbuffer_new();
   unsigned char h[]={0x82,127,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff};
   evbuffer_add(b,h,sizeof(h));r.onData(b);assert(sink.error==1009);evbuffer_free(b); }
 { Sink sink; WebSocketReceiver r(sink);std::vector<unsigned char> a(1024*1024,0);
   r.handleDataFrame(a.data(),a.size(),false,2,false);
   r.handleContinuationFrame(a.data(),1,true);assert(sink.error==1009); }
 { Sink sink; WebSocketReceiver r(sink);PerMessageDeflateConfig c;c.enabled=true;
   assert(r.initializeCompression(c)); std::vector<unsigned char> a(2*1024*1024,0);
   const uint8_t* out;size_t n;bool compressed;assert(r.txPrepare(a.data(),a.size(),true,out,n,compressed));
   std::vector<uint8_t> decoded;assert(compressed);assert(!r.rxInflate(out,n,decoded)); }
 std::cout << "receiver limits: PASS\n";
}
