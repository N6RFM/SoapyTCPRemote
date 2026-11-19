//  LoopbackTest.cpp - end to end tests of SoapyTCPRemote against the fake device
//  usage: LoopbackTest <port> <mode> [key]
//    stream    verify sample integrity (CS16 x2 channels, then CF32 conversion)
//    robust    misbehaving clients must not take the server down
//    authok    connecting with the right key works
//    authbad   connecting without / with the wrong key is refused
//  SPDX-License-Identifier: BSL-1.0

#include <SoapySDR/Device.hpp>
#include <SoapySDR/Formats.hpp>
#include <cstdint>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static int failures = 0;
#define CHECK(cond, ...) do { if (!(cond)) { ++failures; printf("FAIL %s:%d: ", __FILE__, __LINE__); printf(__VA_ARGS__); printf("\n"); } } while (0)

static std::string g_port;

static SoapySDR::Device *connectDev(const char *key) {
    SoapySDR::Kwargs a;
    a["driver"] = "tcpremote";
    a["tcpremote:address"] = "127.0.0.1:" + g_port;
    a["tcpremote:driver"] = "faketest";
    if (key)
        a["tcpremote:key"] = key;
    return SoapySDR::Device::make(a);
}

static int rawConnect() {
    int s = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(atoi(g_port.c_str()));
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    if (connect(s, (struct sockaddr *)&sa, sizeof(sa))) {
        close(s);
        return -1;
    }
    return s;
}

static void send_(int s, const char *t) { if (write(s, t, strlen(t))<0) {} }

// CS16, two channels, odd read sizes: frames will straddle TCP reads
static void testStream() {
    SoapySDR::Device *dev = connectDev(nullptr);
    CHECK(dev!=nullptr, "make failed");
    if (!dev) return;
    CHECK(dev->getNumChannels(SOAPY_SDR_RX)==2, "channels");
    dev->setSampleRate(SOAPY_SDR_RX, 0, 1e6);
    CHECK(dev->getSampleRate(SOAPY_SDR_RX, 0)==1e6, "sample rate roundtrip");
    dev->setFrequency(SOAPY_SDR_RX, 0, 433.92e6);
    CHECK(dev->getFrequency(SOAPY_SDR_RX, 0)==433.92e6, "frequency roundtrip");

    SoapySDR::Stream *st = dev->setupStream(SOAPY_SDR_RX, SOAPY_SDR_CS16, std::vector<size_t>{0,1});
    CHECK(st!=nullptr, "setupStream");
    CHECK(dev->activateStream(st)==0, "activate");
    const size_t chunk = 37;
    std::vector<int16_t> b0(2*chunk), b1(2*chunk);
    void *bufs[2] = { b0.data(), b1.data() };
    long long expect = -1, total = 0;
    int timeouts = 0;
    while (total < 100000 && timeouts < 50) {
        int flags = 0; long long t = 0;
        int n = dev->readStream(st, bufs, chunk, flags, t, 200000);
        if (n==SOAPY_SDR_TIMEOUT) { ++timeouts; continue; }
        CHECK(n>0, "readStream returned %d", n);
        if (n<=0) break;
        for (int i=0; i<n; ++i) {
            long long cur = b0[2*i];
            if (expect>=0 && cur != (expect & 0x7fff)) {
                CHECK(false, "ch0 discontinuity at %lld: got %lld want %lld", total+i, cur, expect & 0x7fff);
                goto done;
            }
            if (expect<0) expect = cur;
            CHECK(b0[2*i+1]==(int16_t)((3*expect) & 0x7fff), "ch0 Q at %lld", total+i);
            CHECK(b1[2*i]==(int16_t)(expect & 0x7fff), "ch1 I at %lld", total+i);
            CHECK(b1[2*i+1]==(int16_t)((3*expect+100) & 0x7fff), "ch1 Q at %lld", total+i);
            ++expect;
            if (failures>10) goto done;
        }
        total += n;
    }
done:
    CHECK(total>=100000, "only received %lld samples", total);
    printf("CS16 x2: %lld samples verified\n", total);
    dev->deactivateStream(st);
    dev->closeStream(st);

    // CF32 requested: client side converts from the CS16 wire format
    std::vector<size_t> ch0{0};
    st = dev->setupStream(SOAPY_SDR_RX, SOAPY_SDR_CF32, ch0);
    CHECK(st!=nullptr, "setupStream CF32");
    if (st) {
        CHECK(dev->activateStream(st)==0, "activate CF32");
        std::vector<float> f(2*512);
        void *fb[1] = { f.data() };
        int flags = 0; long long t = 0;
        int n = dev->readStream(st, fb, 512, flags, t, 500000);
        CHECK(n>0, "CF32 read %d", n);
        long long c0 = n>0? (long long)(f[0]*INT16_MAX + 0.5f): 0;
        for (int i=0; i<n; ++i) {
            float wantI = (float)((c0+i) & 0x7fff)/(float)INT16_MAX;
            float wantQ = (float)((3*(c0+i)) & 0x7fff)/(float)INT16_MAX;
            if (f[2*i]!=wantI || f[2*i+1]!=wantQ) {
                CHECK(false, "CF32 mismatch at %d", i);
                break;
            }
        }
        printf("CF32: %d samples verified\n", n);
        dev->deactivateStream(st);
        dev->closeStream(st);
    }
    // an unreadable stream asks for a timeout, not a hang
    SoapySDR::Device::unmake(dev);
}

static void testRobust() {
    // 1. connect and hang up immediately (used to make the server exit)
    for (int i=0; i<5; ++i) { int s = rawConnect(); CHECK(s>=0, "connect"); if (s>=0) close(s); }
    // 2. garbage instead of a connection type
    { int s = rawConnect(); send_(s, "hello there\n"); close(s); }
    { int s = rawConnect(); send_(s, "9\n"); close(s); }
    // 3. an over-long line
    { int s = rawConnect(); std::string big(5000, 'x'); send_(s, big.c_str()); close(s); }
    // 4. identifies as RPC, then sends half a request and vanishes
    { int s = rawConnect(); send_(s, "0\nfaketest\n"); usleep(100000); close(s); }
    // 5. a silent client that stays connected must not block other clients
    int silent = rawConnect();
    // 6. RPC client that sends nonsense after loading the driver
    { int s = rawConnect(); send_(s, "0\nfaketest\n\n"); char b[32]; if (read(s, b, sizeof(b))<0) {} send_(s, "--\n99999\n"); usleep(100000); close(s); }
    usleep(200000);
    // the server must still be there, and serve a full session
    SoapySDR::Device *dev = connectDev(nullptr);
    CHECK(dev!=nullptr, "server did not survive bad clients");
    if (dev) {
        CHECK(dev->getNumChannels(SOAPY_SDR_RX)==2, "session after abuse");
        SoapySDR::Device::unmake(dev);
    }
    if (silent>=0) close(silent);
    printf("robust: server survived\n");
}

static void testAuth(bool good, const char *key) {
    SoapySDR::Device *dev = nullptr;
    try { dev = connectDev(good? key: nullptr); } catch (const std::exception &e) { dev = nullptr; }
    if (good) {
        CHECK(dev!=nullptr, "valid key refused");
        if (dev) { CHECK(dev->getNumChannels(SOAPY_SDR_RX)==2, "channels with key"); SoapySDR::Device::unmake(dev); }
        // data connections need the key too, so stream with it
        printf("auth: valid key accepted\n");
    } else {
        CHECK(dev==nullptr, "connection without key was accepted");
        if (dev) SoapySDR::Device::unmake(dev);
        try { dev = connectDev("wrong-key"); } catch (const std::exception &e) { dev = nullptr; }
        CHECK(dev==nullptr, "wrong key was accepted");
        if (dev) SoapySDR::Device::unmake(dev);
        printf("auth: missing / wrong key refused\n");
    }
}

int main(int argc, char **argv) {
    if (argc<3) { puts("usage: LoopbackTest <port> <mode> [key]"); return 2; }
    g_port = argv[1];
    std::string mode = argv[2];
    const char *key = argc>3? argv[3]: nullptr;
    if (mode=="stream") testStream();
    else if (mode=="robust") testRobust();
    else if (mode=="authok") testAuth(true, key);
    else if (mode=="authbad") testAuth(false, key);
    else { printf("unknown mode %s\n", mode.c_str()); return 2; }
    if (failures) { printf("%d FAILURE(S)\n", failures); return 1; }
    printf("PASS: %s\n", mode.c_str());
    return 0;
}
