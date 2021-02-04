//  SoapyTCPserver.cpp
//  Copyright (c) 2021 Phil Ashby
//  SPDX-License-Identifier: BSL-1.0

// Design approach is KISS, main thread accepts connections into a
// map, handles RPCs.
// Worker threads are created per data stream to pump in/out.
#include <SoapySDR/Logger.hpp>
#include <SoapySDR/Device.hpp>
#include "SoapyRPC.hpp"
#include <pthread.h>
#include <unistd.h>
#include <stdio.h>
#include <poll.h>
#include <sys/socket.h>
#include <netdb.h>

struct ConnectionInfo
{
    // NB: existance of an rpc object implies this is an RPC connection, otherwise data stream
    SoapyRPC *rpc;
    SoapySDR::Device *dev;
    FILE *netFp;
    int direction;
    double rate;
    size_t fSize;
    size_t numChans;
    SoapySDR::Stream *stream;
    volatile pthread_t pid;
};

static std::map<int, ConnectionInfo> s_connections;

int createRpc(int sock) {
    SoapySDR_log(SOAPY_SDR_DEBUG, "createRpc()");
    ConnectionInfo conn;
    conn.rpc = new SoapyRPC(sock);
    conn.rate = 0;
    // read driver and args..
    SoapySDR::Kwargs kwargs;
    kwargs["driver"] = conn.rpc->readString();
    std::string args = conn.rpc->readString();
    // args contains all driver name=value pairs, separated by '/',
    // splitting this is a faff as there is no native method..
    // http://www.cplusplus.com/faq/sequences/strings/split/
    size_t cur, nxt = -1;
    do {
        cur = nxt+1;
        nxt = args.find('/',cur);
        std::string arg = args.substr(cur, nxt-cur);
        // now we split this into name=value...
        size_t off = arg.find('=');
        if (off!=std::string::npos) {
            kwargs[arg.substr(0,off)]=arg.substr(off+1);
        }
    } while (nxt != std::string::npos);
    // make the device
    conn.dev = SoapySDR::Device::make(kwargs);
    if (!conn.dev) {
        // oops - report failure to client and drop connection
        SoapySDR_logf(SOAPY_SDR_ERROR,"failed to create SoapySDR::Device: %s", kwargs["driver"]);
        conn.rpc->writeInteger(-1);
        delete conn.rpc;
        return 0;
    }
    // all good - add to map and respond with map key
    s_connections[sock] = conn;
    conn.rpc->writeInteger(sock);
    SoapySDR_logf(SOAPY_SDR_INFO, "New RPC connection: %d", sock);
    return 0;
}

int createData(int sock, int type) {
    SoapySDR_logf(SOAPY_SDR_DEBUG, "createData, type: %d", type);
    ConnectionInfo conn;
    conn.rpc = nullptr;     // ensure we aren't treated as RPC stream
    conn.netFp = fdopen(sock,TCPREMOTE_DATA_SEND==type? "w": "r");
    if (!conn.netFp) {
        SoapySDR_logf(SOAPY_SDR_ERROR,"failed to open stdio stream on data connection: %s", strerror(errno));
        close(sock);
        return -1;
    }
    // all good - add to map and respond with map key
    // NB: we write to raw socket as stdio stream may be read-only..
    s_connections[sock] = conn;
    char id[10];
    int ilen = sprintf(id,"%d\n",sock);
    write(sock, id, ilen);
    SoapySDR_logf(SOAPY_SDR_INFO, "New data connection: %d", sock);
    return 0;
}

void *dataPump(void *ctx) {
    ConnectionInfo *conn = (ConnectionInfo *)ctx;
    // first - activate the underlying stream
    if (conn->dev->activateStream(conn->stream)) {
        SoapySDR_log(SOAPY_SDR_ERROR, "dataPump: failed to activate underlying stream");
        return nullptr;
    }
    // which direction?
    if (SOAPY_SDR_RX==conn->direction) {
        // calculate appropriate element count and block sizes for ~4Hz read rate
        size_t numElems = (int)(conn->rate / 4.0);
        size_t blkSize = numElems * conn->fSize;
        size_t bufSize = blkSize * conn->numChans;
        // allocate buffers for channel data and serialised network data
        void **buffs = (void **)alloca(conn->numChans);
        uint8_t *cbuf = (uint8_t *)alloca(bufSize);
        uint8_t *nbuf = (uint8_t *)alloca(bufSize);
        for (size_t c=0; c<conn->numChans; ++c)
            buffs[c] = cbuf+(c*blkSize);
        // read until told to stop!
        while (conn->pid!=0) {
            int flags = 0;
            long long time = 0;
            long timeout = 1000000; // 1 second
            int nread = conn->dev->readStream(conn->stream, buffs, numElems, flags, time, timeout);
            if (nread<0) {
                SoapySDR_logf(SOAPY_SDR_ERROR, "dataPump: error reading underlying stream: %d", nread);
                break;
            }
            // interleave samples across channels for network format
            uint8_t *pn = nbuf;
            for (int idx=0; idx<nread; ++idx) {
                size_t eoff = idx*conn->fSize;
                for (size_t c=0; c<conn->numChans; ++c) {
                    uint8_t *pc = (uint8_t *)buffs[c];
                    memcpy(pn, pc+eoff, conn->fSize);
                    pn += conn->fSize;
                }
            }
            // write to network
            if ((int)fwrite(nbuf, conn->fSize*conn->numChans, nread, conn->netFp)!=nread) {
                SoapySDR_logf(SOAPY_SDR_ERROR, "dataPump: error writing to network: %s", strerror(errno));
                break;
            }
        }
    } else {
        // TODO:XXX:
        SoapySDR_log(SOAPY_SDR_ERROR, "dataPump: unimplemented data receive funtion :=(");
    }
    // dropping out - deactivate underlying stream
    conn->dev->deactivateStream(conn->stream);
    return nullptr;
}

int handleListen(struct pollfd *pfd) {
    // oops before expected..
    if (pfd->revents & (POLLERR|POLLHUP)) {
        SoapySDR_log(SOAPY_SDR_ERROR,"EOF or error in listen socket");
        return -1;
    }
    // connect pending
    if (pfd->revents & POLLIN) {
        struct sockaddr addr;
        socklen_t len = sizeof(addr);
        int sock = accept(pfd->fd, &addr, &len);
        if (sock<0) {
            SoapySDR_logf(SOAPY_SDR_ERROR,"error accepting connection: %s", strerror(errno));
            return -1;
        }
        // now we read an integer which types the connection
        char buf[2];
        if (read(sock, buf, 2)<=0) {
            SoapySDR_logf(SOAPY_SDR_ERROR,"error reading connection type: %s", strerror(errno));
            close(sock);
            return -1;
        }
        int type = buf[0]-'0';
        // create appropriate ConnectionInfo and insert into map..
        if (TCPREMOTE_RPC_LOAD==type)
            return createRpc(sock);
        else if (TCPREMOTE_DATA_SEND==type || TCPREMOTE_DATA_RECV==type)
            return createData(sock, type);
        // ..or drop it as unknown.
        SoapySDR_logf(SOAPY_SDR_ERROR, "unknown connection type: %d", type);
        close(sock);
    }
    return 0;
}

int handleGetHardwareKey(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetHardwareKey()");
    conn.rpc->writeString(conn.dev->getHardwareKey());
    return 0;
}

int handleGetHardwareInfo(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetHardwareInfo()");
    conn.rpc->writeKwargs(conn.dev->getHardwareInfo());
    return 0;
}

int handleSetFrontendMapping(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleSetFrontendMapping()");
    // NB: Do NOT try and be clever and NEST the reads as device method arguments..
    // your friendly local compiler can re-order evaluation and thus read the
    // protocol incorrectly. I have been bitten once, PAA.
    int dir = conn.rpc->readInteger();
    std::string cfg = conn.rpc->readString();
    conn.dev->setFrontendMapping(dir, cfg);
    conn.rpc->writeInteger(0);
    return 0;
}

int handleGetFrontendMapping(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetFrontendMapping()");
    // NB: this we get away with since there is only one method argument..
    conn.rpc->writeString(conn.dev->getFrontendMapping(conn.rpc->readInteger()));
    return 0;
}

int handleGetNumChannels(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetNumChannels()");
    conn.rpc->writeInteger(conn.dev->getNumChannels(conn.rpc->readInteger()));
    return 0;
}

int handleGetChannelInfo(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetChannelInfo()");
    // NB: here we MUST read before calling the method, although the arguments
    // are the same type their values would be re-ordered.
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    conn.rpc->writeKwargs(conn.dev->getChannelInfo(dir, chn));
    return 0;
}

int handleGetFullDuplex(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetFullDuplex()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    conn.rpc->writeInteger(conn.dev->getFullDuplex(dir, chn));
    return 0;
}

int handleGetStreamFormats(ConnectionInfo &conn) {
    // pass-thru (with some serialisation)
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetStreamFormats()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    for (auto &fmt: conn.dev->getStreamFormats(dir,chn)) {
        conn.rpc->writeString(fmt);
    }
    // terminate list
    conn.rpc->writeString("");
    return 0;
}

int handleGetNativeStreamFormat(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetNativeStreamFormat()");
    double fullScale = 0.0;
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    std::string fmt = conn.dev->getNativeStreamFormat(dir,chn,fullScale);
    conn.rpc->writeString(fmt);
    conn.rpc->writeDouble(fullScale);
    return 0;
}

int handleGetStreamArgsInfo(ConnectionInfo &conn) {
    // not implemented, skeleton only
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetStreamArgsInfo()");
    conn.rpc->readInteger();
    conn.rpc->readInteger();
    conn.rpc->writeString("");
    return 0;
}

int handleSetupStream(ConnectionInfo &conn) {
    // The actually complex(ish) bit..
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleSetupStream()");
    int dataId = conn.rpc->readInteger();
    int direction = conn.rpc->readInteger();
    std::string fmt = conn.rpc->readString();
    std::string chans = conn.rpc->readString();
    SoapySDR::Kwargs args = conn.rpc->readKwargs();
    // find the data stream (client must connect a data stream first)
    if (s_connections.find(dataId)==s_connections.end()) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "setupStream: no such data stream ID: %d", dataId);
        conn.rpc->writeInteger(-1);
        return 0;
    }
    // find the frame size
    if (g_frameSizes.find(fmt)==g_frameSizes.end()) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "setupStream: unknown sample format: %s", fmt.c_str());
        conn.rpc->writeInteger(-2);
        return 0;
    }
    // check we have a sample rate
    if (conn.rate==0) {
        SoapySDR_log(SOAPY_SDR_ERROR, "setupStream: sample rate not set for connection");
        conn.rpc->writeInteger(-3);
        return 0;
    }
    // parse the channel list
    std::vector<size_t> channels;
    size_t cur;
    size_t nxt = -1;
    do {
        cur = nxt+1;
        nxt = chans.find(' ',cur);
        channels.push_back(atoi(chans.substr(cur, nxt-cur).c_str()));
    } while (nxt!=std::string::npos);
    // fill out the connection details
    ConnectionInfo &data = s_connections.at(dataId);
    data.dev = conn.dev;
    data.direction = direction;
    data.rate = conn.rate;
    data.fSize = g_frameSizes.at(fmt);
    data.numChans = channels.size();
    // open the underlying stream
    data.stream = conn.dev->setupStream(direction, fmt, channels, args);
    if (!data.stream) {
        SoapySDR_log(SOAPY_SDR_ERROR, "setupStream: failed to create underlying stream");
        conn.rpc->writeInteger(-4);
    }
    // all good!
    conn.rpc->writeInteger(dataId);
    return 0;
}

int handleCloseStream(ConnectionInfo &conn) {
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleCloseStream()");
    int dataId = conn.rpc->readInteger();
    if (s_connections.find(dataId)==s_connections.end()) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "closeStream: no such data stream ID: %d", dataId);
        return 0;
    }
    ConnectionInfo &data = s_connections.at(dataId);
    data.dev->closeStream(data.stream);
    SoapySDR_logf(SOAPY_SDR_INFO, "Closed data connection: %d", dataId);
    // no response
    return 0;
}

int handleGetStreamMTU(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetStreamMTU()");
    int dataId = conn.rpc->readInteger();
    if (s_connections.find(dataId)==s_connections.end()) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "getStreamMTU: no such data stream ID: %d", dataId);
        conn.rpc->writeInteger(-1);
        return 0;
    }
    return conn.rpc->writeInteger(conn.dev->getStreamMTU(s_connections.at(dataId).stream));
}

int handleActivateStream(ConnectionInfo &conn) {
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleActivateStream()");
    int dataId = conn.rpc->readInteger();
    if (s_connections.find(dataId)==s_connections.end()) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "activateStream: no such data stream ID: %d", dataId);
        conn.rpc->writeInteger(-1);
        return 0;
    }
    // start data pump thread
    ConnectionInfo &data = s_connections.at(dataId);
    data.pid = -1;  // non-zero, to prevent thread terminating if it's scheduled before we can copy in real value!
    pthread_t pid;
    if (pthread_create(&pid, nullptr, dataPump, &data)) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "activateStream: failed to create data pump thread: %s", strerror(errno));
        conn.rpc->writeInteger(-2);
        return 0;
    }
    data.pid = pid;
    conn.rpc->writeInteger(0);
    return 0;
}

int handleDeactivateStream(ConnectionInfo &conn) {
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleDeactivateStream()");
    int dataId = conn.rpc->readInteger();
    if (s_connections.find(dataId)==s_connections.end()) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "deactivateStream: no such data stream ID: %d", dataId);
        conn.rpc->writeInteger(-1);
        return 0;
    }
    // stop data pump thread
    ConnectionInfo &data = s_connections.at(dataId);
    pthread_t pid = data.pid;
    data.pid = 0;
    if (pthread_join(pid, nullptr)) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "deactivateStream: failed to join data pump thread: %s", strerror(errno));
        conn.rpc->writeInteger(-2);
        return 0;
    }
    conn.rpc->writeInteger(0);
    return 0;
}

int handleListAntennas(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleListAntennas()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    conn.rpc->writeStrVector(conn.dev->listAntennas(dir,chn));
    return 0;
}

int handleSetAntenna(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleSetAntenna()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    std::string nam = conn.rpc->readString();
    conn.dev->setAntenna(dir,chn,nam);
    conn.rpc->writeInteger(0);
    return 0;
}

int handleGetAntenna(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetAntenna()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    conn.rpc->writeString(conn.dev->getAntenna(dir,chn));
    return 0;
}

int handleListGains(ConnectionInfo &conn){
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleListGains()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    conn.rpc->writeStrVector(conn.dev->listGains(dir,chn));
    return 0;
}

int handleHasGainMode(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleHasGainMode()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    conn.rpc->writeInteger(conn.dev->hasGainMode(dir,chn));
    return 0;
}

int handleSetGainMode(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleSetGainMode()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    int set = conn.rpc->readInteger();
    conn.dev->setGainMode(dir,chn,set>0);
    conn.rpc->writeInteger(0);
    return 0;
}

int handleGetGainMode(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetGainMode()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    conn.rpc->writeInteger(conn.dev->getGainMode(dir,chn));
    return 0;
}

int handleSetGain(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleSetGain()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    double gain = conn.rpc->readDouble();
    conn.dev->setGain(dir,chn,gain);
    conn.rpc->writeInteger(0);
    return 0;
}

int handleSetGainNamed(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleSetGainNamed()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    std::string nam = conn.rpc->readString();
    double gain = conn.rpc->readDouble();
    conn.dev->setGain(dir,chn,nam,gain);
    conn.rpc->writeInteger(0);
    return 0;
}

int handleGetGain(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetGain()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    conn.rpc->writeDouble(conn.dev->getGain(dir,chn));
    return 0;
}

int handleGetGainNamed(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetGainNamed()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    std::string nam = conn.rpc->readString();
    conn.rpc->writeDouble(conn.dev->getGain(dir,chn,nam));
    return 0;
}

int handleGetGainRange(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetGainRange()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    SoapySDR::Range r = conn.dev->getGainRange(dir,chn);
    conn.rpc->writeDouble(r.minimum());
    conn.rpc->writeDouble(r.maximum());
    conn.rpc->writeDouble(r.step());
    return 0;
}

int handleGetGainRangeNamed(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetGainRangeNamed()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    std::string nam = conn.rpc->readString();
    SoapySDR::Range r = conn.dev->getGainRange(dir,chn,nam);
    conn.rpc->writeDouble(r.minimum());
    conn.rpc->writeDouble(r.maximum());
    conn.rpc->writeDouble(r.step());
    return 0;
}

int handleSetFrequency(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleSetFrequency()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    double frq = conn.rpc->readDouble();
    SoapySDR::Kwargs kwargs = conn.rpc->readKwargs();
    conn.dev->setFrequency(dir,chn,frq,kwargs);
    conn.rpc->writeInteger(0);
    return 0;
}

int handleSetFrequencyNamed(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleSetFrequencyNamed()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    std::string nam = conn.rpc->readString();
    double frq = conn.rpc->readDouble();
    SoapySDR::Kwargs kwargs = conn.rpc->readKwargs();
    conn.dev->setFrequency(dir,chn,nam,frq,kwargs);
    conn.rpc->writeInteger(0);
    return 0;
}

int handleGetFrequency(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetFrequency()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    conn.rpc->writeDouble(conn.dev->getFrequency(dir,chn));
    return 0;    
}

int handleGetFrequencyNamed(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetFrequencyNamed()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    std::string nam = conn.rpc->readString();
    conn.rpc->writeDouble(conn.dev->getFrequency(dir,chn,nam));
    return 0;    
}

int handleListFrequencies(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleListFrequencies()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    conn.rpc->writeStrVector(conn.dev->listFrequencies(dir,chn));
    return 0;
}

int handleGetFrequencyRange(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetFrequencyRange()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    SoapySDR::RangeList list = conn.dev->getFrequencyRange(dir,chn);
    for (auto r: list) {
        conn.rpc->writeDouble(r.minimum());
        conn.rpc->writeDouble(r.maximum());
        conn.rpc->writeDouble(r.step());
    }
    conn.rpc->writeDouble(0);
    conn.rpc->writeDouble(0);
    conn.rpc->writeDouble(-1.0);
    return 0;
}
int handleGetFrequencyRangeNamed(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetFrequencyRangeNamed()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    std::string nam = conn.rpc->readString();
    SoapySDR::RangeList list = conn.dev->getFrequencyRange(dir,chn,nam);
    for (auto r: list) {
        conn.rpc->writeDouble(r.minimum());
        conn.rpc->writeDouble(r.maximum());
        conn.rpc->writeDouble(r.step());
    }
    conn.rpc->writeDouble(0);
    conn.rpc->writeDouble(0);
    conn.rpc->writeDouble(-1.0);
    return 0;
}
int handleGetFrequencyArgsInfo(ConnectionInfo &conn) {
    // not implemented, skeleton only
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetFrequencyArgsInfo()");
    conn.rpc->readInteger();
    conn.rpc->readInteger();
    conn.rpc->writeString("");
    return 0;
}

int handleSetSampleRate(ConnectionInfo &conn) {
    // pass-thru, but remember for data stream setup
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleSetSampleRate()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    double rate = conn.rpc->readDouble();
    conn.dev->setSampleRate(dir,chn,conn.rate=rate);
    conn.rpc->writeInteger(0);
    return 0;
}

int handleGetSampleRate(ConnectionInfo &conn) {
    // pass-thru, but update rate if device altered it
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetSampleRate()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    conn.rpc->writeDouble(conn.rate=conn.dev->getSampleRate(dir,chn));
    return 0;    
}

int handleGetSampleRateRange(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetSampleRateRange()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    SoapySDR::RangeList list = conn.dev->getSampleRateRange(dir,chn);
    for (auto r: list) {
        conn.rpc->writeDouble(r.minimum());
        conn.rpc->writeDouble(r.maximum());
        conn.rpc->writeDouble(r.step());
    }
    conn.rpc->writeDouble(0);
    conn.rpc->writeDouble(0);
    conn.rpc->writeDouble(-1.0);
    return 0;
}

int handleRPC(struct pollfd *pfd) {
    // oops before expected..
    if (pfd->revents & (POLLERR|POLLHUP)) {
        SoapySDR_log(SOAPY_SDR_ERROR,"ERR or HUP on RPC socket");
        return -1;
    }
    // get connection..
    ConnectionInfo &conn = s_connections.at(pfd->fd);
    // dispatch requested RPC..
    int call = conn.rpc->readInteger();
    if (call<0) {
        SoapySDR_log(SOAPY_SDR_ERROR, "EOF or error on RPC socket");
        return -1;
    }
    SoapySDR_logf(SOAPY_SDR_DEBUG, "handleRPC: call=%d", call);
    switch (call) {
    // unknown
    default:
        SoapySDR_logf(SOAPY_SDR_ERROR,"Unknown RPC call: %d", call);
        conn.rpc->writeInteger(-1000);
        return -1;
    // special - dropping connection
    case TCPREMOTE_DROP_RPC:
        SoapySDR_logf(SOAPY_SDR_INFO,"Dropping connection: %d", pfd->fd);
        delete conn.rpc;
        SoapySDR::Device::unmake(conn.dev);
        s_connections.erase(pfd->fd);
        return 0;
    // identification API
    case TCPREMOTE_GET_HARDWARE_KEY:
        return handleGetHardwareKey(conn);
    case TCPREMOTE_GET_HARDWARE_INFO:
        return handleGetHardwareInfo(conn);
    // channel API
    case TCPREMOTE_SET_FRONTEND_MAPPING:
        return handleSetFrontendMapping(conn);
    case TCPREMOTE_GET_FRONTEND_MAPPING:
        return handleGetFrontendMapping(conn);
    case TCPREMOTE_GET_NUM_CHANNELS:
        return handleGetNumChannels(conn);
    case TCPREMOTE_GET_CHANNEL_INFO:
        return handleGetChannelInfo(conn);
    case TCPREMOTE_GET_FULL_DUPLEX:
        return handleGetFullDuplex(conn);
    // stream API
    case TCPREMOTE_GET_STREAM_FORMATS:
        return handleGetStreamFormats(conn);
    case TCPREMOTE_GET_STREAM_NATIVE_FORMAT:
        return handleGetNativeStreamFormat(conn);
    case TCPREMOTE_GET_STREAM_ARGS_INFO:
        return handleGetStreamArgsInfo(conn);
    case TCPREMOTE_SETUP_STREAM:
        return handleSetupStream(conn);
    case TCPREMOTE_CLOSE_STREAM:
        return handleCloseStream(conn);
    case TCPREMOTE_GET_STREAM_MTU:
        return handleGetStreamMTU(conn);
    case TCPREMOTE_ACTIVATE_STREAM:
        return handleActivateStream(conn);
    case TCPREMOTE_DEACTIVATE_STREAM:
        return handleDeactivateStream(conn);
    // antenna API
    case TCPREMOTE_LIST_ANTENNAS:
        return handleListAntennas(conn);
    case TCPREMOTE_SET_ANTENNA:
        return handleSetAntenna(conn);
    case TCPREMOTE_GET_ANTENNA:
        return handleGetAntenna(conn);
    // gain API
    case TCPREMOTE_LIST_GAINS:
        return handleListGains(conn);
    case TCPREMOTE_HAS_GAIN_MODE:
        return handleHasGainMode(conn);
    case TCPREMOTE_SET_GAIN_MODE:
        return handleSetGainMode(conn);
    case TCPREMOTE_GET_GAIN_MODE:
        return handleGetGainMode(conn);
    case TCPREMOTE_SET_GAIN:
        return handleSetGain(conn);
    case TCPREMOTE_SET_GAIN_NAMED:
        return handleSetGainNamed(conn);
    case TCPREMOTE_GET_GAIN:
        return handleGetGain(conn);
    case TCPREMOTE_GET_GAIN_NAMED:
        return handleGetGainNamed(conn);
    case TCPREMOTE_GET_GAIN_RANGE:
        return handleGetGainRange(conn);
    case TCPREMOTE_GET_GAIN_RANGE_NAMED:
        return handleGetGainRangeNamed(conn);
    // frequency API
    case TCPREMOTE_SET_FREQUENCY:
        return handleSetFrequency(conn);
    case TCPREMOTE_SET_FREQUENCY_NAMED:
        return handleSetFrequencyNamed(conn);
    case TCPREMOTE_GET_FREQUENCY:
        return handleGetFrequency(conn);
    case TCPREMOTE_GET_FREQUENCY_NAMED:
        return handleGetFrequencyNamed(conn);
    case TCPREMOTE_LIST_FREQUENCIES:
        return handleListFrequencies(conn);
    case TCPREMOTE_GET_FREQUENCY_RANGE:
        return handleGetFrequencyRange(conn);
    case TCPREMOTE_GET_FREQUENCY_RANGE_NAMED:
        return handleGetFrequencyRangeNamed(conn);
    case TCPREMOTE_GET_FREQUENCY_ARGS_INFO:
        return handleGetFrequencyArgsInfo(conn);
    // sample rate API
    case TCPREMOTE_SET_SAMPLE_RATE:
        return handleSetSampleRate(conn);
    case TCPREMOTE_GET_SAMPLE_RATE:
        return handleGetSampleRate(conn);
    case TCPREMOTE_GET_SAMPLE_RATE_RANGE:
        return handleGetSampleRateRange(conn);
    /* NOT IMPLEMENTED ON CLIENT YET!
    // frontend corrections API
    TCPREMOTE_HAS_DC_OFFSET_MODE,
    TCPREMOTE_SET_DC_OFFSET_MODE,
    TCPREMOTE_GET_DC_OFFSET_MODE,
    TCPREMOTE_HAS_DC_OFFSET,
    TCPREMOTE_SET_DC_OFFSET,
    TCPREMOTE_GET_DC_OFFSET,
    TCPREMOTE_HAS_IQ_BALANCE,
    TCPREMOTE_SET_IQ_BALANCE,
    TCPREMOTE_GET_IQ_BALANCE,
    TCPREMOTE_HAS_FREQUENCY_CORRECTION,
    TCPREMOTE_SET_FREQUENCY_CORRECTION,
    TCPREMOTE_GET_FREQUENCY_CORRECTION,
    // bandwidth API
    TCPREMOTE_SET_BANDWIDTH,
    TCPREMOTE_GET_BANDWIDTH,
    // list bandwidths deprecated, we emulate in client side
    TCPREMOTE_GET_BANDWIDTH_RANGE,
    // clocking API
    TCPREMOTE_SET_MASTER_CLOCK_RATE,
    TCPREMOTE_GET_MASTER_CLOCK_RATE,
    TCPREMOTE_GET_MASTER_CLOCK_RATES,
    TCPREMOTE_LIST_CLOCK_SOURCES,
    TCPREMOTE_SET_CLOCK_SOURCE,
    TCPREMOTE_GET_CLOCK_SOURCE,
    // time API
    TCPREMOTE_LIST_TIME_SOURCES,
    TCPREMOTE_SET_TIME_SOURCE,
    TCPREMOTE_GET_TIME_SOURCE,
    TCPREMOTE_HAS_HARDWARE_TIME,
    TCPREMOTE_GET_HARDWARE_TIME,
    TCPREMOTE_SET_HARDWARE_TIME,
    TCPREMOTE_SET_COMMAND_TIME,
    // sensor API
    TCPREMOTE_LIST_SENSORS,
    TCPREMOTE_GET_SENSOR_INFO,
    TCPREMOTE_READ_SENSOR,
    TCPREMOTE_LIST_CHANNEL_SENSORS,
    TCPREMOTE_GET_CHANNEL_SENSOR_INFO,
    TCPREMOTE_READ_CHANNEL_SENSOR,
    // register API
    TCPREMOTE_LIST_REGISTER_INTERFACES,
    TCPREMOTE_WRITE_REGISTER_NAMED,
    TCPREMOTE_READ_REGISTER_NAMED,
    TCPREMOTE_WRITE_REGISTER,
    TCPREMOTE_READ_REGISTER,
    TCPREMOTE_WRITE_REGISTERS_NAMED,
    TCPREMOTE_READ_REGISTERS_NAMED,
    // settings API
    TCPREMOTE_GET_SETTING_INFO,
    TCPREMOTE_WRITE_SETTING,
    TCPREMOTE_READ_SETTING,
    TCPREMOTE_GET_CHANNEL_SETTING_INFO,
    TCPREMOTE_WRITE_CHANNEL_SETTING,
    TCPREMOTE_READ_CHANNEL_SETTING,
    // GPIO API
    TCPREMOTE_LIST_GPIO_BANKS,
    TCPREMOTE_WRITE_GPIO,
    TCPREMOTE_WRITE_GPIO_MASKED,
    TCPREMOTE_READ_GPIO,
    TCPREMOTE_WRITE_GPIO_DIR,
    TCPREMOTE_READ_GPIO_DIR,
    // I2C API
    TCPREMOTE_WRITE_I2C,
    TCPREMOTE_READ_I2C,
    // SPI API
    TCPREMOTE_TRANSACT_SPI,
    // UART API
    TCPREMOTE_LIST_UARTS,
    TCPREMOTE_WRITE_UART,
    TCPREMOTE_READ_UART */
    }
    return 0;
}

int usage() {
    puts("usage: SoapyTCPServer [-?|--help] [-l <listen host/IP:default *>] [-p <listen port: default 20655>]");
    return 0;
}

int main(int argc, char **argv) {
    const char *host = "0.0.0.0";
    const char *port = "20655";           // 0x50AF
    for (int arg=1; arg<argc; ++arg) {
        if (strncmp(argv[arg],"-?",2)==0 || strncmp(argv[arg],"--h",3)==0)
            return usage();
        else if (strncmp(argv[arg],"-h",2)==0)
            host = argv[++arg];
        else if (strncmp(argv[arg],"-p",2)==0)
            port = argv[++arg];
    }
    printf("SoapyTCPServer: listening on: %s:%s\n", host, port);
    // Set up listen socket
    struct addrinfo *res = nullptr;
    if (getaddrinfo(host, port, nullptr, &res)) {
        SoapySDR_logf(SOAPY_SDR_ERROR,"parsing listen host");
        return 1;
    }
    int lsock = socket(AF_INET, SOCK_STREAM, 0);
    int opt=1;
    setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(int));
    if (lsock<0 || bind(lsock, res->ai_addr, res->ai_addrlen)!=0) {
        SoapySDR_logf(SOAPY_SDR_ERROR,"binding listen socket");
        return 2;
    }
    listen(lsock, 5);
    freeaddrinfo(res);
    // Wait for connections / requests on RPC sockets
    while (true) {
        // allocate maximum possible number of pollfds, then omit data stream connections
        size_t nfds = s_connections.size()+1;
        struct pollfd *pfds = (struct pollfd *)alloca(nfds);
        pfds[0].fd = lsock;
        pfds[0].events = POLLIN;
        pfds[0].revents = 0;
        nfds = 1;
        for (auto it=s_connections.begin(); it!=s_connections.end(); ++it) {
            if (it->second.rpc) {  // RPC connection
                pfds[nfds].fd = it->first;
                pfds[nfds].events = POLLIN;
                pfds[nfds].revents = 0;
                ++nfds;
            }
        }
        if (poll(pfds, nfds, -1)<=0) {
            SoapySDR_logf(SOAPY_SDR_ERROR,"waiting for input");
            return 3;
        }
        // Handle listen socket events
        if (pfds[0].revents && handleListen(pfds)<0)
            break;
        // Handle RPC socket events
        for (size_t idx=1; idx<nfds; ++idx)
            if (pfds[idx].revents && handleRPC(pfds+idx)<0)
                return 4;
    }
    s_connections.clear();
    close(lsock);
    return 0;
}
