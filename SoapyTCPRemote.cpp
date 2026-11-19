//  SoapyTCPRemote.cpp
//  Copyright (c) 2021 Phil Ashby
//  SPDX-License-Identifier: BSL-1.0

#include "SoapyTCPRemote.hpp"
#include "SoapyLog.hpp"

#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <poll.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netdb.h>
#include <vector>
#include <algorithm>

// declare the contents of a Stream object for ourselves
class SoapySDR::Stream
{
public:
    int netSock;
    int remoteId;
    int numChans;
    size_t fSize;
    bool running;
    // receive staging buffer, and how many bytes of it are valid (always less than
    // one whole frame between calls: the tail of a frame split across TCP reads)
    std::vector<uint8_t> rxbuf;
    size_t rxhave = 0;
    // transmit staging buffer
    std::vector<uint8_t> txbuf;
};

SoapyTCPRemote::SoapyTCPRemote(const std::string &address, const std::string &port, const std::string &remdriver, const std::string &remargs, const std::string &remkey) :
    remoteAddress(address),
    remotePort(port),
    remoteDriver(remdriver),
    remoteArgs(remargs),
    remoteKey(remkey),
    rpc(nullptr),
    log(nullptr),
    logId(0)
{
    SoapySDR_logf(SOAPY_SDR_TRACE, "SoapyTCPRemote::<cons>(%s,%s,%s,%s)",
        address.c_str(), port.c_str(), remdriver.c_str(), remargs.c_str());
    int status = connectLogStream(detectLogLevel());
    if (status<0)
        throw std::runtime_error("unable to connect log stream");
    int sock = connect(true);
    if (sock<0)
        throw std::runtime_error("unable to connect to remote");
    rpc = new SoapyRPC(sock);
    status = loadRemoteDriver();
    if (status<0)
        throw std::runtime_error("unable to load remote driver");
}

SoapyTCPRemote::~SoapyTCPRemote()
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::<dest>");
    if (rpc) {
        rpc->writeString(TCPREMOTE_RPC_SEP);
        rpc->writeInteger(TCPREMOTE_DROP_RPC);
        delete rpc;
        rpc = nullptr;
    }
    if (log) {
        // writing to this stream terminates it via the remote end
        if (write(fileno(log), "\n", 1)<0)
            SoapySDR_logf(SOAPY_SDR_DEBUG, "SoapyTCPRemote::<dest>, log stream: %s", strerror(errno));
        logThread.join();
    }
}

std::string SoapyTCPRemote::typeLine(int type) const
{
    std::string line = std::to_string(type);
    if (!remoteKey.empty())
        line += ":" + remoteKey;
    return line;
}

// private connector method
int SoapyTCPRemote::connect(bool lowLatency) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::connect()");
    // resolve address (IPv4 or IPv6, name or literal)
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo *res = nullptr;
    int err = getaddrinfo(remoteAddress.c_str(), remotePort.c_str(), &hints, &res);
    if (err) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "Failed to resolve address/port: %s/%s: %s",
            remoteAddress.c_str(), remotePort.c_str(), gai_strerror(err));
        return -1;
    }
    // attempt connection to remote server, trying each address in turn
    int sock = -1;
    int lasterr = 0;
    for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
        sock = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (sock < 0) {
            lasterr = errno;
            continue;
        }
        if (::connect(sock, ai->ai_addr, ai->ai_addrlen)==0)
            break;
        lasterr = errno;
        close(sock);
        sock = -1;
    }
    freeaddrinfo(res);
    if (sock < 0) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "Failed to connect to address/port: %s/%s: %s",
            remoteAddress.c_str(), remotePort.c_str(), strerror(lasterr));
        return -1;
    }
    // small request/response traffic wants low latency, bulk data does not
    if (lowLatency)
        soapyNoDelay(sock);
    SoapySDR_logf(SOAPY_SDR_DEBUG, "SoapyTCPRemote: connected: %s/%s",
            remoteAddress.c_str(), remotePort.c_str());
    return sock;
}

int SoapyTCPRemote::loadRemoteDriver() const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::loadRemoteDriver()");
    // identify this connection as an RPC stream load the remote driver
    rpc->writeString(typeLine(TCPREMOTE_RPC_LOAD));
    rpc->writeString(remoteDriver);
    rpc->writeString(remoteArgs);
    return rpc->readInteger();
}

int SoapyTCPRemote::connectLogStream(SoapySDRLogLevel level)
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::connectLogStream");
    // first connect a new socket
    int sock = connect(true);
    if (sock<0)
        return sock;
    log = fdopen(sock, "r+");
    if (!log) {
        close(sock);
        SoapySDR_logf(SOAPY_SDR_ERROR, "fdopen() log stream: %s", strerror(errno));
        return -1;
    }
    setlinebuf(log);
    // identify this as the log stream, set level, obtain remote handle
    fprintf(log, "%s\n%d\n", typeLine(TCPREMOTE_LOG_STREAM).c_str(), (int)level);
    char ret[10];
    if (!fgets(ret, sizeof(ret), log)) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "fgets() log identifier: %s", strerror(errno));
        return -1;
    }
    if (sscanf(ret, "%d", &logId)!=1) {
        SoapySDR_log(SOAPY_SDR_ERROR, "sscanf() logId: not an integer");
        return -1;
    }
    logThread = std::thread(processLogStream, this);
    return 0;
}

void SoapyTCPRemote::processLogStream(SoapyTCPRemote *rem)
{
    char msg[256];
    while (fgets(msg, sizeof(msg), rem->log)) {
        // parse level from message, write to local handler
        SoapySDRLogLevel lev = SOAPY_SDR_ERROR;
        if (sscanf(msg, "%d:", (int*)&lev)>0) {
            msg[0]='r';
            msg[1]='<';
        } else {
            msg[0]='?';
            msg[1]='<';
        }
        // replace trailing newline
        msg[strlen(msg)-1]='>';
        SoapySDR_log(lev, msg);
    }
    fclose(rem->log);
}

// Identification API
std::string SoapyTCPRemote::getHardwareKey() const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getHardwareKey()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_HARDWARE_KEY);
    return rpc->readString();
}

SoapySDR::Kwargs SoapyTCPRemote::getHardwareInfo() const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getHardwareInfo()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_HARDWARE_INFO);
    return rpc->readKwargs();
}

// Channels API
void SoapyTCPRemote::setFrontendMapping(const int direction, const std::string &mapping)
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::setFrontendMapping()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_SET_FRONTEND_MAPPING);
    rpc->writeInteger(direction);
    rpc->writeString(mapping);
    rpc->readInteger(); // wait for completion!
}

std::string SoapyTCPRemote::getFrontendMapping(const int direction) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getFrontendMapping()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_FRONTEND_MAPPING);
    rpc->writeInteger(direction);
    return rpc->readString();
}

size_t SoapyTCPRemote::getNumChannels(const int dir) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getNumChannels()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_NUM_CHANNELS);
    rpc->writeInteger(dir);
    return rpc->readInteger();
}

SoapySDR::Kwargs SoapyTCPRemote::getChannelInfo(const int direction, const size_t channel) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getChannelInfo()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_CHANNEL_INFO);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    return rpc->readKwargs();
}

bool SoapyTCPRemote::getFullDuplex(const int direction, const size_t channel) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getFullDuplex()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_FULL_DUPLEX);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    return rpc->readInteger()>0;
}

std::vector<std::string> SoapyTCPRemote::getStreamFormats(const int direction, const size_t channel) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getStreamFormats()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_STREAM_FORMATS);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    return rpc->readStrVector();
}

std::string SoapyTCPRemote::getNativeStreamFormat(const int direction, const size_t channel, double &fullScale) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getNativeStreamFormat()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_STREAM_NATIVE_FORMAT);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    std::string fmt = rpc->readString();
    fullScale = rpc->readDouble();
   return fmt;
}

SoapySDR::ArgInfoList SoapyTCPRemote::getStreamArgsInfo(const int direction, const size_t channel) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getStreamArgsInfo()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_STREAM_ARGS_INFO);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    SoapySDR::ArgInfoList info;
    while (true) {
        // fugly complex structure to serialise... maybe later!
        std::string s = rpc->readString();
        if (s.length()==0)
            break;
    }
    SoapySDR_log(SOAPY_SDR_ERROR, "Unimplemented: getStreamArgsInfo");
    return info;
}

SoapySDR::Stream *SoapyTCPRemote::setupStream(const int direction, const std::string &format, const std::vector<size_t> &channels, const SoapySDR::Kwargs &args)
{
    SoapySDR_logf(SOAPY_SDR_TRACE, "SoapyTCPRemote::setupStream(%d,%s,%d,...)",
        direction, format.c_str(), channels.size());
    // if no channels were provided, assume channel 0
    std::vector<size_t> lchannels;
    if (channels.size()<1) {
        lchannels.push_back(0);
    } else {
        lchannels.insert(lchannels.begin(), channels.begin(), channels.end());
    }
    // grab the native format
    double fs;
    std::string fmtnat = getNativeStreamFormat(direction, lchannels[0], fs);
    // first check we have a frame size for both formats
    if (g_frameSizes.find(format)==g_frameSizes.end()) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "SoapyTCPRemote::setupStream, unknown requested format (%s)", format.c_str());
        return nullptr;
    }
    if (g_frameSizes.find(fmtnat)==g_frameSizes.end()) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "SoapyTCPRemote::setupStream, unknown native format (%s)", fmtnat.c_str());
        return nullptr;
    }
    // choose smallest wire format..
    if (g_frameSizes.at(fmtnat)<g_frameSizes.at(format)) {
        fmtwire = fmtnat;
        fmtout = format;
    } else {
        fmtwire = format;
        fmtout = format;
    }
    // in order to help the remote side associate the data stream with the setup call,
    // we create the data connection *first*, then send it's remoteId as the first
    // parameter to the RPC call..
    int data = connect(false);
    if (data<0) {
        SoapySDR_log(SOAPY_SDR_ERROR, "SoapyTCPRemote::setupStream, data stream failed to connect");
        return nullptr;
    }
    // sending one of TCPREMOTE_DATA_<x> makes this a data stream in the remote
    char dir[16];
    int dlen;
    std::string dline = typeLine(SOAPY_SDR_RX==direction? TCPREMOTE_DATA_SEND : TCPREMOTE_DATA_RECV) + "\n";
    if (write(data, dline.c_str(), dline.length())!=(ssize_t)dline.length()) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "SoapyTCPRemote::setupStream, failed to write data stream type: %s",
            strerror(errno));
        close(data);
        return nullptr;
    }
    dlen = read(data, dir, sizeof(dir) - 1);
    if (dlen<=0) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "SoapyTCPRemote::setupStream, failed to read data stream remoteId: %s",
            strerror(errno));
        close(data);
        return nullptr;
    }
    SoapySDR::Stream *rv = new SoapySDR::Stream();
    dir[dlen]=0;
    sscanf(dir, "%d", &rv->remoteId);
    rv->netSock = data;
    rv->fSize = g_frameSizes.at(fmtwire);
    rv->numChans = lchannels.size();
    rv->running = false;
    // make the RPC call with the remoteId
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_SETUP_STREAM);
    rpc->writeInteger(rv->remoteId);
    rpc->writeInteger(direction);
    rpc->writeString(fmtwire);
    // channel list, sent as a space separated list of numbers on one line
    std::string chans;
    for (auto it=lchannels.begin(); it!=lchannels.end(); ++it) {
        if (chans.length()>0)
            chans += " ";
        chans += std::to_string(*it);
    }
    rpc->writeString(chans);
    rpc->writeKwargs(args);
    int status = rpc->readInteger();
    if (status>=0) {
        SoapySDR_logf(SOAPY_SDR_TRACE,"SoapyTCPRemote::setupStream, data stream remoteId: %d", rv->remoteId);
    } else {
        SoapySDR_logf(SOAPY_SDR_ERROR, "SoapyTCPRemote::setupStream, error: %d", status);
        if (rv)
            closeStream(rv);
        rv = nullptr;
    }
    return rv;
}

void SoapyTCPRemote::closeStream(SoapySDR::Stream *stream)
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::closeStream()");
    if (stream->running)
        deactivateStream(stream);
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_CLOSE_STREAM);
    rpc->writeInteger(stream->remoteId);
    rpc->readInteger(); // ignore return value, but wait!
    close(stream->netSock);
    delete stream;
}

size_t SoapyTCPRemote::getStreamMTU(SoapySDR::Stream *stream) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getStreamMTU()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_STREAM_MTU);
    rpc->writeInteger(stream->remoteId);
    return rpc->readInteger();
}

int SoapyTCPRemote::activateStream(SoapySDR::Stream *stream,
                               const int flags,
                               const long long timeNs,
                               const size_t numElems)
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::activateStream()");
    if (stream->running)
        return 0;
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_ACTIVATE_STREAM);
    rpc->writeInteger(stream->remoteId);
    int status = rpc->readInteger();
    if (status==0)
        stream->running = true;
    return status;
}

int SoapyTCPRemote::deactivateStream(SoapySDR::Stream *stream, const int flags, const long long timeNs)
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::deactivateStream()");
    if (!stream->running)
        return 0;
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_DEACTIVATE_STREAM);
    rpc->writeInteger(stream->remoteId);
    int status = rpc->readInteger();
    if (status==0)
        stream->running = false;
    return status;
}

static void *convertCS16toCF32(void *dst, const void *src, size_t cnt /* ignored */)
{
    int16_t *cs16 = (int16_t *)src;
    float *cf32   = (float *)dst;
    // convert complex sample in range -INT16_MAX>INT16_MAX to -1.0>1.0
    int16_t si = cs16[0];
    int16_t sq = cs16[1];
    cf32[0]   = (float)si/(float)INT16_MAX;
    cf32[1] = (float)sq/(float)INT16_MAX;
    return dst;
}

int SoapyTCPRemote::readStream(SoapySDR::Stream *stream,
                           void * const *buffs,
                           const size_t numElems,
                           int &flags,
                           long long &timeNs,
                           const long timeoutUs)
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::readStream()");
    // Not running? timeout (says the docs)
    if (!stream->running)
        return SOAPY_SDR_TIMEOUT;
    // Transfer format on the wire is interleaved sample frames (each fSize) across channels.
    // TCP gives us a byte stream, so a read may end part way through a frame: we keep
    // the partial tail for the next call. Then we de-interleave and possibly convert
    // formats into buffs.
    const size_t blkSize = stream->fSize * stream->numChans;
    if (numElems==0 || blkSize==0)
        return 0;
    const size_t want = blkSize * numElems;
    if (stream->rxbuf.size() < want)
        stream->rxbuf.resize(want);
    uint8_t *swamp = stream->rxbuf.data();
    // wait (up to the timeout) until we hold at least one whole frame
    while (stream->rxhave < blkSize) {
        struct pollfd pfd = { stream->netSock, POLLIN, 0 };
        int pr = poll(&pfd, 1, timeoutUs>0? (int)((timeoutUs+999)/1000): 0);
        if (pr==0)
            return SOAPY_SDR_TIMEOUT;
        if (pr<0) {
            if (errno==EINTR)
                continue;
            SoapySDR_logf(SOAPY_SDR_ERROR, "SoapyTCPRemote::readStream, error waiting for data: %s", strerror(errno));
            return SOAPY_SDR_STREAM_ERROR;
        }
        ssize_t n = read(stream->netSock, swamp + stream->rxhave, want - stream->rxhave);
        if (n==0) {
            SoapySDR_log(SOAPY_SDR_ERROR, "SoapyTCPRemote::readStream, remote closed the data connection");
            return SOAPY_SDR_STREAM_ERROR;
        }
        if (n<0) {
            if (errno==EINTR || errno==EAGAIN)
                continue;
            SoapySDR_logf(SOAPY_SDR_ERROR, "SoapyTCPRemote::readStream, error reading data: %s", strerror(errno));
            return SOAPY_SDR_STREAM_ERROR;
        }
        stream->rxhave += n;
    }
    const size_t elems = stream->rxhave / blkSize;
    size_t soff=0;
    size_t boff=0;
    size_t bSize=stream->fSize;
    void *(*cnv)(void *, const void *, size_t) = memcpy;
    if (fmtout!=fmtwire) {
        bSize = g_frameSizes.at(fmtout);
        // TODO: actually check these formats =)
        cnv = convertCS16toCF32;
    }
    for (size_t e=0; e<elems; ++e) {
        for (int c=0; c<stream->numChans; ++c) {
            uint8_t *buf = (uint8_t *)(buffs[c]);
            cnv(buf+boff, swamp+soff, stream->fSize);
            soff += stream->fSize;
        }
        boff += bSize;
    }
    // keep any partial frame for next time
    stream->rxhave -= elems * blkSize;
    if (stream->rxhave)
        memmove(swamp, swamp + elems * blkSize, stream->rxhave);
    return (int)elems;
}

int SoapyTCPRemote::writeStream(SoapySDR::Stream *stream,
                    const void * const *buffs,
                    const size_t numElems,
                    int &flags,
                    const long long timeNs,
                    const long timeoutUs)
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::writeStream()");
    // Not running? timeout (says the docs)
    if (!stream->running)
        return SOAPY_SDR_TIMEOUT;
    // assemble all elements as interleaved frames, then write the lot with as
    // few syscalls as possible (TCP back pressure blocks us if the server is slow)
    const size_t blkSize = stream->fSize * stream->numChans;
    const size_t total = blkSize * numElems;
    if (total==0)
        return 0;
    if (stream->txbuf.size() < total)
        stream->txbuf.resize(total);
    uint8_t *out = stream->txbuf.data();
    size_t noff=0;
    for (size_t e=0; e<numElems; ++e) {
        for (int c=0; c<stream->numChans; ++c) {
            memcpy(out+noff, (const uint8_t *)buffs[c] + e*stream->fSize, stream->fSize);
            noff += stream->fSize;
        }
    }
    size_t done = 0;
    while (done < total) {
        // MSG_NOSIGNAL: a vanished server must not raise SIGPIPE in the host application
        ssize_t n = send(stream->netSock, out+done, total-done, MSG_NOSIGNAL);
        if (n<0) {
            if (errno==EINTR)
                continue;
            SoapySDR_logf(SOAPY_SDR_ERROR, "SoapyTCPRemote::writeStream, error writing data: %s", strerror(errno));
            break;
        }
        done += n;
    }
    return (int)(done / blkSize);
}

int SoapyTCPRemote::readStreamStatus(
                    SoapySDR::Stream *stream,
                    size_t &chanMask,
                    int &flags,
                    long long &timeNs,
                    const long timeoutUs)
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::readStreamStatus()");
    // TODO: report something?
    return SOAPY_SDR_NOT_SUPPORTED;
}

bool SoapyTCPRemote::hasFrequencyCorrection(const int direction, const size_t channel) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::hasFrequencyCorrection()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_HAS_FREQUENCY_CORRECTION);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    return rpc->readInteger()!=0;
}

void SoapyTCPRemote::setFrequencyCorrection(const int direction, const size_t channel, double value)
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::setFrequencyCorrection()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_SET_FREQUENCY_CORRECTION);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    rpc->writeDouble(value);
    rpc->readInteger(); // wait for completion!
}

double SoapyTCPRemote::getFrequencyCorrection(const int direction, const size_t channel) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getFrequencyCorrection()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_FREQUENCY_CORRECTION);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    return rpc->readDouble();
}

std::vector<std::string> SoapyTCPRemote::listGains(const int direction, const size_t channel) const
{
    //list available gain element names,
    //the functions below have a "name" parameter
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::listGains()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_LIST_GAINS);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    return rpc->readStrVector();
}

bool SoapyTCPRemote::hasGainMode(const int direction, const size_t channel) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::hasGainMode()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_HAS_GAIN_MODE);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    return rpc->readInteger()>0;
}

void SoapyTCPRemote::setGainMode(const int direction, const size_t channel, const bool automatic)
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::setGainMode()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_SET_GAIN_MODE);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    rpc->writeInteger(automatic?1:0);
    rpc->readInteger(); // wait for completion!
}

bool SoapyTCPRemote::getGainMode(const int direction, const size_t channel) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getGainMode()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_GAIN_MODE);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    return rpc->readInteger()>0;
}

void SoapyTCPRemote::setGain(const int direction, const size_t channel, const double value)
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::setGain()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_SET_GAIN);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    rpc->writeDouble(value);
    rpc->readInteger(); // wait for completion!
}

void SoapyTCPRemote::setGain(const int direction, const size_t channel, const std::string &name, const double value)
{
    SoapySDR_logf(SOAPY_SDR_TRACE, "SoapyTCPRemote::setGain(%s)", name.c_str());
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_SET_GAIN_NAMED);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    rpc->writeString(name);
    rpc->writeDouble(value);
    rpc->readInteger(); // wait for completion!
}

double SoapyTCPRemote::getGain(const int direction, const size_t channel) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getGain()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_GAIN);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    return rpc->readDouble();
}

double SoapyTCPRemote::getGain(const int direction, const size_t channel, const std::string &name) const
{
    SoapySDR_logf(SOAPY_SDR_TRACE, "SoapyTCPRemote::getGain(%s)", name.c_str());
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_GAIN_NAMED);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    rpc->writeString(name);
    return rpc->readDouble();
}

SoapySDR::Range SoapyTCPRemote::getGainRange(const int direction, const size_t channel) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getGainRange()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_GAIN_RANGE);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    return SoapySDR::Range(
        rpc->readDouble(),
        rpc->readDouble(),
        rpc->readDouble()
    );
}

SoapySDR::Range SoapyTCPRemote::getGainRange(const int direction, const size_t channel, const std::string &name) const
{
    SoapySDR_logf(SOAPY_SDR_TRACE, "SoapyTCPRemote::getGainRange(%s)", name.c_str());
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_GAIN_RANGE_NAMED);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    rpc->writeString(name);
    return SoapySDR::Range(
        rpc->readDouble(),
        rpc->readDouble(),
        rpc->readDouble()
    );
}

// Frequency
void SoapyTCPRemote::setFrequency(const int direction,
                              const size_t channel,
                              const double frequency,
                              const SoapySDR::Kwargs &args)
{
    SoapySDR_logf(SOAPY_SDR_TRACE, "SoapyTCPRemote::setFrequency(%f)", frequency);
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_SET_FREQUENCY);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    rpc->writeDouble(frequency);
    rpc->writeKwargs(args);
    rpc->readInteger(); // wait for completion!
}

void SoapyTCPRemote::setFrequency(const int direction,
                              const size_t channel,
                              const std::string &name,
                              const double frequency,
                              const SoapySDR::Kwargs &args)
{
    SoapySDR_logf(SOAPY_SDR_TRACE, "SoapyTCPRemote::setFrequency(%s,%f)", name.c_str(), frequency);
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_SET_FREQUENCY_NAMED);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    rpc->writeString(name);
    rpc->writeDouble(frequency);
    rpc->writeKwargs(args);
    rpc->readInteger(); // wait for completion!
}

double SoapyTCPRemote::getFrequency(const int direction, const size_t channel) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getFrequency()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_FREQUENCY);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    return rpc->readDouble();
}

double SoapyTCPRemote::getFrequency(const int direction, const size_t channel, const std::string &name) const
{
    SoapySDR_logf(SOAPY_SDR_TRACE, "SoapyTCPRemote::getFrequency(%s)", name.c_str());
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_FREQUENCY_NAMED);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    rpc->writeString(name);
    return rpc->readDouble();
}

std::vector<std::string> SoapyTCPRemote::listFrequencies(const int direction, const size_t channel) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::listFrequencies()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_LIST_FREQUENCIES);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    return rpc->readStrVector();
}
SoapySDR::RangeList SoapyTCPRemote::getFrequencyRange(const int direction, const size_t channel) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getFrequencyRange()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_FREQUENCY_RANGE);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    // read triplets of beg/end/step until step<0
    SoapySDR::RangeList list;
    while (true) {
        double beg = rpc->readDouble();
        double end = rpc->readDouble();
        double step = rpc->readDouble();
        if (step<0)
            break;
        list.push_back(SoapySDR::Range(beg,end,step));
    }
    return list;
}


SoapySDR::RangeList SoapyTCPRemote::getFrequencyRange(const int direction, const size_t channel, const std::string &name) const
{
    SoapySDR_logf(SOAPY_SDR_TRACE, "SoapyTCPRemote::getFrequencyRange(%s)", name.c_str());
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_FREQUENCY_RANGE_NAMED);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    rpc->writeString(name);
    // read triplets of beg/end/step until step<0
    SoapySDR::RangeList list;
    while (true) {
        double beg = rpc->readDouble();
        double end = rpc->readDouble();
        double step = rpc->readDouble();
        if (step<0)
            break;
        list.push_back(SoapySDR::Range(beg,end,step));
    }
    return list;
}

SoapySDR::ArgInfoList SoapyTCPRemote::getFrequencyArgsInfo(const int direction, const size_t channel) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getFrequencyArgsInfo()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_FREQUENCY_ARGS_INFO);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    // TODO: parse complex structure!
    SoapySDR::ArgInfoList list;
    while (true) {
        // fugly complex structure to serialise... maybe later!
        std::string s = rpc->readString();
        if (s.length()==0)
            break;
    }
    SoapySDR_log(SOAPY_SDR_ERROR, "Unimplemented: getFrequencyArgsInfo");
    return list;
}

void SoapyTCPRemote::setSampleRate(const int direction, const size_t channel, const double rate)
{
    SoapySDR_logf(SOAPY_SDR_TRACE, "SoapyTCPRemote::setSampleRate(%f)", rate);
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_SET_SAMPLE_RATE);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    rpc->writeDouble(rate);
    rpc->readInteger(); // wait for completion!
}

double SoapyTCPRemote::getSampleRate(const int direction, const size_t channel) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getSampleRate()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_SAMPLE_RATE);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    return rpc->readDouble();
}

std::vector<double> SoapyTCPRemote::listSampleRates(const int direction, const size_t channel) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::listSampleRates()");
    // emulate through current API
    SoapySDR::RangeList list = getSampleRateRange(direction, channel);
    std::vector<double> rates;
    for (auto r: list) {
        for (double f=r.minimum(); f<=r.maximum(); f+=r.step())
            rates.push_back(f);
    }
    return rates;
}

SoapySDR::RangeList SoapyTCPRemote::getSampleRateRange(const int direction, const size_t channel) const
{
    SoapySDR_log(SOAPY_SDR_TRACE, "SoapyTCPRemote::getSampleRateRange()");
    rpc->writeString(TCPREMOTE_RPC_SEP);
    rpc->writeInteger(TCPREMOTE_GET_SAMPLE_RATE_RANGE);
    rpc->writeInteger(direction);
    rpc->writeInteger(channel);
    SoapySDR::RangeList list;
    // read triplets of beg/end/step until step<0
    while (true) {
        double beg = rpc->readDouble();
        double end = rpc->readDouble();
        double step = rpc->readDouble();
        if (step<0)
            break;
        list.push_back(SoapySDR::Range(beg,end,step));
    }
    return list;
}

std::string getConfFile() {
    // We support a configuration file in one of:
    // [$XDG_CONFIG_DIRS]/SoapyTCPRemote.conf, $HOME/.config/SoapyTCPRemote.conf
    SoapySDR_log(SOAPY_SDR_TRACE, "getConfFile");
    std::string tst="";
    if (getenv("XDG_CONFIG_DIRS")!=nullptr) {
        std::string env = getenv("XDG_CONFIG_DIRS");
        size_t pos, nxt=-1;
        do {
            pos = nxt+1;
            nxt = env.find(':', pos);
            tst = env.substr(pos,nxt-pos);
            tst += "/SoapyTCPRemote.conf";
            if (access(tst.c_str(), R_OK)==0)
                goto done;
        } while (nxt!=std::string::npos);
    }
    tst = "/etc/xdg/SoapyTCPRemote.conf";
    if (access(tst.c_str(), R_OK)==0)
        goto done;
    if (getenv("HOME")!=nullptr) {
        tst = getenv("HOME");
        tst += "/.config/SoapyTCPRemote.conf";
        if (access(tst.c_str(), R_OK)==0)
            goto done;
    }
    tst = "";
done:
    SoapySDR_logf(SOAPY_SDR_TRACE, "SoapySDRRemote::getConfFile()=%s", tst.c_str());
    return tst;
}

std::string getConfValue(const std::string &key) {
    SoapySDR_logf(SOAPY_SDR_TRACE, "getConfValue(%s)", key.c_str());
    std::string rv = "";
    std::string cf = getConfFile();
    FILE *fp = nullptr;
    if (cf.length()>0 && (fp=fopen(cf.c_str(), "r"))!=nullptr) {
        char line[512];
        while (fgets(line, sizeof(line), fp)) {
            std::string arg = line;
            while (!arg.empty() && (arg.back()=='\n' || arg.back()=='\r'))
                arg.pop_back();
            if (arg.empty() || arg[0]=='#')
                continue;
            // exact "name=value" match on the name (previously any line containing it)
            size_t off = arg.find('=');
            if (std::string::npos==off || arg.substr(0, off)!=key)
                continue;
            rv = arg.substr(off+1);
            break;
        }
        fclose(fp);
    }
    return rv;
}

// NB: this is called with random (can be NULL) parameters when enumeration is done by an app,
// and with *merged* parameters (app & enumeration response) when a device is created by Device::make().
SoapySDR::KwargsList findTCPRemote(const SoapySDR::Kwargs &args)
{
    SoapySDR_log(SOAPY_SDR_TRACE, "findTCPRemote");

    SoapySDR::KwargsList results;
    
    // We MUST have:
    // tcpremote:address - server IP:Port
    // tcpremote:driver  - driver to load at the remote end
    // We MAY have:
    // tcpremote:args    - driver arguments when creating
    // We ensure mandatory args are present, and parse them
    std::string address = getConfValue("address");
    if (args.find("tcpremote:address")==args.end()) {
        SoapySDR_logf(SOAPY_SDR_DEBUG, "Missing tcpremote:address, using: %s", address.c_str());
    } else {
         address = args.at("tcpremote:address");
    }
    std::string driver = getConfValue("driver");
    if (args.find("tcpremote:driver")==args.end()) {
        SoapySDR_logf(SOAPY_SDR_DEBUG, "Missing tcpremote:driver, using: %s", driver.c_str());
    } else {
        driver = args.at("tcpremote:driver");
    }
    // no port, default to 0x50AF (20655)
    int port = 0x50AF;
    if (!address.empty() && address[0]=='[') {
        // [ipv6]:port or [ipv6]
        size_t rb = address.find(']');
        if (rb != std::string::npos) {
            if (rb+1 < address.length() && address[rb+1]==':')
                port = atoi(address.substr(rb+2).c_str());
            address = address.substr(1, rb-1);
        }
    } else if (std::count(address.begin(), address.end(), ':')==1) {
        // host:port (more than one colon is a bare IPv6 address, which has no port)
        size_t colon = address.find(':');
        port = atoi(address.substr(colon+1).c_str());
        address = address.substr(0, colon);
    }
    SoapySDR_logf(SOAPY_SDR_TRACE, "findTCPRemote parsed: address=%s port=%u", address.c_str(), port);
    SoapySDR::Kwargs soapyInfo;
    // This is the name that shows up.
    soapyInfo["device"] = "TCP remote: "+address;
    soapyInfo["address"] = address;
    soapyInfo["port"] = std::to_string(port);
    soapyInfo["tcpremote:driver"] = driver;
    if (args.find("tcpremote:args")!=args.end())
        soapyInfo["tcpremote:args"] = args.at("tcpremote:args");
    else
        soapyInfo["tcpremote:args"] = getConfValue("args");
    results.push_back(soapyInfo);
    return results;
}

SoapySDR::Device *makeTCPRemote(const SoapySDR::Kwargs &args)
{
    SoapySDR_log(SOAPY_SDR_INFO, "makeTCPRemote");
    
    std::string address = args.at("address");
    std::string port = args.at("port");
    std::string remdriver = args.at("tcpremote:driver");
    std::string remargs = args.at("tcpremote:args");
    // shared secret: device args, then environment, then config file. Deliberately not
    // copied into the enumeration results, so it never shows up in --probe output or logs.
    std::string remkey;
    if (args.find("tcpremote:key")!=args.end())
        remkey = args.at("tcpremote:key");
    else if (getenv("SOAPY_TCPREMOTE_KEY"))
        remkey = getenv("SOAPY_TCPREMOTE_KEY");
    else
        remkey = getConfValue("key");
    return (SoapySDR::Device*) new SoapyTCPRemote(address, port, remdriver, remargs, remkey);
}

/* Register this driver */
static SoapySDR::Registry registerTCPRemote("tcpremote", &findTCPRemote, &makeTCPRemote, SOAPY_SDR_ABI_VERSION);
