//  SoapyTCPserver.cpp
//  Copyright (c) 2021 Phil Ashby
//  SPDX-License-Identifier: BSL-1.0

// Design approach is KISS, main thread accepts connections into a
// map, handles RPCs. Log connections exist separately (allowing
// custom network loggers).
// Worker threads are created per data stream to pump in/out.
#include <SoapySDR/Device.hpp>
#include "SoapyRPC.hpp"
#include "SoapyLog.hpp"
#include <signal.h>
#include <pthread.h>
#include <unistd.h>
#include <stdio.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netdb.h>
#include <unordered_set>
#include <vector>
#include <map>
#include <string>
#include <algorithm>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

// Runtime configuration (see usage())
static std::string s_key;         // shared secret, empty = no authentication
static int s_bufferMs = 0;        // target network buffering in ms, 0 = legacy 10 x MTU
static const int HANDSHAKE_TIMEOUT_S = 5;   // time allowed to identify a new connection
static const int SOCKET_RCVTIMEO_S = 10;    // max time a peer may stall mid-request
static const int ORPHAN_DATA_TIMEOUT_S = 30; // data connections never bound to a stream
static const size_t MAX_PENDING = 64;

struct pipebuf_t {
    void *buf;
    int len, in, out;
    pthread_mutex_t mutex;
    pthread_cond_t rd, wr;
};

struct ConnectionInfo
{
// default constructor clears all values
    ConnectionInfo(): rpc(nullptr), dev(nullptr), netSock(0), netPipe(nullptr), direction(0), stream(nullptr), pid(0), created(0), log(nullptr), level(SOAPY_SDR_INFO) {}
// RPC connection bits
    // NB: existance of an rpc object implies this is an RPC connection, otherwise data stream
    SoapyRPC *rpc;
    // our underlying real device
    SoapySDR::Device *dev;
    // a set of data connections / streams for this device
    std::unordered_set<int> dataIds;
// data connection bits
    // the raw socket
    int netSock;
    // our memory buffer & inter-thread storage
    pipebuf_t *netPipe;
    // which way are we going
    int direction;
    // selected stream format
    std::string format;
    // selected channels
    std::vector<size_t> channels;
    // our underlying device stream
    SoapySDR::Stream *stream;
    // thread ID (for data pump)
    volatile pthread_t pid;
    // creation time, used to expire data connections that are never set up
    time_t created;
// log stream bits
    FILE *log;
    SoapySDRLogLevel level;
};

pipebuf_t *newpipe(int size) {
    pipebuf_t *pipe = (pipebuf_t *)malloc(sizeof(pipebuf_t)+size);
    if (!pipe)
        return nullptr;
    pipe->buf = pipe+1;
    pipe->len = size;
    pipe->in = pipe->out = 0;
    pthread_mutex_init(&pipe->mutex, NULL);
    pthread_cond_init(&pipe->rd, NULL);
    pthread_cond_init(&pipe->wr, NULL);
    return pipe;
}

void freepipe(pipebuf_t *pipe) {
    if (!pipe)
        return;
    pthread_mutex_destroy(&pipe->mutex);
    pthread_cond_destroy(&pipe->rd);
    pthread_cond_destroy(&pipe->wr);
    free(pipe);
}

// bulk copies into/out of the ring buffer (at most two memcpy each)
static void pipecopyin(pipebuf_t *pipe, const char *src, int n) {
    int first = std::min(n, pipe->len - pipe->in);
    memcpy((char *)pipe->buf + pipe->in, src, first);
    if (n > first)
        memcpy(pipe->buf, src + first, n - first);
    pipe->in = (pipe->in + n) % pipe->len;
}

static void pipecopyout(pipebuf_t *pipe, char *dst, int n) {
    int first = std::min(n, pipe->len - pipe->out);
    memcpy(dst, (char *)pipe->buf + pipe->out, first);
    if (n > first)
        memcpy(dst + first, pipe->buf, n - first);
    pipe->out = (pipe->out + n) % pipe->len;
}

// blocking/failing write, allows thread switching
int pipewrite(void *src, int sz, int num, pipebuf_t *pipe, bool block = true) {
    int rv = -1;
    // args check
    if (!src || !sz || !num || !pipe)
        return -1;
    // gain exclusive access to the pipe..
    pthread_mutex_lock(&pipe->mutex);
    // wait for space..
    int av=0;
    do {
        // calculate pipe space used & available..
        int us = pipe->in-pipe->out;
        if (us<0) us += pipe->len;
        av = pipe->len-us-1;
        if (av<sz) {
            // block or fail?
            if (block)
                pthread_cond_wait(&pipe->rd, &pipe->mutex);
            else {
                pthread_mutex_unlock(&pipe->mutex);
                return rv;
            }
        }
    } while (av<sz);
    // calculate how many items of sz will fit (up to num)
    int ft = av/sz;
    if (ft>num) ft=num;
    // move those bytes!
    pipecopyin(pipe, (const char *)src, ft*sz);
    // assign return value = number of items written
    rv = ft;
    // release access under all exit conditions
    pthread_mutex_unlock(&pipe->mutex);
    // signal a write has occurred
    pthread_cond_signal(&pipe->wr);
    return rv;
}

// blocking/failing read, allows thread switching
int piperead(void *dst, int sz, int num, pipebuf_t *pipe, bool block = true) {
    int rv = -1;
    // args check
    if (!dst || !sz || !num || !pipe)
        return -1;
    // gain exclusive access to the pipe..
    pthread_mutex_lock(&pipe->mutex);
    // wait for data..
    int us=0;
    do {
        // calculate pipe space used
        us = pipe->in-pipe->out;
        if (us<0) us += pipe->len;
        if (us<sz) {
            if (block)
                pthread_cond_wait(&pipe->wr, &pipe->mutex);
            else {
                pthread_mutex_unlock(&pipe->mutex);
                return 0;
            }
        }
    } while (us<sz);
    // calculate how many items of sz are in the pipe (up to num)
    int nm = us/sz;
    if (nm>num) nm=num;
    // move those bytes!
    pipecopyout(pipe, (char *)dst, nm*sz);
    // assign return value = number of items read
    rv = nm;
    // release access under all exit conditions
    pthread_mutex_unlock(&pipe->mutex);
    // signal that a read has occurred
    pthread_cond_signal(&pipe->rd);
    return rv;
}

static std::map<int, ConnectionInfo> s_connections;

// bound how long a peer can stall us part way through a request
static void setSockTimeout(int sock, int secs) {
    struct timeval tv;
    tv.tv_sec = secs;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

int createRpc(int sock) {
    SoapySDR_log(SOAPY_SDR_DEBUG, "createRpc()");
    ConnectionInfo conn;
    soapyNoDelay(sock);
    setSockTimeout(sock, SOCKET_RCVTIMEO_S);
    conn.rpc = new SoapyRPC(sock);
    conn.log = nullptr;     // ensure we aren't treated as LOG stream
    // read driver and args..
    SoapySDR::Kwargs kwargs;
    std::string drv = conn.rpc->readString();
    std::string fix = drv;
    // check for upper-case in driver name, fix and warn
    bool warn = false;
    for (auto &c : fix) {
        if (isupper(c)) {
            warn = true;
            c = tolower(c);
        }
    }
    if (warn)
        SoapySDR_logf(SOAPY_SDR_WARNING,"driver name forced to lower case: %s -> %s",drv.c_str(), fix.c_str());
    kwargs["driver"] = fix;
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
    try {
        conn.dev = SoapySDR::Device::make(kwargs);
    } catch(const std::exception &ex) {
        conn.dev = nullptr;
        SoapySDR_logf(SOAPY_SDR_ERROR,"exception from Device::make(): %s", ex.what());
    }
    if (!conn.dev) {
        // oops - report failure to client and drop connection
        SoapySDR_logf(SOAPY_SDR_ERROR,"failed to create SoapySDR::Device: %s", kwargs["driver"].c_str());
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
    conn.log = nullptr;     // ensure we aren't treated as LOG stream
    conn.netSock = sock;
    conn.created = time(nullptr);
    // all good - add to map and respond with map key
    // NB: we write to raw socket as stdio stream may be read-only..
    char id[16];
    int ilen = snprintf(id, sizeof(id), "%d\n", sock);
    if (write(sock, id, ilen)!=ilen) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "createData: failed to send id: %s", strerror(errno));
        close(sock);
        return -1;
    }
    s_connections[sock] = conn;
    SoapySDR_logf(SOAPY_SDR_INFO, "New data connection: %d", sock);
    return 0;
}

static SoapySDRLogLevel s_defaultLogLevel;

int createLog(int sock) {
    SoapySDR_log(SOAPY_SDR_DEBUG, "createLog()");
    ConnectionInfo conn;
    conn.rpc = nullptr;     // ensure we aren't treated as RPC stream
    conn.netSock = sock;
    soapyNoDelay(sock);
    setSockTimeout(sock, SOCKET_RCVTIMEO_S);
    conn.log = fdopen(sock, "r+");
    if (!conn.log) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "createLog: fdopen: %s", strerror(errno));
        close(sock);
        return -1;
    }
    setlinebuf(conn.log);
    // read log level from client
    int level = (SoapySDRLogLevel)SOAPY_SDR_INFO;
    char buf[16];
    if(!fgets(buf, sizeof(buf), conn.log)) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "fgets() on log stream: %s", strerror(errno));
        fclose(conn.log);
        return -1;
    }
    sscanf(buf, "%d", (int*)&level);
    conn.level = (SoapySDRLogLevel)level;
    s_connections[sock] = conn;
    fprintf(conn.log, "%d\n", sock);    // write our id (map key)
    SoapySDR_logf(SOAPY_SDR_INFO, "New log connection: %d @ %d", sock, level);
    return 0;
}

// uSec difference between timespec samples
long tsdiff(struct timespec *t1, struct timespec *t2) {
    long r = (t2->tv_sec-t1->tv_sec)*1000000;
    r += (t2->tv_nsec-t1->tv_nsec)/1000;
    return r;
}
// Size of the inter-thread pipe: by default 10 x the read size (copes with TCP
// jitter), or enough for -b <ms> of samples at the current sample rate.
static size_t pipeSizeFor(ConnectionInfo *conn, size_t frameBytes, size_t readSize) {
    size_t sz = readSize * 10;
    if (s_bufferMs > 0 && !conn->channels.empty()) {
        double rate = conn->dev->getSampleRate(conn->direction, conn->channels.at(0));
        if (rate > 0) {
            size_t want = (size_t)(rate * s_bufferMs / 1000.0) * frameBytes;
            sz = std::max(want, readSize * 2);
        }
    }
    return sz;
}

void *netPump(void *ctx) {
    ConnectionInfo *conn = (ConnectionInfo *)ctx;
    // you had 1 job... read that pipe and stuff down network
    size_t elemSize = g_frameSizes.at(conn->format)*conn->channels.size();
    size_t numElems = BUFSIZ/elemSize;
    uint8_t wrbuf[numElems*elemSize];
    int nrd;
    SoapySDR_logf(SOAPY_SDR_DEBUG, "netPump: start: %d", conn->netSock);
    struct timespec lt;
    clock_gettime(CLOCK_MONOTONIC, &lt);
    // ignore SIGPIPE, so we get EPIPE returned
    signal(SIGPIPE, SIG_IGN);
    while ((nrd=piperead(wrbuf, elemSize, numElems, conn->netPipe))>0 && conn->pid!=0) {
        if (nullptr==getenv("INHIBIT_WRITE") &&
            write(conn->netSock, wrbuf, elemSize*nrd)!=(int)elemSize*nrd) {
            SoapySDR_logf(SOAPY_SDR_ERROR, "netPump: unable to write to network: %s", strerror(errno));
            break;
        }
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        SoapySDR_logf(SOAPY_SDR_TRACE, "%ld: netPump: write: %d<=%d",
            tsdiff(&lt, &ts), conn->netSock, nrd*elemSize);
        lt = ts;
    }
    SoapySDR_logf(SOAPY_SDR_DEBUG, "netPump: stop: %d", conn->netSock);
    return nullptr;
}

void *dataPump(void *ctx) {
    ConnectionInfo *conn = (ConnectionInfo *)ctx;
    SoapySDR_logf(SOAPY_SDR_DEBUG, "dataPump: start: %d", conn->netSock);
    // first - activate the underlying stream
    if (conn->dev->activateStream(conn->stream)) {
        SoapySDR_log(SOAPY_SDR_ERROR, "dataPump: failed to activate underlying stream");
        return nullptr;
    }
    // special case: one channel, in native format, with direct buffers supported - we can avoid lots of work
    double full;
    if (1==conn->channels.size()
        && conn->dev->getNativeStreamFormat(conn->direction, conn->channels.at(0), full)==conn->format
        && conn->dev->getNumDirectAccessBuffers(conn->stream) > 0) {
        SoapySDR_log(SOAPY_SDR_DEBUG, "dataPump: using direct buffers");
        if (SOAPY_SDR_RX!=conn->direction) {
            SoapySDR_log(SOAPY_SDR_ERROR, "dataPump: transmit not supported - sorry :=(");
            conn->dev->deactivateStream(conn->stream);
            return nullptr;
        }
        // make the network output pipe (10x MTU for jitter buffering)
        size_t fSize = g_frameSizes.at(conn->format);
        size_t mtu = conn->dev->getStreamMTU(conn->stream);
        size_t pipeSize = pipeSizeFor(conn, fSize, mtu * fSize);
        conn->netPipe = newpipe(pipeSize);
        // start network pump, unless asked to use direct write
        bool bDirect = nullptr!=getenv("SOAPY_TCPREMOTE_DIRECT_WRITE");
        pthread_t fpid;
        if (!bDirect) {
            pthread_create(&fpid, nullptr, netPump, conn);
        }
        while (conn->pid!=0) {
            // map a buffer, copy to pipe, repeat => simples :)
            size_t handle;
            const void *pBuf;
            int flags = 0;
            long long timeNs;
            long timeoutUs = 1000000;
            int err = conn->dev->acquireReadBuffer(conn->stream, handle, &pBuf, flags, timeNs, timeoutUs);
            if (err<0) {
                // non-fatal overflow, retry
                if (SOAPY_SDR_OVERFLOW==err) {
                    SoapySDR_log(SOAPY_SDR_WARNING, "dataPump: overrun direct buffer, data loss");
                    continue;
                }
                SoapySDR_logf(SOAPY_SDR_ERROR, "dataPump: error mapping direct buffer: %s", SoapySDR_errToStr(err));
                break;
            }
            if (bDirect) {
                if (send(conn->netSock, pBuf, err*fSize, MSG_DONTWAIT)!=(int)(err*fSize)) {
                    SoapySDR_logf(SOAPY_SDR_WARNING, "dataPump: direct write error: %s", strerror(errno));
                }
            } else {
                if (pipewrite((void *)pBuf, fSize, err, conn->netPipe, false)<0) {
                    SoapySDR_log(SOAPY_SDR_WARNING, "dataPump: overrun network pipe, data loss");
                }
            }
            conn->dev->releaseReadBuffer(conn->stream, handle);
        }
        if (!bDirect) {
            // final write to ensure netPump wakes up and terminates
            pipewrite((void *)".", 1, 1, conn->netPipe, false);
            pthread_join(fpid, nullptr);
        }
        freepipe(conn->netPipe);
        conn->netPipe = nullptr;
        // stop the byte flood :=)
        conn->dev->deactivateStream(conn->stream);
        SoapySDR_logf(SOAPY_SDR_DEBUG, "dataPump: stop: %d", conn->netSock);
        return nullptr;
    }
    // which direction?
    if (SOAPY_SDR_RX==conn->direction) {
        // use maximum number of elements/samples per read supported by the underlying driver
        size_t numElems = conn->dev->getStreamMTU(conn->stream);
        size_t fSize = g_frameSizes.at(conn->format);
        size_t numChans = conn->channels.size();
        size_t elemSize = fSize * numChans;
        size_t chnSize = numElems * fSize;
        size_t readSize = chnSize * numChans;
        // inter-thread pipe large enough to hold 10xMTU, should cope with TCP jitter
        size_t pipeSize = pipeSizeFor(conn, elemSize, readSize);
        // allocate buffers & pointers to them
        void *buffs[numChans];
        uint8_t cbuf[readSize];
        uint8_t pbuf[readSize];
        conn->netPipe = newpipe(pipeSize);
        for (size_t c=0; c<numChans; ++c)
            buffs[c] = cbuf+(c*chnSize);
        SoapySDR_logf(SOAPY_SDR_TRACE, "dataPump: numElems=%d", numElems);
        // start network pump
        pthread_t fpid;
        pthread_create(&fpid, nullptr, netPump, conn);
        // pump until told to stop!
        struct timespec lt;
        clock_gettime(CLOCK_MONOTONIC, &lt);
        while (conn->pid!=0) {
            int flags = 0;
            long long time = 0;
            long timeout = 1000000; // 1 second
            int nread = conn->dev->readStream(conn->stream, buffs, numElems, flags, time, timeout);
            if (nread<0) {
                SoapySDR_logf(SOAPY_SDR_ERROR,
                    "dataPump: error reading underlying stream: %s", SoapySDR_errToStr(nread));
                // non-fatal overflow
                if (nread==SOAPY_SDR_OVERFLOW)
                    continue;
                break;
            }
            // interleave samples across channels for network format:
            // Soapy readStream (channelized) format:
            //            <--------- nread -------//--->
            //            +-----------------//---+  -+    ^
            // buffs[0]-> | channel 0 samples..  |   |    |
            //            +-----------------//---+   |    |
            // buffs[1]-> | channel 1 samples..  |   |    |
            //            +-----------------//---+ cbuf   |
            // ...        | ...                  |   |  numChans
            //            +-----------------//---+   |    |
            // buffs[n]-> | channel n samples..  |   |    |
            //            +-----------------//---+   v    v
            //
            // Our network (interleaved) format:
            //            <---- numChans -//--->
            // one sample +---------------//---+  -+    ^
            // frame per  | ch0, ch1, ..., chN |   |    |
            // channel    +---------------//---+  pbuf  |
            //            | ...                |   |   nread
            //            +---------------//---+   v    v
            //
            // WHY? Because we would like to reduce latency of delivery,
            // while transferring data as a byte stream in variable sized
            // (smallish) chunks over memory pipes and TCP/IP.
            // The readStream() API at the receiver requires us to return
            // an equal number of samples from each channel, so we choose
            // to send one sample from each channel through the plumbing
            // for all nread blocks. A receiver can then deliver data to
            // clients after every block.
            uint8_t *pn = pbuf;
            for (int idx=0; idx<nread; ++idx) {
                size_t eoff = idx*fSize;
                for (size_t c=0; c<numChans; ++c) {
                    uint8_t *pc = (uint8_t *)buffs[c];
                    memcpy(pn, pc+eoff, fSize);
                    pn += fSize;
                }
            }
            struct timespec ts;
            clock_gettime(CLOCK_MONOTONIC, &ts);
            SoapySDR_logf(SOAPY_SDR_TRACE, "%ld: dataPump: p<=%d",
                tsdiff(&lt, &ts), elemSize*nread);
            lt = ts;
            // push to pipe in multiples of element size
            if (nullptr==getenv("INHIBIT_PIPE") && pipewrite(pbuf, elemSize, nread, conn->netPipe, false)<0) {
                SoapySDR_log(SOAPY_SDR_WARNING, "dataPump: overrun network pipe, data loss");
            }
        }
        // final write to ensure netPump wakes up and terminates
        pipewrite(pbuf, elemSize, 1, conn->netPipe, false);
        pthread_join(fpid, nullptr);
        freepipe(conn->netPipe);
        conn->netPipe = nullptr;
    } else {
        // TODO:XXX:
        SoapySDR_log(SOAPY_SDR_ERROR, "dataPump: unimplemented data receive funtion :=(");
    }
    // dropping out - deactivate underlying stream
    conn->dev->deactivateStream(conn->stream);
    SoapySDR_logf(SOAPY_SDR_DEBUG, "dataPump: stop: %d", conn->netSock);
    return nullptr;
}

// Connections that have been accepted but have not yet told us what they are.
// The first line sent is "<type>" or "<type>:<key>" (the key is only needed
// when the server was started with one). We read it without ever blocking
// the main loop, and drop peers that take too long.
struct PendingConn {
    std::string line;
    time_t since;
};
static std::map<int, PendingConn> s_pending;

// compare secrets without leaking where they differ
static bool keyMatches(const std::string &a, const std::string &b) {
    unsigned char diff = (a.length() != b.length());
    size_t n = std::min(a.length(), b.length());
    for (size_t i=0; i<n; ++i)
        diff |= (unsigned char)(a[i] ^ b[i]);
    return diff == 0;
}

static void dropPending(int sock) {
    s_pending.erase(sock);
    close(sock);
}

// accept a new connection and park it until it identifies itself.
// Per-client failures are never fatal for the server.
static void acceptConnection(int lsock) {
    struct sockaddr_storage addr;
    socklen_t len = sizeof(addr);
    int sock = accept(lsock, (struct sockaddr *)&addr, &len);
    if (sock<0) {
        SoapySDR_logf(SOAPY_SDR_WARNING,"error accepting connection: %s", strerror(errno));
        return;
    }
    if (s_pending.size()>=MAX_PENDING) {
        SoapySDR_log(SOAPY_SDR_WARNING, "too many unidentified connections, dropping new one");
        close(sock);
        return;
    }
    int fl = fcntl(sock, F_GETFL, 0);
    if (fl<0 || fcntl(sock, F_SETFL, fl|O_NONBLOCK)<0) {
        close(sock);
        return;
    }
    PendingConn pc;
    pc.since = time(nullptr);
    s_pending[sock] = pc;
}

// the type line is complete: authenticate and create the right kind of connection
static void identifyConnection(int sock, const std::string &line) {
    // back to blocking mode for the stdio based protocol handlers
    int fl = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, fl & ~O_NONBLOCK);
    if (line.empty() || !isdigit((unsigned char)line[0]) || (line.length()>1 && line[1]!=':')) {
        SoapySDR_log(SOAPY_SDR_ERROR, "malformed connection type, dropping");
        close(sock);
        return;
    }
    int type = line[0]-'0';
    if (!s_key.empty()) {
        std::string key = line.length()>2? line.substr(2): std::string();
        if (!keyMatches(key, s_key)) {
            SoapySDR_log(SOAPY_SDR_WARNING, "connection rejected: missing or incorrect key");
            close(sock);
            return;
        }
    }
    int rv = -1;
    if (TCPREMOTE_RPC_LOAD==type)
        rv = createRpc(sock);
    else if (TCPREMOTE_LOG_STREAM==type)
        rv = createLog(sock);
    else if (TCPREMOTE_DATA_SEND==type || TCPREMOTE_DATA_RECV==type)
        rv = createData(sock, type);
    else {
        SoapySDR_logf(SOAPY_SDR_ERROR, "unknown connection type: %d", type);
        close(sock);
    }
    // NB: the create...() functions close the socket themselves on failure
    (void)rv;
}

// data is available on a pending connection: collect the type line, a byte at a time
static void handlePending(int sock) {
    auto it = s_pending.find(sock);
    if (it==s_pending.end())
        return;
    while (true) {
        char c;
        ssize_t n = recv(sock, &c, 1, 0);
        if (n==0) {
            dropPending(sock);
            return;
        }
        if (n<0) {
            if (errno==EAGAIN || errno==EWOULDBLOCK)
                return;     // wait for more
            if (errno==EINTR)
                continue;
            dropPending(sock);
            return;
        }
        if (c=='\n') {
            std::string line = it->second.line;
            s_pending.erase(it);
            if (!line.empty() && line.back()=='\r')
                line.pop_back();
            identifyConnection(sock, line);
            return;
        }
        it->second.line += c;
        if (it->second.line.length()>256) {
            SoapySDR_log(SOAPY_SDR_WARNING, "connection type line too long, dropping");
            dropPending(sock);
            return;
        }
    }
}

// discard peers that never identified themselves, and data connections that
// were never attached to a stream (client died between connect and setup)
static void expireConnections() {
    time_t now = time(nullptr);
    std::vector<int> old;
    for (auto &kv : s_pending)
        if (now - kv.second.since > HANDSHAKE_TIMEOUT_S)
            old.push_back(kv.first);
    for (int fd : old) {
        SoapySDR_log(SOAPY_SDR_WARNING, "unidentified connection timed out");
        dropPending(fd);
    }
    old.clear();
    for (auto &kv : s_connections) {
        ConnectionInfo &ci = kv.second;
        if (!ci.rpc && !ci.log && !ci.dev && ci.created && now - ci.created > ORPHAN_DATA_TIMEOUT_S)
            old.push_back(kv.first);
    }
    for (int fd : old) {
        SoapySDR_logf(SOAPY_SDR_WARNING, "unused data connection timed out: %d", fd);
        s_connections.erase(fd);
        close(fd);
    }
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
    // parse the channel list
    std::vector<size_t> channels;
    size_t cur;
    size_t nxt = -1;
    do {
        cur = nxt+1;
        nxt = chans.find(' ',cur);
        channels.push_back(atoi(chans.substr(cur, nxt-cur).c_str()));
    } while (nxt!=std::string::npos);
    // channels must exist on this device
    size_t nchan = conn.dev->getNumChannels(direction);
    for (size_t c : channels) {
        if (c >= nchan) {
            SoapySDR_logf(SOAPY_SDR_ERROR, "setupStream: no such channel: %d", (int)c);
            conn.rpc->writeInteger(-3);
            return 0;
        }
    }
    // fill out the connection details
    ConnectionInfo &data = s_connections.at(dataId);
    data.dev = conn.dev;
    data.direction = direction;
    data.format = fmt;
    data.channels = channels;
    // open the underlying stream
    try {
        data.stream = conn.dev->setupStream(direction, fmt, channels, args);
    } catch (const std::exception &ex) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "setupStream: exception: %s", ex.what());
        data.stream = nullptr;
    }
    if (!data.stream) {
        SoapySDR_log(SOAPY_SDR_ERROR, "setupStream: failed to create underlying stream");
        // the data connection is useless now, don't leak it
        s_connections.erase(dataId);
        close(dataId);
        conn.rpc->writeInteger(-4);
        return 0;
    }
    // all good!
    conn.dataIds.insert(dataId);
    conn.rpc->writeInteger(dataId);
    return 0;
}

int internalStopPumps(ConnectionInfo &data) {
    if (data.pid) {
        pthread_t pid = data.pid;
        data.pid = 0;
        if (pthread_join(pid, nullptr)) {
            SoapySDR_logf(SOAPY_SDR_ERROR, "internalStopPumps: failed to join data pump thread: %s", strerror(errno));
            return -1;
        }
    }
    return 0;
}

int internalCloseStream(ConnectionInfo &conn, int dataId) {
    if (s_connections.find(dataId)==s_connections.end()) {
        SoapySDR_logf(SOAPY_SDR_WARNING, "closeStream: no such data stream ID: %d", dataId);
        return 0;
    }
    ConnectionInfo &data = s_connections.at(dataId);
    internalStopPumps(data);
    if (data.dev && data.stream)
        data.dev->closeStream(data.stream);
    s_connections.erase(dataId);
    close(dataId);
    conn.dataIds.erase(dataId);
    SoapySDR_logf(SOAPY_SDR_INFO, "Closed data connection: %d", dataId);
    // no response
    return 0;
}

int handleCloseStream(ConnectionInfo &conn) {
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleCloseStream()");
    int dataId = conn.rpc->readInteger();
    conn.rpc->writeInteger(0);
    return internalCloseStream(conn, dataId);
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
    // create ourselves a real-time thread to read the data..
    pthread_attr_t pat;
    pthread_attr_init(&pat);
    pthread_attr_setschedpolicy(&pat, SCHED_FIFO);
    struct sched_param sch;
    sch.sched_priority = 1;
    pthread_attr_setschedparam(&pat, &sch);
    pthread_t pid;
    if (pthread_create(&pid, &pat, dataPump, &data)) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "activateStream: failed to create data pump thread: %s", strerror(errno));
        data.pid = 0;
        conn.rpc->writeInteger(-2);
        return 0;
    }
    pthread_attr_destroy(&pat);
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
    if (internalStopPumps(data))
        conn.rpc->writeInteger(-2);
    else
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
    conn.dev->setSampleRate(dir,chn,rate);
    conn.rpc->writeInteger(0);
    return 0;
}

int handleGetSampleRate(ConnectionInfo &conn) {
    // pass-thru, but update rate if device altered it
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetSampleRate()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    conn.rpc->writeDouble(conn.dev->getSampleRate(dir,chn));
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

int handleHasFrequencyCorrection(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleHasFrequencyCorrection()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    conn.rpc->writeInteger(conn.dev->hasFrequencyCorrection(dir,chn));
    return 0;    
}

int handleSetFrequencyCorrection(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleSetFrequencyCorrection()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    double val = conn.rpc->readDouble();
    conn.dev->setFrequencyCorrection(dir,chn,val);
    conn.rpc->writeInteger(0);
    return 0;    
}

int handleGetFrequencyCorrection(ConnectionInfo &conn) {
    // pass-thru
    SoapySDR_log(SOAPY_SDR_DEBUG, "handleGetFrequencyCorrection()");
    int dir = conn.rpc->readInteger();
    int chn = conn.rpc->readInteger();
    conn.rpc->writeDouble(conn.dev->getFrequencyCorrection(dir,chn));
    return 0;    
}

int dropRPC(ConnectionInfo &conn, int fd) {
    SoapySDR_logf(SOAPY_SDR_INFO,"Dropping connection: %d", fd);
    delete conn.rpc;
    while (!conn.dataIds.empty()) {
        int dataId = *(conn.dataIds.begin());
        internalCloseStream(conn, dataId);
    }
    SoapySDR::Device::unmake(conn.dev);
    s_connections.erase(fd);
    return 0;
}

int handleRPC(struct pollfd *pfd, ConnectionInfo &conn) {
    // oops before expected..
    if (pfd->revents & (POLLERR|POLLHUP)) {
        SoapySDR_log(SOAPY_SDR_ERROR,"ERR or HUP on RPC socket");
        return dropRPC(conn, pfd->fd);
    }
    // ensure we have a separator
    if (conn.rpc->readString() != TCPREMOTE_RPC_SEP) {
        SoapySDR_log(SOAPY_SDR_ERROR,"Missing separator on RPC socket (out of sync?)");
        return dropRPC(conn, pfd->fd);
    }
    // dispatch requested RPC..
    int call = conn.rpc->readInteger();
    if (call<0) {
        SoapySDR_log(SOAPY_SDR_ERROR, "EOF or error on RPC socket");
        return dropRPC(conn, pfd->fd);
    }
    SoapySDR_logf(SOAPY_SDR_DEBUG, "handleRPC: call=%d", call);
    switch (call) {
    // unknown
    default:
        // we cannot know how many arguments to skip, so the stream is now out of sync
        SoapySDR_logf(SOAPY_SDR_ERROR,"Unknown RPC call: %d, dropping connection", call);
        conn.rpc->writeInteger(-1000);
        return dropRPC(conn, pfd->fd);
    // special - dropping connection
    case TCPREMOTE_DROP_RPC:
        return dropRPC(conn, pfd->fd);
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
    // frontend corrections API
    case TCPREMOTE_HAS_FREQUENCY_CORRECTION:
        return handleHasFrequencyCorrection(conn);
    case TCPREMOTE_SET_FREQUENCY_CORRECTION:
        return handleSetFrequencyCorrection(conn);
    case TCPREMOTE_GET_FREQUENCY_CORRECTION:
        return handleGetFrequencyCorrection(conn);
    /* NOT IMPLEMENTED ON CLIENT YET!
    TCPREMOTE_HAS_DC_OFFSET_MODE,
    TCPREMOTE_SET_DC_OFFSET_MODE,
    TCPREMOTE_GET_DC_OFFSET_MODE,
    TCPREMOTE_HAS_DC_OFFSET,
    TCPREMOTE_SET_DC_OFFSET,
    TCPREMOTE_GET_DC_OFFSET,
    TCPREMOTE_HAS_IQ_BALANCE,
    TCPREMOTE_SET_IQ_BALANCE,
    TCPREMOTE_GET_IQ_BALANCE,
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

static bool s_logged;
static void handleLog(const SoapySDRLogLevel level, const char *message) {
    // pass to all connected log streams if level is appropriate
    s_logged = true;
    for (auto it = s_connections.begin(); it!=s_connections.end(); ++it) {
        ConnectionInfo &ci = (*it).second;
        if (ci.log && ci.level) {
            // it's a log stream
            if (level > ci.level)
                continue;
            // send level then original message
            fprintf(ci.log, "%d:%s\n", level, message);
        }
    }
    // now our own log
    if (level > s_defaultLogLevel)
        return;
    defaultLogHandler(level, message);
}

int usage() {
    puts("usage: SoapyTCPServer [options]\n"
         "  -l, --listen <host/IP>   address to listen on (default 127.0.0.1, use 0.0.0.0 or :: for all)\n"
         "  -p, --port <port>        port to listen on (default 20655)\n"
         "  -b, --buffer-ms <ms>     target network buffering in milliseconds (default: 10 x stream MTU)\n"
         "  -k, --key <secret>       require clients to present this shared secret\n"
         "  -K, --key-file <file>    as --key, reading the secret from the first line of a file\n"
         "                           (the secret may also be given in $SOAPY_TCPREMOTE_KEY)\n"
         "  -?, --help               show this help\n"
         "\n"
         "NB: the secret is sent in clear text and traffic is not encrypted: on untrusted\n"
         "networks tunnel the port over SSH or a VPN.");
    return 0;
}

static bool isLoopback(const std::string &h) {
    return h=="127.0.0.1" || h=="::1" || h=="localhost";
}

// create, bind and listen, trying each address returned by the resolver
static int makeListenSocket(const std::string &host, const char *port) {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    // "*" (the old documented wildcard) means every address, both families
    std::string h = (host=="*")? "::": host;
    struct addrinfo *res = nullptr;
    int err = getaddrinfo(h.c_str(), port, &hints, &res);
    if (err) {
        SoapySDR_logf(SOAPY_SDR_ERROR, "cannot resolve listen address %s: %s", host.c_str(), gai_strerror(err));
        return -1;
    }
    int lsock = -1;
    for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
        lsock = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (lsock<0)
            continue;
        int opt = 1;
        setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        if (ai->ai_family==AF_INET6) {
            int off = 0;    // dual stack when listening on ::
            setsockopt(lsock, IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof(off));
        }
        if (bind(lsock, ai->ai_addr, ai->ai_addrlen)==0 && listen(lsock, 16)==0)
            break;
        SoapySDR_logf(SOAPY_SDR_WARNING, "listen on %s:%s failed: %s", host.c_str(), port, strerror(errno));
        close(lsock);
        lsock = -1;
    }
    freeaddrinfo(res);
    return lsock;
}

static bool readKeyFile(const char *path, std::string &key) {
    FILE *fp = fopen(path, "r");
    if (!fp) {
        fprintf(stderr, "SoapyTCPServer: cannot open key file %s: %s\n", path, strerror(errno));
        return false;
    }
    char line[512];
    bool ok = fgets(line, sizeof(line), fp)!=nullptr;
    fclose(fp);
    if (!ok) {
        fprintf(stderr, "SoapyTCPServer: key file %s is empty\n", path);
        return false;
    }
    key = line;
    while (!key.empty() && (key.back()=='\n' || key.back()=='\r'))
        key.pop_back();
    return true;
}

int main(int argc, char **argv) {
    std::string host = "127.0.0.1";
    const char *port = "20655";           // 0x50AF ~= SOAP
    if (getenv("SOAPY_TCPREMOTE_KEY"))
        s_key = getenv("SOAPY_TCPREMOTE_KEY");
    for (int arg=1; arg<argc; ++arg) {
        std::string a = argv[arg];
        // options taking a value
        bool hasVal = (a=="-l" || a=="--listen" || a=="-h" || a=="-p" || a=="--port" ||
                       a=="-b" || a=="--buffer-ms" || a=="-k" || a=="--key" || a=="-K" || a=="--key-file");
        if (a=="-?" || a=="--help")
            return usage();
        if (!hasVal) {
            fprintf(stderr, "SoapyTCPServer: unknown option: %s\n", a.c_str());
            usage();
            return 1;
        }
        if (arg+1>=argc) {
            fprintf(stderr, "SoapyTCPServer: option %s needs a value\n", a.c_str());
            return 1;
        }
        const char *val = argv[++arg];
        if (a=="-l" || a=="--listen")
            host = val;
        else if (a=="-h") {     // pre-1.0 spelling, kept for existing scripts
            fprintf(stderr, "SoapyTCPServer: -h <host> is deprecated, use -l\n");
            host = val;
        } else if (a=="-p" || a=="--port")
            port = val;
        else if (a=="-b" || a=="--buffer-ms") {
            s_bufferMs = atoi(val);
            if (s_bufferMs<0 || s_bufferMs>10000) {
                fprintf(stderr, "SoapyTCPServer: buffer-ms must be 0..10000\n");
                return 1;
            }
        } else if (a=="-k" || a=="--key")
            s_key = val;
        else if (a=="-K" || a=="--key-file") {
            if (!readKeyFile(val, s_key))
                return 1;
        }
    }
    // writing to a peer that has gone away must give EPIPE, not kill the server
    signal(SIGPIPE, SIG_IGN);
    // Detect current log level - shenannigans required as we cannot simply read the value
    s_defaultLogLevel = detectLogLevel();
    printf("SoapyTCPServer: log level=%d\n", (int)s_defaultLogLevel);
    // Now collect all log levels, we filter per-client ourselves
    SoapySDR_registerLogHandler(handleLog);
    SoapySDR_setLogLevel(SOAPY_SDR_TRACE);
    if (s_key.empty() && !isLoopback(host))
        SoapySDR_logf(SOAPY_SDR_WARNING,
            "listening on %s with no key: anyone who can reach this port can use your SDR (see --key)", host.c_str());
    printf("SoapyTCPServer: listening on: %s:%s%s\n", host.c_str(), port, s_key.empty()? "": " (key required)");
    // Set up listen socket
    int lsock = makeListenSocket(host, port);
    if (lsock<0) {
        SoapySDR_log(SOAPY_SDR_ERROR, "unable to create listen socket");
        return 2;
    }
    // Wait for connections / requests on RPC sockets
    enum Kind { LISTEN, PENDING, CONN };
    struct Slot { int fd; Kind kind; };
    while (true) {
        // one poll entry for the listen socket, each unidentified connection, and each RPC/LOG connection;
        // data stream connections are driven by their own threads
        std::vector<struct pollfd> pfds;
        std::vector<Slot> slots;
        pfds.push_back({lsock, POLLIN, 0});
        slots.push_back({lsock, LISTEN});
        for (auto &kv : s_pending) {
            pfds.push_back({kv.first, POLLIN, 0});
            slots.push_back({kv.first, PENDING});
        }
        for (auto &kv : s_connections) {
            if (kv.second.rpc || kv.second.log) {
                pfds.push_back({kv.first, POLLIN, 0});
                slots.push_back({kv.first, CONN});
            }
        }
        // wake up regularly so we can expire stale connections
        int pr = poll(pfds.data(), pfds.size(), 1000);
        if (pr<0) {
            if (errno==EINTR)
                continue;
            SoapySDR_logf(SOAPY_SDR_ERROR,"waiting for input: %s", strerror(errno));
            break;
        }
        expireConnections();
        if (pr==0)
            continue;
        // Handle listen socket events
        if (pfds[0].revents) {
            if (pfds[0].revents & (POLLERR|POLLHUP|POLLNVAL)) {
                SoapySDR_log(SOAPY_SDR_ERROR,"EOF or error in listen socket");
                break;
            }
            acceptConnection(lsock);
        }
        // Handle everything else. Handlers may remove connections, so look each one up afresh.
        for (size_t idx=1; idx<pfds.size(); ++idx) {
            if (!pfds[idx].revents)
                continue;
            int fd = slots[idx].fd;
            if (PENDING==slots[idx].kind) {
                if (pfds[idx].revents & (POLLERR|POLLNVAL))
                    dropPending(fd);
                else
                    handlePending(fd);       // also copes with HUP (read returns 0)
                continue;
            }
            auto it = s_connections.find(fd);
            if (it==s_connections.end())
                continue;
            ConnectionInfo &ci = it->second;
            if (ci.rpc) {
                int r = handleRPC(&pfds[idx], ci);
                // a failed handler (or failed socket) only costs us that client
                it = s_connections.find(fd);
                if (it!=s_connections.end() && it->second.rpc && (r<0 || it->second.rpc->failed()))
                    dropRPC(it->second, fd);
            } else if (ci.log) {
                // any input or error on log stream means we're done
                fclose(ci.log);
                s_connections.erase(fd);
                SoapySDR_logf(SOAPY_SDR_INFO, "log stream closed: %d", fd);
            }
        }
    }
    // orderly shutdown: release devices
    while (!s_connections.empty()) {
        auto it = s_connections.begin();
        if (it->second.rpc)
            dropRPC(it->second, it->first);
        else
            s_connections.erase(it);
    }
    for (auto &kv : s_pending)
        close(kv.first);
    close(lsock);
    return 0;
}
