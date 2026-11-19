// SoapyRPC.hpp - remote procedure call endpoint
// Copyright (c) 2021 Phil Ashby
// SPDX-License-Identifier: BSL-1.0

#ifndef SoapyRPC_hpp
#define SoapyRPC_hpp

// This RPC implementation uses text I/O over TCP
// in the tradition of many 'simple xxx' internet
// protocols.

#include <stdio.h>
#include <errno.h>
#include <math.h>
#include <cstring>
#include <map>
#include <string>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <SoapySDR/Types.hpp>
#include <SoapySDR/Logger.hpp>

// Disable Nagle on a socket. RPC traffic is many small request/response
// lines, where Nagle + delayed ACK can add ~40ms to every call.
static inline void soapyNoDelay(int sock) {
    int one = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

// Longest line we will accept from the network (protects against a peer
// that never sends a newline).
const size_t TCPREMOTE_MAX_LINE = 65536;

// map of format names to frame sizes
const std::map<std::string, size_t> g_frameSizes = {
    { "CS8", 2 }, { "CS16", 4 }, { "CF32", 8 },
};

// RPC separator
const std::string TCPREMOTE_RPC_SEP = "--";

// RPC codes
enum
{
    // internal specials - socket type identifier, single digit value please..
    TCPREMOTE_RPC_LOAD,
    TCPREMOTE_LOG_STREAM,
    TCPREMOTE_DATA_SEND,
    TCPREMOTE_DATA_RECV,
    // identification API
    TCPREMOTE_GET_HARDWARE_KEY = 10,
    TCPREMOTE_GET_HARDWARE_INFO,
    // channel API
    TCPREMOTE_SET_FRONTEND_MAPPING,
    TCPREMOTE_GET_FRONTEND_MAPPING,
    TCPREMOTE_GET_NUM_CHANNELS,
    TCPREMOTE_GET_CHANNEL_INFO,
    TCPREMOTE_GET_FULL_DUPLEX,
    // stream API
    TCPREMOTE_GET_STREAM_FORMATS,
    TCPREMOTE_GET_STREAM_NATIVE_FORMAT,
    TCPREMOTE_GET_STREAM_ARGS_INFO,
    TCPREMOTE_SETUP_STREAM,
    TCPREMOTE_CLOSE_STREAM,
    TCPREMOTE_GET_STREAM_MTU,
    TCPREMOTE_ACTIVATE_STREAM,
    TCPREMOTE_DEACTIVATE_STREAM,
    // direct buffer access API - bit tricky over a network, no RPCs =)
    // antenna API
    TCPREMOTE_LIST_ANTENNAS,
    TCPREMOTE_SET_ANTENNA,
    TCPREMOTE_GET_ANTENNA,
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
    // gain API
    TCPREMOTE_LIST_GAINS,
    TCPREMOTE_HAS_GAIN_MODE,
    TCPREMOTE_SET_GAIN_MODE,
    TCPREMOTE_GET_GAIN_MODE,
    TCPREMOTE_SET_GAIN,
    TCPREMOTE_SET_GAIN_NAMED,
    TCPREMOTE_GET_GAIN,
    TCPREMOTE_GET_GAIN_NAMED,
    TCPREMOTE_GET_GAIN_RANGE,
    TCPREMOTE_GET_GAIN_RANGE_NAMED,
    // frequency API
    TCPREMOTE_SET_FREQUENCY,
    TCPREMOTE_SET_FREQUENCY_NAMED,
    TCPREMOTE_GET_FREQUENCY,
    TCPREMOTE_GET_FREQUENCY_NAMED,
    TCPREMOTE_LIST_FREQUENCIES,
    TCPREMOTE_GET_FREQUENCY_RANGE,
    TCPREMOTE_GET_FREQUENCY_RANGE_NAMED,
    TCPREMOTE_GET_FREQUENCY_ARGS_INFO,
    // sample rate API
    TCPREMOTE_SET_SAMPLE_RATE,
    TCPREMOTE_GET_SAMPLE_RATE,
    // list rates deprecated, we emulate in client side
    TCPREMOTE_GET_SAMPLE_RATE_RANGE,
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
    TCPREMOTE_READ_UART,
    // internal special - dropping connection
    TCPREMOTE_DROP_RPC = 1000
};

// Design notes:
// - holds error state, to ensure no further I/O is attempted once errored,
//   this allows strerror()/perror() to work despite subsequent rpc methods
//   and allows error to be detected at the end of a series of rpc methods.
class SoapyRPC
{
public:
    SoapyRPC(int socket) {
        hasError = false;
        handle = fdopen(socket, "r+");
        if (handle) {
            setlinebuf(handle);
            SoapySDR_logf(SOAPY_SDR_TRACE, "SoapyRPC::<cons>(%d)", socket);
        } else {
            SoapySDR_logf(SOAPY_SDR_ERROR, "SoapyRPC::<cons>, failed to fdopen socket: %s", strerror(errno));
            hasError = true;
        }
    }
    SoapyRPC(FILE *fp) {
        hasError = false;
        handle = fp;
        setlinebuf(handle);
        SoapySDR_logf(SOAPY_SDR_TRACE, "SoapyRPC::<cons>(%p=>%d)", fp, fileno(fp));
    }
    ~SoapyRPC() {
        SoapySDR_logf(SOAPY_SDR_TRACE, "SoapyRPC::<dest>(%d)", handle? fileno(handle): -1);
        if (handle)
            fclose(handle);
    }
    int writeInteger(const int i) {
        if (hasError) return -1;
        SoapySDR_logf(SOAPY_SDR_TRACE, "Wi %d", i);
        int r = fprintf(handle, "%d\n", i);
        if (r<0) {
            SoapySDR_logf(SOAPY_SDR_ERROR, "SoapyRPC::writeInteger: %s", strerror(errno));
            hasError = true;
        }
        return r;
    }
    int writeDouble(const double d) {
        if (hasError) return -1;
        SoapySDR_logf(SOAPY_SDR_TRACE, "Wd %.17g", d);
        // %.17g round-trips any double exactly (plain %f kept only 6 decimals)
        int r = fprintf(handle, "%.17g\n", d);
        if (r<0) {
            SoapySDR_logf(SOAPY_SDR_ERROR, "SoapyRPC::writeDouble %s", strerror(errno));
            hasError = true;
        }
        return r;
    }
    int writeString(const std::string s) {
        if (hasError) return -1;
        SoapySDR_logf(SOAPY_SDR_TRACE, "Ws %s", s.c_str());
        int r = fprintf(handle, "%s\n", s.c_str());
        if (r<0) {
            SoapySDR_logf(SOAPY_SDR_ERROR, "SoapyRPC::writeString: %s", strerror(errno));
            hasError = true;
        }
        return r;
    }
    int writeKwargs(const SoapySDR::Kwargs &args) {
        if (hasError) return -1;
        int n = 0, r;
        for (auto it=args.begin(); it!=args.end(); ++it) {
            std::string nv;
            nv += it->first;
            nv += '=';
            nv += it->second;
            r = writeString(nv);
            if (r<0)
                return r;
            n += r;
        }
        r = writeString("=");      // terminator
        if (r<0)
            return r;
        n += r;
        return n;
    }
    int writeStrVector(const std::vector<std::string> &vec) {
        if (hasError) return -1;
        int n = 0, r;
        for (auto it: vec) {
            r = writeString(it);
            if (r<0)
                return r;
            n += r;
        }
        r = writeString("");
        if (r<0)
            return r;
        n += r;
        return n;
    }
    int readInteger() {
        if (hasError) return -1;
        int rv=-1;
        std::string s = readString();
        if (s.length()>0)
            sscanf(s.c_str(), "%d", &rv);
        else
            SoapySDR_log(SOAPY_SDR_ERROR, "SoapyRPC::readInteger, empty line");
        return rv;
    }
    double readDouble() {
        if (hasError) return NAN;
        double rv=NAN;
        std::string s = readString();
        if (s.length()>0)
            sscanf(s.c_str(), "%lf", &rv);
        else
            SoapySDR_log(SOAPY_SDR_ERROR, "SoapyRPC::readDouble, empty line");
        return rv;
    }
    // Read one line, without its terminating "\n" (or "\r\n").
    // Lines of any length up to TCPREMOTE_MAX_LINE are supported; EOF, a read
    // error or an over-long line puts the connection into the error state.
    std::string readString() {
        std::string rv;
        if (hasError) return rv;
        char chunk[256];
        bool done = false;
        while (!done) {
            if (!fgets(chunk, sizeof(chunk), handle)) {
                if (feof(handle))
                    SoapySDR_log(SOAPY_SDR_DEBUG, "SoapyRPC::readString: EOF");
                else
                    SoapySDR_logf(SOAPY_SDR_ERROR, "SoapyRPC::readString: %s", strerror(errno));
                hasError = true;
                rv.clear();
                return rv;
            }
            size_t n = strlen(chunk);
            if (n>0 && chunk[n-1]=='\n') {
                chunk[--n] = 0;
                done = true;
            }
            rv.append(chunk, n);
            if (rv.length()>TCPREMOTE_MAX_LINE) {
                SoapySDR_log(SOAPY_SDR_ERROR, "SoapyRPC::readString: line too long");
                hasError = true;
                rv.clear();
                return rv;
            }
        }
        if (!rv.empty() && rv.back()=='\r')
            rv.pop_back();
        SoapySDR_logf(SOAPY_SDR_TRACE, "R '%s'", rv.c_str());
        return rv;
    }
    SoapySDR::Kwargs readKwargs() {
        SoapySDR::Kwargs args;
        if (hasError) return args;
        while (true) {
            std::string nv = readString();
            if (nv.length()<2)      // '=' or empty is a terminator
                break;
            size_t p = nv.find('=');
            if (p!=std::string::npos && p>0) {
                args[nv.substr(0,p)] = nv.substr(p+1);
            } else {
                SoapySDR_logf(SOAPY_SDR_INFO,"SoapyRPC::readKwargs, ignored invalid line: %s", nv.c_str());
            }
        }
        return args;
    }
    std::vector<std::string> readStrVector() {
        std::vector<std::string> list;
        if (hasError) return list;
        while (true) {
            // blank/error indicates end of list
            std::string str = readString();
            if (str.length()==0)
                break;
            list.push_back(str);
        }
        return list;
    }

    // true once any I/O has failed; no further I/O is attempted
    bool failed() const { return hasError; }

private:
    FILE *handle;
    bool hasError;
};

#endif
