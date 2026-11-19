//  SoapyTCPRemote.hpp
//  Copyright (c) 2021 Phil Ashby
//  SPDX-License-Identifier: BSL-1.0

#ifndef SoapyTCPRemote_hpp
#define SoapyTCPRemote_hpp

#include <thread>

#include <SoapySDR/Device.hpp>
#include <SoapySDR/Registry.hpp>
#include <SoapySDR/Version.hpp>

#include "SoapyRPC.hpp"

class SoapyTCPRemote : public SoapySDR::Device
{

private:
    // connection details & remote driver + any settings
    const std::string remoteAddress;
    const std::string remotePort;
    const std::string remoteDriver;
    const std::string remoteArgs;
    // optional shared secret, sent with each connection type line
    const std::string remoteKey;
    // requested & wire formats, as we may choose smaller native format
    std::string fmtout;
    std::string fmtwire;
    // network connect
    int connect(bool lowLatency) const;
    // first line of every connection: "<type>" or "<type>:<key>"
    std::string typeLine(int type) const;
    // RPC handler
    SoapyRPC *rpc;
    // Log stream, ID and thread
    FILE *log;
    int logId;
    std::thread logThread;
    // helpers
    int loadRemoteDriver() const;
    int connectLogStream(SoapySDRLogLevel level);
    static void processLogStream(SoapyTCPRemote *rem);
public:
    SoapyTCPRemote(const std::string &address, const std::string &port, const std::string &remdriver, const std::string &remargs, const std::string &remkey = std::string());
    ~SoapyTCPRemote();

    // Identification API (driver local, others remote)
    std::string getDriverKey() const { return "tcpRemote"; }
    std::string getHardwareKey() const;
    SoapySDR::Kwargs getHardwareInfo(void) const;

    // Channels API (all remote)
    void setFrontendMapping(const int direction, const std::string &mapping);
    std::string getFrontendMapping(const int direction) const;
    size_t getNumChannels(const int dir) const;
    SoapySDR::Kwargs getChannelInfo(const int direction, const size_t channel) const;
    bool getFullDuplex(const int direction, const size_t channel) const;

    // Stream API (all remote)
    std::vector<std::string> getStreamFormats(const int direction, const size_t channel) const;
    std::string getNativeStreamFormat(const int direction, const size_t channel, double &fullScale) const;
    SoapySDR::ArgInfoList getStreamArgsInfo(const int direction, const size_t channel) const;
    SoapySDR::Stream* setupStream(const int direction, const std::string &format, const std::vector<size_t> &channels = std::vector<size_t>(), const SoapySDR::Kwargs &args = SoapySDR::Kwargs());

    void closeStream(SoapySDR::Stream *stream);
    size_t getStreamMTU(SoapySDR::Stream *stream) const;
    int activateStream(SoapySDR::Stream *stream,
                       const int flags = 0,
                       const long long timeNs = 0,
                       const size_t numElems = 0);
    int deactivateStream(SoapySDR::Stream *stream, const int flags = 0, const long long timeNs = 0);

    // local methods, streams data to/from remote
    int readStream(SoapySDR::Stream *stream,
                   void * const *buffs,
                   const size_t numElems,
                   int &flags,
                   long long &timeNs,
                   const long timeoutUs = 100000);
    int writeStream(SoapySDR::Stream *stream,
                    const void * const *buffs,
                    const size_t numElems,
                    int &flags,
                    const long long timeNs = 0,
                    const long timeoutUs = 100000);
    int readStreamStatus(
                    SoapySDR::Stream *stream,
                    size_t &chanMask,
                    int &flags,
                    long long &timeNs,
                    const long timeoutUs = 100000);

    // Direct Buffer Access API (nope!)
    size_t getNumDirectAccessBuffers(SoapySDR::Stream *stream) const { return 0; }

    // Antennas (not yet!)
    std::vector<std::string> listAntennas(const int direction, const size_t channel) const { std::vector<std::string> l; return l; }

    // DC offset, IQ balance & Frequency correction (only frequency so far)
    bool hasDCOffsetMode(const int direction, const size_t channel) const { return false; }
    bool hasDCOffset(const int direction, const size_t channel) const { return false; }
    bool hasIQBalanceMode(const int direction, const size_t channel) const { return false; }
    bool hasIQBalance(const int direction, const size_t channel) const { return false; }
    bool hasFrequencyCorrection(const int direction, const size_t channel) const;
    void setFrequencyCorrection(const int direction, const size_t channel, double value);
    double getFrequencyCorrection(const int direction, const size_t channel) const;

    // Gain (all remote)
    std::vector<std::string> listGains(const int direction, const size_t channel) const;
    bool hasGainMode(const int direction, const size_t channel) const;
    void setGainMode(const int direction, const size_t channel, const bool automatic);
    bool getGainMode(const int direction, const size_t channel) const;
    void setGain(const int direction, const size_t channel, const double value);
    void setGain(const int direction, const size_t channel, const std::string &name, const double value);
    double getGain(const int direction, const size_t channel) const;
    double getGain(const int direction, const size_t channel, const std::string &name) const;
    SoapySDR::Range getGainRange(const int direction, const size_t channel) const;
    SoapySDR::Range getGainRange(const int direction, const size_t channel, const std::string &name) const;

    // Frequency (all remote)
    void setFrequency(const int direction,
                      const size_t channel,
                      const double frequency,
                      const SoapySDR::Kwargs &args = SoapySDR::Kwargs());
    void setFrequency(const int direction,
                      const size_t channel,
                      const std::string &name,
                      const double frequency,
                      const SoapySDR::Kwargs &args = SoapySDR::Kwargs());
    double getFrequency(const int direction, const size_t channel) const;
    double getFrequency(const int direction, const size_t channel, const std::string &name) const;
    std::vector<std::string> listFrequencies(const int direction, const size_t channel) const;
    SoapySDR::RangeList getFrequencyRange(const int direction, const size_t channel) const;
    SoapySDR::RangeList getFrequencyRange(const int direction, const size_t channel, const std::string &name) const;
    SoapySDR::ArgInfoList getFrequencyArgsInfo(const int direction, const size_t channel) const;

    // Sample Rate API (all remote)
    void setSampleRate(const int direction, const size_t channel, const double rate);
    double getSampleRate(const int direction, const size_t channel) const;
    std::vector<double> listSampleRates(const int direction, const size_t channel) const;
    SoapySDR::RangeList getSampleRateRange(const int direction, const size_t channel) const;

    // Bandwidth, Clocking, Time, Sensor, Register, Settings, GPIO, I2C, SPI, UART APIs (not yet!)
};

#endif /* SoapyTCPRemote_hpp */
