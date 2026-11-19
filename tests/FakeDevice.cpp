//  FakeDevice.cpp - deterministic fake SDR used by the loopback tests
//  SPDX-License-Identifier: BSL-1.0
//
//  Two RX channels of CS16 samples. Sample number n (counting from zero since
//  the stream was activated) on channel c is:  I = n & 0x7fff,  Q = (3n + 100c) & 0x7fff
//  so the receiving side can verify that nothing was lost, duplicated,
//  mis-interleaved or split part way through a frame.

#include <SoapySDR/Device.hpp>
#include <SoapySDR/Registry.hpp>
#include <SoapySDR/Formats.hpp>
#include <chrono>
#include <thread>
#include <string.h>

class FakeStream {
public:
    std::vector<size_t> chans;
};

class FakeDevice : public SoapySDR::Device
{
    double rate = 1e6, freq = 100e6, gain = 10;
    unsigned long long counter = 0;
    bool active = false;
    std::chrono::steady_clock::time_point next;
    static const size_t MTU = 256;
public:
    std::string getDriverKey() const { return "faketest"; }
    std::string getHardwareKey() const { return "faketest-hw"; }
    size_t getNumChannels(const int dir) const { return SOAPY_SDR_RX==dir? 2: 0; }
    std::vector<std::string> getStreamFormats(const int, const size_t) const { return {SOAPY_SDR_CS16}; }
    std::string getNativeStreamFormat(const int, const size_t, double &fullScale) const {
        fullScale = 32768;
        return SOAPY_SDR_CS16;
    }
    SoapySDR::Stream *setupStream(const int dir, const std::string &fmt, const std::vector<size_t> &ch, const SoapySDR::Kwargs &) {
        if (dir!=SOAPY_SDR_RX || fmt!=SOAPY_SDR_CS16)
            throw std::runtime_error("faketest: only CS16 RX");
        FakeStream *fs = new FakeStream();
        fs->chans = ch;
        return (SoapySDR::Stream *)fs;
    }
    void closeStream(SoapySDR::Stream *s) { delete (FakeStream *)s; }
    size_t getStreamMTU(SoapySDR::Stream *) const { return MTU; }
    int activateStream(SoapySDR::Stream *, const int, const long long, const size_t) {
        counter = 0;
        active = true;
        next = std::chrono::steady_clock::now();
        return 0;
    }
    int deactivateStream(SoapySDR::Stream *, const int, const long long) { active = false; return 0; }
    int readStream(SoapySDR::Stream *stream, void * const *buffs, const size_t numElems, int &flags, long long &timeNs, const long) {
        // pace the output at the sample rate, as real hardware would
        next += std::chrono::nanoseconds((long long)(1e9 * MTU / rate));
        std::this_thread::sleep_until(next);
        size_t n = std::min(numElems, MTU);
        FakeStream *fs = (FakeStream *)stream;
        for (size_t k=0; k<fs->chans.size(); ++k) {
            const size_t c = fs->chans[k];
            int16_t *p = (int16_t *)buffs[k];
            for (size_t i=0; i<n; ++i) {
                p[2*i]   = (int16_t)((counter+i) & 0x7fff);
                p[2*i+1] = (int16_t)((3*(counter+i) + 100*c) & 0x7fff);
            }
        }
        counter += n;
        return (int)n;
    }
    void setSampleRate(const int, const size_t, const double r) { rate = r; }
    double getSampleRate(const int, const size_t) const { return rate; }
    SoapySDR::RangeList getSampleRateRange(const int, const size_t) const { return {SoapySDR::Range(1e5, 2e6)}; }
    void setFrequency(const int, const size_t, const double f, const SoapySDR::Kwargs &) { freq = f; }
    double getFrequency(const int, const size_t) const { return freq; }
    void setGain(const int, const size_t, const double g) { gain = g; }
    double getGain(const int, const size_t) const { return gain; }
};

static SoapySDR::KwargsList findFake(const SoapySDR::Kwargs &) {
    SoapySDR::Kwargs a;
    a["driver"] = "faketest";
    return {a};
}
static SoapySDR::Device *makeFake(const SoapySDR::Kwargs &) { return new FakeDevice(); }
static SoapySDR::Registry registerFake("faketest", &findFake, &makeFake, SOAPY_SDR_ABI_VERSION);
