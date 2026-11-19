# SoapyTCPRemote
TCP based SoapySDR remoting, no clever protocols, just a TCP/IP byte stream.

## Credit where it's due
**SoapyTCPRemote is the work of [Phil Ashby](https://github.com/phlash).** The idea, the protocol, the
client driver and the server are all his: this repository carries his full commit history, and the
source files still carry his copyright and licence headers. Phil built it because SoapyRemote kept
glitching on his network, USB/IP was far too slow, and a UDP audio streaming approach flattened his small
source device, so he wrote a deliberately simple TCP design that works. Thank you, Phil.

This version keeps his design and extends it with fixes, security and tooling, described next. If you
find these changes useful, the good ideas are his; if you find a bug, it's very likely in the parts added
here, so please report it here.

## What this version adds
Everything below is on top of the original, and the wire protocol stays compatible (see the notes under
*Compatibility*).

### Reliability fixes
 * **The server survives bad clients.** Previously a client that connected and hung up, a failed reply or an
   accept error could end the whole server. Now only that connection is dropped, new connections are
   identified without blocking the server (5 s deadline), stalled peers time out, and `SIGPIPE` is ignored.
 * **Correct sample delivery.** The client's `readStream` could drop or misalign data when a TCP read ended
   part way through a frame, miscounted multi-channel streams, overran a buffer and ignored the timeout. It
   now keeps partial frames, honours the timeout and handles any channel count. `writeStream` sends in one
   batch instead of one syscall per sample.
 * Stream set-up no longer sends two replies on failure; channels are validated; driver exceptions are
   caught; unused data connections are cleaned up; the RX ring buffer is freed.
 * Long lines are read correctly (the last character was previously chopped), and numbers are sent with
   full precision.

### Security
 * **Optional shared key** (`--key`, `--key-file` or `SOAPY_TCPREMOTE_KEY` on the server, `tcpremote:key`,
   the same variable, or `key=` in the config file on the client).
 * **Listens on `127.0.0.1` by default** instead of every interface, with a warning when you open it up
   without a key.

### Features and performance
 * **`-b <ms>` buffering control**, so the ~250 ms buffering Phil's design aims for is a setting rather than
   a guess.
 * **IPv6** on both ends (`[::1]:20655`, bare IPv6 addresses, dual-stack listening), and fallback across every
   address a name resolves to.
 * `TCP_NODELAY` on control connections, and bulk `memcpy` in the data path (useful on small devices).
 * Long option names, and a usage message that matches the real options.
 * A stricter, documented client config file.

### Project
 * **Tests:** a fake SDR and end-to-end loopback tests (`ctest`) that check sample integrity, conversion,
   authentication and misbehaving clients. They also run clean under AddressSanitizer and UBSan.
 * **CI** on Ubuntu 22.04/24.04 and Debian stable, a **systemd unit**, a **LICENSE** file (BSL-1.0, as the
   source headers state), written **protocol documentation** ([docs/PROTOCOL.md](docs/PROTOCOL.md)), and
   builds with `-Wall -Wextra`.

### Compatibility
 * The data stream and RPC protocol are unchanged apart from two additions: the first line of a connection
   may carry a key (`<type>:<key>`), and floating point values are sent as `%.17g`. A client that does not
   send a key works with a server that has none, and old and new peers otherwise interoperate. Servers with
   a key reject old clients, and a keyed client talking to an old server will fail, so update both ends.
 * **Behaviour change:** the server listens on localhost by default. Use `-l 0.0.0.0` to serve other
   machines. The old `-h <host>` option still works but is deprecated.

## Why? (from the original README)
Because I've tried SoapyRemote, and it always has streaming errors due to network underrun/audio overrun,
which is daft when I have a solid 80Mbits/sec (10Mbytes/sec) from the source device to my laptop (measured
using a simple `scp` command).

I've also tried USB/IP, which is a total loss for anything faster than keyboards.

I've tried a network audio solution [TRX](http://www.pogo.org.uk/%7Emark/trx/streaming-desktop-audio.html),
which looks good with RTP for networking, but sadly due to having to switch between user and kernel
mode for every UDP packet at 500Hz+ thrashes my wee source device to kernel panic & death =(

## How? (from the original README)
By using the same approach as simple TCP/IP network protocols, use the capabilities of the underlying
TCP/IP stack which is reliable, efficiently and implemented in the kernel so I don't have problems
due to process/thread thrashing. Atop this, use `stdio` to manage buffering and data segmentation
(less code for me to write), and finally, don't aim for stupid low latency, choose buffer sizes that
provide ~250msecs delay, this again reduces thrashing on the source device (which is tiny =)).

The protocol is documented in [docs/PROTOCOL.md](docs/PROTOCOL.md).

## Building
You will need SoapySDR with its development files (`libsoapysdr-dev` on Debian/Ubuntu, or built from
source), `cmake` and a C++ compiler (`build-essential`).

Then it's the usual CMake incantations (on both source and target devices!):
 * `cmake -B build`
 * `cmake --build build`
 * `su` or `sudo -s`
 * `cd build; make install`

After which you should be able to check the driver is installed with `SoapySDRUtil --info` and run
the server (on the device where your SDR is attached) `SoapyTCPServer`.

Options: `-DBUILD_TESTS=OFF` skips the tests (handy on small devices), `-DINSTALL_SYSTEMD=ON` installs the
systemd unit. If an older copy of the module is already installed, remove it first, because SoapySDR loads
the first `tcpremote` module it finds.

## Usage
 * Run the server on the target device: `SoapyTCPServer`
 * Connect from the client: `SoapySDRUtil --probe="driver=tcpremote,tcpremote:address=<serverIP>,tcpremote:driver=<serverSDR>"`
 * Once you have a working connection string, use in your favourite SDR package such as gqrx.

`tcpremote:address` is `host`, `host:port`, `[ipv6]:port` or a bare IPv6 address, and the port defaults to 20655.
Only one program can use an SDR at a time, so stop anything else holding it (such as SpyServer).

### Server options
| option | meaning |
|--------|---------|
| `-l, --listen <host>` | address to listen on. **Default is `127.0.0.1` (this machine only).** Use `0.0.0.0` (IPv4) or `::` / `*` (everything) to serve other machines |
| `-p, --port <port>`   | port, default 20655 |
| `-b, --buffer-ms <ms>`| buffer about this many milliseconds of samples between the device and the network (default: ten stream MTUs, which is only a few ms on fast devices). `-b 250` gives the ~250ms described above, and rides out network hiccups |
| `-k, --key <secret>`, `-K, --key-file <file>` | require clients to present this secret (or set `SOAPY_TCPREMOTE_KEY`) |

### Security
The server gives whoever connects full control of the SDR, including (where the device supports
them) register, GPIO, I2C and SPI access, so don't expose it to networks you don't trust.

 * Listening is limited to localhost unless you ask otherwise, and the server warns if you open it
   up with no key.
 * With `--key` the server refuses clients that don't present the same key. Give the client the key
   with the device argument `tcpremote:key=<secret>`, the `SOAPY_TCPREMOTE_KEY` environment variable,
   or `key=<secret>` in the client config file (below). Keep that file private (`chmod 600`).
 * The key and the sample data are **not encrypted**. Anywhere you can't trust the network, tunnel the
   port, e.g. `ssh -L 20655:localhost:20655 user@sdrhost` and connect to `127.0.0.1`, or use WireGuard.

### Client configuration file
Instead of repeating connection arguments, put defaults in `SoapyTCPRemote.conf`, searched for in
`$XDG_CONFIG_DIRS`, `/etc/xdg` and `~/.config`. One `name=value` per line (no spaces around `=`), `#` for
comments, names `address`, `driver`, `args` and `key`:

    address=10.10.10.3:20655
    driver=airspy
    args=period=9600
    key=mysecret

Then `SoapySDRUtil --probe="driver=tcpremote"` is enough.

### Running at boot
There is a systemd unit in [systemd/](systemd/) (`cmake -DINSTALL_SYSTEMD=ON` installs the unit file; copy
`soapytcpserver.default` to `/etc/default/soapytcpserver` to configure it).

## Debugging
So it's not working first time? You can get significant details by setting the SoapySDR log level in the environment:
 * `SOAPY_SDR_LOG_LEVEL=<VALUE>` where `<VALUE>` is one of: `ERROR, WARNING, NOTICE, INFO (def), DEBUG, TRACE`

The server's log messages are also sent to the client's log, filtered by the client's level.

### Troubleshooting
 * *"SoapySDR::Device::make() no match"*: the `tcpremote` module isn't installed (or isn't being found) on the client.
   Check `SoapySDRUtil --info`.
 * *"duplicate entry for tcpremote"*: two copies of the module are installed. Remove the old one.
 * *"unable to connect log stream" / "Failed to connect"*: is the server running, and listening on an address
   the client can reach (`-l 0.0.0.0`)? Is a firewall in the way of port 20655?
 * *Connection accepted, then immediately closed*: the server has a key and the client didn't send the right one.
 * *"overrun network pipe, data loss" on the server*: the network can't keep up with the sample rate. Lower the
   sample rate, and try a larger buffer (`-b 500`).
 * *Gaps or overflow messages on a healthy network*: increase `-b`.
 * *The SDR can't be opened*: another program (SpyServer, rtl_tcp, ...) is probably using it.

## Limitations
 * RX only: the server does not implement TX streams yet.
 * Not implemented yet (the client reports "not supported"/empty): antennas, bandwidth, DC offset and IQ
   balance, clocking, time, sensors, registers, settings, GPIO, I2C, SPI and UART. Applications that insist on
   antenna or bandwidth controls may need these first. PRs welcome!
 * Streams use the formats `CS8`, `CS16` and `CF32`. When `CF32` is requested from a `CS16` device the
   conversion is done on the client, to halve the bytes on the wire.
 * Traffic is not encrypted (see Security).
 * Developed and tested on Debian and Ubuntu, including an Orange Pi Zero 3 serving an Airspy One; other ports welcome.

## Testing
`ctest --test-dir build --output-on-failure` runs end to end loopback tests: real `SoapyTCPServer` processes
serve a small fake SDR (`tests/FakeDevice.cpp`, which generates a known ramp) to a client, checking
sample integrity over split reads and multiple channels, `CF32` conversion, authentication, and that
misbehaving clients cannot take the server down.

## Licence
Boost Software License 1.0, see [LICENSE](LICENSE). Original work Copyright (c) 2021 Phil Ashby; changes
in this version are offered under the same licence.

## Feedback
Feel free to raise issues for discussion or problems or features, better yet: submit PRs. As Phil put it in
the original: "submit PRs to fix my poor code please!"
