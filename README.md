# SoapyTCPRemote
TCP based SoapySDR remoting, no clever protocols, just a TCP/IP byte stream.

Note: currently developed and tested on Debian stable and Ubuntu (see the CI workflow).. other ports welcome (see below!)

## Why?
Because I've tried SoapyRemote, and it always has streaming errors due to network underrun/audio overrun,
which is daft when I have a solid 80Mbits/sec (10Mbytes/sec) from the source device to my laptop (measured
using a simple `scp` command).

I've also tried USB/IP, which is a total loss for anything faster than keyboards.

I've tried a network audio solution [TRX](http://www.pogo.org.uk/%7Emark/trx/streaming-desktop-audio.html),
which looks good with RTP for networking, but sadly due to having to switch between user and kernel
mode for every UDP packet at 500Hz+ thrashes my wee source device to kernel panic & death =(

## How?
By using the same approach as simple TCP/IP network protocols, use the capabilities of the underlying
TCP/IP stack which is reliable, efficiently and implemented in the kernel so I don't have problems
due to process/thread thrashing. Atop this, use `stdio` to manage buffering and data segmentation
(less code for me to write), and finally, don't aim for stupid low latency, choose buffer sizes that
provide ~250msecs delay, this again reduces thrashing on the source device (which is tiny =)).

The protocol is documented in [docs/PROTOCOL.md](docs/PROTOCOL.md).

## Building
You will need `libsoapysdr-dev`, `cmake` and `build-essential`

Then it's the usual CMake incantations (on both source and target devices!):
 * `cmake -B build`
 * `cmake --build build`
 * `su` or `sudo -s`
 * `cd build; make install`

After which you should be able to check the driver is installed with `SoapySDRUtil --info` and run
the server (on the device where your SDR is attached) `SoapyTCPServer`.

On a small device you can skip building the tests with `-DBUILD_TESTS=OFF`.

## Usage
 * Run the server on the target device: `SoapyTCPServer`
 * Connect from the client: `SoapySDRUtil --probe="driver=tcpremote,tcpremote:address=<serverIP>,tcpremote:driver=<serverSDR>"`
 * Once you have a working connection string, use in your favourite SDR package such as gqrx.

`tcpremote:address` is `host`, `host:port`, `[ipv6]:port` or a bare IPv6 address, and the port defaults to 20655.

### Server options
| option | meaning |
|--------|---------|
| `-l, --listen <host>` | address to listen on. **Default is `127.0.0.1` (this machine only).** Use `0.0.0.0` (IPv4) or `::` / `*` (everything) to serve other machines |
| `-p, --port <port>`   | port, default 20655 |
| `-b, --buffer-ms <ms>`| buffer about this many milliseconds of samples between the device and the network (default: ten stream MTUs, which is only a few ms on fast devices). `-b 250` gives the ~250ms described above, and rides out network hiccups |
| `-k, --key <secret>`, `-K, --key-file <file>` | require clients to present this secret (or set `SOAPY_TCPREMOTE_KEY`) |

> **Upgrading:** earlier versions listened on all interfaces by default and the host option was `-h`
> (still accepted, but deprecated). If clients on other machines can no longer connect, start the
> server with `-l 0.0.0.0`.

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
Instead of repeating connection arguments, you can put defaults in `SoapyTCPRemote.conf`, searched for in
`$XDG_CONFIG_DIRS`, `/etc/xdg` and `~/.config`: one `name=value` per line, `#` for comments, names
`address`, `driver`, `args` and `key`.

### Running at boot
There is a systemd unit in [systemd/](systemd/) (`cmake -DINSTALL_SYSTEMD=ON` installs the unit file; copy
`soapytcpserver.default` to `/etc/default/soapytcpserver` to configure it).

## Debugging
So it's not working first time? You can get significant details by setting the SoapySDR log level in the environment:
 * `SOAPY_SDR_LOG_LEVEL=<VALUE>` where `<VALUE>` is one of: `ERROR, WARNING, NOTICE, INFO (def), DEBUG, TRACE`

The server's log messages are also sent to the client's log, filtered by the client's level.

### Troubleshooting
 * *"unable to connect log stream" / "Failed to connect"*: is the server running, and listening on an address
   the client can reach? (`-l 0.0.0.0`). Is a firewall in the way of port 20655?
 * *Connection accepted, then immediately closed*: the server has a key and the client didn't send the right one.
 * *"overrun network pipe, data loss" on the server*: the network can't keep up with the sample rate. Lower the
   sample rate, and try a larger buffer (`-b 500`).
 * *Gaps or overflow messages on a healthy network*: increase `-b`.

## Limitations
 * RX only: the server does not implement TX streams yet.
 * Not implemented yet (the client reports "not supported"/empty): antennas, bandwidth, DC offset and IQ
   balance, clocking, time, sensors, registers, settings, GPIO, I2C, SPI and UART. Applications that insist on
   antenna or bandwidth controls may need these first. PRs welcome!
 * Streams use the formats `CS8`, `CS16` and `CF32`. When `CF32` is requested from a `CS16` device the
   conversion is done on the client, to halve the bytes on the wire.
 * Traffic is not encrypted (see Security).

## Testing
`ctest --test-dir build --output-on-failure` runs end to end loopback tests: real `SoapyTCPServer` processes
serve a small fake SDR (`tests/FakeDevice.cpp`, which generates a known ramp) to a client, checking
sample integrity over split reads and multiple channels, `CF32` conversion, authentication, and that
misbehaving clients cannot take the server down.

## Feedback
Feel free to raise issues for discussion or problems or features, better yet: submit PRs to fix my poor code please!
