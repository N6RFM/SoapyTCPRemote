# SoapyTCPRemote wire protocol

The protocol is deliberately simple: **text lines over TCP** for control, and a **raw byte
stream** for samples. Everything is plain text/binary with no framing beyond newlines, so
it is easy to debug with `nc` or `tcpdump` and to implement in another language.

Default port: `20655` (`0x50AF`). A client opens several TCP connections to the server
(one control connection per device, one per stream, and one for logging).

## 1. Opening a connection

The first line sent on every connection identifies its purpose:

    <type>\n            or, when the server requires a key:      <type>:<key>\n

| type | name               | meaning                                              |
|------|--------------------|------------------------------------------------------|
| `0`  | `RPC_LOAD`         | control connection; loads a driver on the server     |
| `1`  | `LOG_STREAM`       | server log lines are sent to the client              |
| `2`  | `DATA_SEND`        | sample data, server -> client (an RX stream)         |
| `3`  | `DATA_RECV`        | sample data, client -> server (a TX stream; the server does not implement this yet) |

* The server must see this line within 5 seconds or it drops the connection.
* If the server was started with a key (`--key`), a missing or different key closes the
  connection immediately. If it has no key, any `:<key>` suffix is ignored.
* The key is sent in clear text: tunnel the port (SSH, VPN) on untrusted networks.

## 2. Control (RPC) connection

After the type line the client sends:

    <driver name>\n
    <driver args>\n          name=value pairs separated by '/', may be empty

and the server replies with a single integer line: its handle for this connection (>= 0), or
`-1` if the driver could not be created, after which it closes the connection.

### Calls

Each call is

    --\n                     separator (the server drops the connection if it is missing)
    <call number>\n          integer, see the enum in SoapyRPC.hpp (numbering starts at 10)
    <argument>\n ...         one argument per line

and the server answers with the call's results, one per line. A call that the server does not
know is answered with `-1000` and the connection is closed (the server cannot know how many
argument lines to skip). `TCPREMOTE_DROP_RPC` (1000) closes the device and connection, with no reply.

Value encodings (one per line):

| type            | encoding                                                        |
|-----------------|-----------------------------------------------------------------|
| integer         | `%d`                                                            |
| double          | `%.17g` (exact round trip; older versions sent `%f`, both parse the same) |
| string          | the text itself; no newlines allowed. Max 64 KiB.               |
| string list     | one string per line, terminated by an empty line                |
| kwargs          | `key=value` lines, terminated by a line containing just `=`     |

Calls follow the SoapySDR `Device` API one for one (frequency, gain, sample rate, ...). Calls the
client does not implement yet (antennas, bandwidth, sensors, ...) are simply not sent.

### Setting up a stream

The server must be able to associate a data connection with a stream, so the order is:

1. Client opens a new connection and sends `2` (RX) or `3` (TX). The server replies with one integer
   line, the **data id**.
2. Client makes the `SETUP_STREAM` call with: data id, direction, wire format
   (`CS8`/`CS16`/`CF32`), the channel list (one line, space separated), and stream kwargs. The reply is
   the data id on success or an error: `-1` no such data id, `-2` unknown format, `-3` no such channel,
   `-4` the device refused the stream.
3. `ACTIVATE_STREAM` (data id) starts the flow, `DEACTIVATE_STREAM` stops it, `CLOSE_STREAM`
   releases it.

Data connections that are never set up are closed by the server after 30 seconds.

## 3. Data connection

A bare byte stream of **frames**. A frame is one sample for every channel in the channel list, in
that order (interleaved). Frame size = channels x sample size:

| format | bytes per sample | layout |
|--------|------------------|--------|
| `CS8`  | 2                | int8 I, int8 Q |
| `CS16` | 4                | int16 I, int16 Q (host byte order) |
| `CF32` | 8                | float I, float Q (host byte order) |

TCP may split the stream anywhere, including in the middle of a frame: readers must buffer the tail.
There is no flow control beyond TCP: if the network cannot keep up the server logs
`overrun network pipe, data loss` and drops samples at the source.

## 4. Log connection

After the type line (`1`) the client sends one line with the minimum log level to receive (the
integer value of `SoapySDRLogLevel`). The server replies with an integer id line and then sends one line
per message:

    <level>:<message>\n

The client closes the log stream by writing a newline (or just closing the connection).
