# Via

[English](README.md) | [中文](docs/README.zh-CN.md)

[![CI](https://github.com/adrianceding/via/actions/workflows/ci.yml/badge.svg?branch=dev)](https://github.com/adrianceding/via/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Via is a multipath TCP relay for Linux. Applications connect through a local SOCKS5 proxy, while Via uses multiple egress interfaces such as Ethernet, Wi-Fi, and cellular links. Existing connections can continue over another path when one path slows down or disconnects.

> [!IMPORTANT]
> Via's code was written by AI. The project has extensive automated tests and has undergone manual functional testing, but the code has not received human review. Passing tests do not guarantee reliability. Review the code and assess the risks before using Via for important workloads or in production.

> [!WARNING]
> Traffic between the Via client and server currently uses plaintext TCP. It is not encrypted, the server's identity is not verified, and traffic integrity is not protected. Use Via only on a trusted network or through a controlled VPN. Do not expose it directly to the public internet as an encrypted tunnel.

## Features

- Discovers and uses multiple network interfaces, with optional name filters and dynamic interface changes.
- Supports adaptive and redundant delivery and can recover existing flows after path failures.
- Optional SOCKS5 username/password authentication; clients authenticate to the server with a PSK.
- Embedded read-only Web Manager for paths, sessions, flows, traffic, and connection status.
- Chinese and English Web Manager interface with a persistent language switcher.

Via currently supports TCP and SOCKS5 `CONNECT` only. It is not a VPN and does not support UDP.

## Docker Deployment

The recommended deployment uses the repository's [compose.yml](compose.yml), with the server and client running on separate Linux hosts. Clone the repository on both hosts:

```sh
git clone https://github.com/adrianceding/via.git
cd via
```

On the server host, create the configuration file:

```sh
cp examples/config/server.yml server.yml
```

On the client host:

```sh
cp examples/config/client.yml client.yml
```

Generate a new key with `openssl rand -base64 32`. Set the same value in the server's `principals[].psk` and the client's `psk`, then change the client's `transport.address` to the server address. Containers run as UID/GID `1000:1000` by default; set `VIA_UID` and `VIA_GID` when different IDs are required.

Start the server:

```sh
docker compose pull server
docker compose up -d server
```

Start the client:

```sh
docker compose pull client
docker compose up -d client
```

Applications can then use the SOCKS5 proxy at `127.0.0.1:1080`:

```sh
curl --socks5-hostname 127.0.0.1:1080 https://example.com/
```

## Configuration

Via uses strict YAML configuration. Unknown fields and invalid values stop startup. See the bilingual comments in the [server example](examples/config/server.yml) and [client example](examples/config/client.yml) for delivery modes, interface filters, SOCKS5 authentication, status access, and resource limits.

## Read-Only Manager

Enable `status` on either role, then open the configured `status.listen` address in a browser. The Manager and JSON API expose runtime status only and cannot modify configuration.

```yaml
status:
  enabled: true
  listen: "127.0.0.1:9090"
  basic_auth:
    username: "observer"
    password: "replace-with-a-strong-password"
```

When `basic_auth` is configured, the page, static assets, JSON API, and health endpoint all require credentials. Basic Auth over plain HTTP does not encrypt those credentials.

## Security Boundary

- Traffic between the Via client and server is not encrypted. SOCKS5 credentials and Basic Auth credentials sent over HTTP are also unencrypted.
- Use Via only on a trusted network or controlled VPN, and restrict access to transport and Manager ports with a firewall.
- Replace every example key and password before deployment.

## Development

Development requires Go 1.25 or newer, Node.js 22, and npm. Real network tests run on Linux only.

```sh
make production
make test-network
```

`make production` runs formatting, frontend and backend tests, race detection, static checks, and builds. `make test-network` validates real multipath failures with Linux `netns`, `veth`, and `tc netem`.

## License

Via is available under the [MIT License](LICENSE).
