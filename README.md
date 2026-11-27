<h1 align="left">Prysm: An Ethereum Consensus Implementation Written in Go</h1>

<div align="left">

[![Buildkite](https://badge.buildkite.com/b555891daf3614bae4284dcf365b2340cefc0089839526f096.svg?branch=develop)](https://buildkite.com/prysmatic-labs/prysm)
[![Build status](https://github.com/OffchainLabs/prysm/actions/workflows/go.yml/badge.svg?branch=develop)](https://github.com/OffchainLabs/prysm/actions/workflows/go.yml)
[![Consensus Spec Version](https://img.shields.io/badge/dynamic/regex?url=https%3A%2F%2Fraw.githubusercontent.com%2FOffchainLabs%2Fprysm%2Fdevelop%2FWORKSPACE&search=consensus_spec_version%20%3D%20%22%28%5B%5E%22%5D%2B%29%22&replace=%241&label=Consensus%20Spec%20Version&color=blue)](https://github.com/ethereum/consensus-specs/releases)
[![Discord](https://img.shields.io/discord/476244492043812875?logo=discord&label=Discord)](https://discord.gg/qEZK94mFXP)

</div>

---

## SEACAST on this branch

This branch adds SEACAST to Prysm's pq-devnet-0: the execution payload travels as erasure-coded chunks over a fork of
go-libp2p-pubsub v0.19.0. The fork lives in `third_party/go-libp2p-pubsub`, and `go.mod` replaces upstream
go-libp2p-pubsub with it. The protocol's core is a module of its own, which the fork imports:
[github.com/cskiraly/seacast-go](https://github.com/cskiraly/seacast-go), licensed LGPL-3.0-or-later.

Build and test this branch with Go:

```
go build ./cmd/beacon-chain ./cmd/validator
```

The SEACAST tests are the ones [`.github/workflows/seacast.yml`](.github/workflows/seacast.yml) runs. Bazel isn't
supported here: it would build upstream go-libp2p-pubsub rather than the fork, so a patch that `deps.bzl` applies
makes such a build fail. Upstream Prysm is removing Bazel as well.

---

## 📖 Overview

This is the core repository for Prysm, a [Golang](https://go.dev/) implementation of the [Ethereum Consensus](https://ethereum.org/en/developers/docs/consensus-mechanisms/#proof-of-stake) [specification](https://github.com/ethereum/consensus-specs), developed by [Offchain Labs](https://www.offchainlabs.com).

See the [Changelog](https://github.com/OffchainLabs/prysm/releases) for details of the latest releases and upcoming breaking changes.

---

## 🚀 Getting Started

A detailed set of installation and usage instructions as well as breakdowns of each individual component are available in the **[official documentation portal](https://prysm.offchainlabs.com/docs/)**.

💬 **Need help?** Join our **[Discord Community](https://discord.gg/qEZK94mFXP)** for support (this invite link never expires).

---

## 🏆 Staking on Mainnet

To participate in staking, you can join the **[official Ethereum launchpad](https://launchpad.ethereum.org)**. The launchpad is the **only recommended** way to become a validator on mainnet.

🔍 Explore validator rewards/penalties:

- **[beaconcha.in](https://beaconcha.in)**
- **[beaconscan](https://beaconscan.com)**

---

## 🤝 Contributing

### 🔥 Branches

Prysm maintains two permanent branches:

- **[`master`](https://github.com/OffchainLabs/prysm/tree/master)** - This points to the latest stable release. It is ideal for most users.
- **[`develop`](https://github.com/OffchainLabs/prysm/tree/develop)** - This is used for development and contains the latest PRs. Developers should base their PRs on this branch.

### 🛠 Contribution Guide

Want to get involved? Check out our **[Contribution Guide](https://prysm.offchainlabs.com/docs/contribute/contribution-guidelines/)** to learn more!

### 🤖 AI Coding Agents

Shared agent instructions live in [`.agents/AGENTS.md`](.agents/AGENTS.md). To use them, point your agent's config file at it from the repo root:

```sh
echo '@.agents/AGENTS.md' > ./AGENTS.md   # or ./CLAUDE.md
```

---

## 📜 License

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0.en.html)  

This project is licensed under the **GNU General Public License v3.0**.

---

## ⚖️ Legal Disclaimer

📜 [Terms of Use](/TERMS_OF_SERVICE.md)
