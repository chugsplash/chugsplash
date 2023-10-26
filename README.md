# Sphinx: DevOps for Deployments

Sphinx automates the smart contract deployment process by funding, executing, and verifying deployments on your behalf.

## Key features:

* **Compatible with Forge Scripts**: You can integrate Sphinx with minimal changes to your existing Forge scripts.

* **Gasless deployments**: You don't need to worry about securing a funded private key or getting native gas tokens on any chain. Instead, simply maintain a balance of USDC on a single chain to fund deployments across all chains.

* **Deployments in CI**: Initiating deployments from a CI process has obvious benefits such as reproducibility and consistency, but it hasn't been practical until now. With Sphinx, you can propose deployments from your CI process, then approve it in our UI (all gaslessly, of course). Then, Sphinx's backend will execute it on your behalf. If you'd prefer not to use a CI process, you can propose deployments from your local machine instead.

* **Automatic Etherscan verification**.

> Sphinx is not audited yet, so you should **always** check that your deployments were executed correctly.

[Request access to the DevOps platform.](https://sphinx.dev)

## Documentation

### Getting Started

- Foundry Plugin:
  - [Quickstart](https://github.com/sphinx-labs/sphinx/blob/develop/docs/cli-quickstart.md)
  - [Integrate Sphinx into an Existing Project](https://github.com/sphinx-labs/sphinx/blob/develop/docs/cli-existing-project.md)
- DevOps Platform:
  - [Getting Started with the DevOps Platform](https://github.com/sphinx-labs/sphinx/blob/develop/docs/ops-getting-started.md)
  - [Using Sphinx in CI](https://github.com/sphinx-labs/sphinx/blob/develop/docs/ci-proposals.md)

### References

- [Writing Deployment Scripts with Sphinx](https://github.com/sphinx-labs/sphinx/blob/develop/docs/writing-sphinx-scripts.md)
- [Troubleshooting Guide](https://github.com/sphinx-labs/sphinx/blob/develop/docs/troubleshooting-guide.md)
- [The `SphinxManager` Contract](https://github.com/sphinx-labs/sphinx/blob/develop/docs/sphinx-manager.md)
- [FAQ](https://github.com/sphinx-labs/sphinx/blob/develop/docs/faq.md)

## Supported Networks

- Ethereum
- Optimism
- Arbitrum
- Polygon
- Polygon zkEVM
- BNB Smart Chain (aka BSC)
- Gnosis Chain
- Avalanche C-Chain
- Linea
- Fantom
- Base

Test networks:

- Ethereum Goerli
- Optimism Goerli
- Arbitrum Goerli
- Polygon Mumbai
- Polygon zkEVM Testnet
- BNB Smart Chain Testnet
- Gnosis Chiado
- Avalanche Fuji
- Linea Goerli
- Fantom Testnet
- Base Goerli

More networks are on the way! Please feel free to reach out in our [Discord](https://discord.gg/7Gc3DK33Np) if there are networks you'd like us to add.

## Contributors

[@smartcontracts](https://github.com/smartcontracts)\
[Wonderland](https://defi.sucks/)\
[@rpate97](https://github.com/RPate97)\
[@sam-goldman](https://github.com/sam-goldman)

## Contributing

Contributions to Sphinx are greatly appreciated! To get started, please read our [contributing guide](https://github.com/sphinx-labs/sphinx/blob/develop/CONTRIBUTING.md). Then, check out the list of [Good First Issues](https://github.com/sphinx-labs/sphinx/contribute). Let us know if you have any questions!

## Reach Out

If you have any questions or feature requests, send us a message in our [Discord!](https://discord.gg/7Gc3DK33Np)

## License

MIT © 2023

