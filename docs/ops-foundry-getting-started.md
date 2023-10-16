# Getting Started with the Sphinx DevOps Platform

This guide will walk you through a sample multi-chain deployment using the Sphinx Foundry plugin and DevOps platform.

If you're using Hardhat instead of Foundry, check out the [Hardhat version of this guide](https://github.com/sphinx-labs/sphinx/blob/develop/docs/ops-hardhat-getting-started.md).

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [High-level overview](#2-high-level-overview)
3. [Get testnet ETH on OP Goerli](#3-get-testnet-eth-on-op-goerli)
4. [Get your credentials](#4-get-your-credentials)
5. [Update your Sphinx config file](#5-update-your-sphinx-config-file)
6. [Add RPC endpoints](#6-add-rpc-endpoints)
7. [Propose the deployment](#7-propose-the-deployment)

## 1. Prerequisites

You'll need an EOA that exists on live networks.

Also, make sure that you've already completed one of the following guides:

* [Quickstart with Foundry](https://github.com/sphinx-labs/sphinx/blob/develop/docs/cli-foundry-quickstart.md)
* [Integrate Sphinx into an Existing Foundry Project](https://github.com/sphinx-labs/sphinx/blob/develop/docs/cli-foundry-existing-project.md)

## 2. High-level overview

To give some context on the deployment process, here's a high-level overview of how it works.

Deployments are a three-step process with the DevOps platform.

1. **Proposal**: The deployment is proposed on the command line or in a CI process. This creates a meta transaction that's signed by the proposer then relayed to Sphinx's back-end. For simplicity, we'll propose the deployment on the command line in this guide.
2. **Approval**: Each owner signs a meta transaction to approve the deployment in the Sphinx UI.
3. **Execution**: The deployment is trustlessly executed on-chain by a relayer. In order to execute the deployment, the relayer must submit the meta transactions signed by the proposer and the owners.

## 3. Get testnet ETH on OP Goerli

You'll need a small amount of testnet ETH on Optimism Goerli, which you can get at [their faucet](https://app.optimism.io/faucet). Later, you'll use this ETH to deploy a `SphinxBalance` contract. We require that you pay for the cost of your deployments by depositing USDC into this contract during execution. You'll need do go through this process on both test and production networks. On testnets we will provide you with testnet USDC to use. This contract only exists on Optimism Goerli (and Optimism Mainnet for production deployments), so deploying it is a one-time cost.

## 4. Get your credentials

You'll need a Sphinx API key and an organization ID. You can get these in the [Sphinx DevOps platform](https://www.sphinx.dev/).

## 5. Update your Sphinx config file

Navigate to the repository where you completed the Foundry getting started guide.

Enter your Sphinx API key in your `.env` file:
```
SPHINX_API_KEY=<your API key>
```

Then, open your Sphinx config file, which is in the `sphinx/` folder. We'll extend this config file to support
a multi-chain deployment, which will occur on several testnets.

We'll add the project settings in the `options` field. Copy and paste the `options` field below into your config file:

```ts
const config: UserSphinxConfig = {
  options: {
    orgId: <your org ID>,
    owners: [<your address>],
    proposers: [<your address>],
    testnets: ['goerli', 'optimism-goerli', 'arbitrum-goerli', 'maticmum', 'bnbt', 'gnosis-chiado'],
    mainnets: [],
    ownerThreshold: 1,
    managerVersion: "v0.2.5",
  },

  // The rest of the config file goes here:
  projectName: ...,
  contracts: ...,
}
```

Enter your address in the `owners` and `proposers` fields. Also, fill in the `orgId` field with your organization ID from the Sphinx UI. The `orgId` is a public field, so you don't need to keep it secret.

If you'd like to learn more about these fields, check out the [Sphinx config file reference](https://github.com/sphinx-labs/sphinx/blob/develop/docs/config-file.md).

## 6. Add RPC endpoints

If you don't already have an RPC endpoint for each testnet, you'll need to add them to your `foundry.toml` under `[rpc_endpoints]`. You can either use private RPC endpoints such as [Ankr](https://www.ankr.com/) or [Chainstack](https://chainstack.com/), or you can use these public RPC endpoints:

```toml
[rpc_endpoints]
goerli = "https://eth-goerli.g.alchemy.com/v2/demo"
optimism_goerli = "https://opt-goerli.g.alchemy.com/v2/demo"
arbitrum_goerli = "https://arb-goerli.g.alchemy.com/v2/demo"
bnb_smart_chain_testnet = "https://bsc-testnet.publicnode.com"
gnosis_chiado = "https://rpc.chiadochain.net"
polygon_mumbai = "https://polygon-mumbai.g.alchemy.com/v2/demo"
```

## 7. Propose the deployment

For simplicity, we'll propose the deployment on the command line in this guide. However, we recommend that you propose deployments in a CI process for production deployments. [Check out the CI integration guide for Foundry.](https://github.com/sphinx-labs/sphinx/blob/develop/docs/ci-foundry-proposals.md)

Add a `PROPOSER_PRIVATE_KEY` field to your `.env`:
```
PROPOSER_PRIVATE_KEY=<your private key>
```

Then, propose the deployment:

```
npx sphinx propose --testnets --config <path_to_sphinx_config>
```

Follow the instructions in the terminal to complete the deployment.
