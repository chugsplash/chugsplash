# Writing Deployment Scripts with Sphinx

This guide will describe how to define deployments with Sphinx.

Before continuing, please complete either the [quickstart guide](https://github.com/sphinx-labs/sphinx/blob/develop/docs/cli-quickstart.md) to setup a project in a new repository, or the guide to [integrate Sphinx into an existing repository](https://github.com/sphinx-labs/sphinx/blob/develop/docs/cli-existing-project.md).

## Table of Contents

- [Sample Sphinx Script](#sample-sphinx-script)
- [Required Configuration Options](#required-configuration-options)
- [Sphinx Deploy Function](#sphinx-deploy-function)
- [Deploying Contracts](#deploying-contracts)
- [Calling Contract Functions](#calling-contract-functions)
- [Owned Contracts](#owned-contracts)
- [Learn More](#learn-more)

## Sample Sphinx Script

A Sphinx deployment script has the following format:

```
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import { Script } from "sphinx-forge-std/Script.sol";
import { SphinxClient } from "../client/SphinxClient.sol";
import { Network } from "../contracts/foundry/SphinxPluginTypes.sol";

contract Sample is Script, SphinxClient {
    function setUp() public {
        // Required configuration options:
        sphinxConfig.projectName = "My Project";
        sphinxConfig.owners = [0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266];
        sphinxConfig.threshold = 1;

        // Sphinx DevOps platform options:
        sphinxConfig.proposers = [0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266];
        sphinxConfig.mainnets = [Network.ethereum, Network.arbitrum];
        sphinxConfig.testnets = [Network.goerli, Network.arbitrum_goerli];
        sphinxConfig.orgId = "<org id>";
    }

    function deploy(Network _network) public override sphinx(_network) {
        // Your deployment goes here:
        HelloSphinx helloSphinx = deployHelloSphinx("Hello!", 2);
        helloSphinx.add(1);
    }
}
```

You'll notice some differences between the sample script above and a vanilla Forge script. There are three main differences:

- There are a few configuration options that you must specify in your `setUp()` function.
- The entry point for the deployment is the `deploy(Network _network)` function defined above instead of a `run()` function.
- In your `deploy(Network _network)` function, you need to deploy your contracts using automatically generated `deployContract` functions which are implemented on the `SphinxClient` contract inherited by the script.

We'll go into detail on each of these below.

## Required Configuration Options
In the `setUp()` function, you'll assign values to a `sphinxConfig` struct to configure your project's settings. We'll go through its fields one by one.

### Project Name
```
sphinxConfig.projectName = "My Project";
```

TODO(md): answer the question: how are addresses generated with sphinx?

The `projectName` is the name of your project, and it can be any name you choose. It's case-sensitive.

### Owners
```
sphinxConfig.owners = [0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266];
```

The list of addresses that own this project. Owners can perform permissioned actions such as approving deployments. If you are deploying using the CLI, you are limited to a single owner address. To use multiple owners, you'll need to deploy using the Sphinx DevOps platform. We recommend that the owner accounts are hardware wallets.

### Threshold
```
sphinxConfig.threshold = 1;
```

The number of owner signatures required to approve a deployment. If you are deploying using the CLI, then this needs to be 1.

### DevOps Platform Options
If you are using the Sphinx DevOps platform, there are several additional options you'll need to configure. You can learn more about them in the [Sphinx DevOps Platform](https://github.com/sphinx-labs/sphinx/blob/develop/docs/writing-sphinx-scripts.md) guide.

## Sphinx Deploy Function

The entry point for Sphinx deployments must always be:

```sol
function deploy(Network _network) public override sphinx(_network) {
    ...
}
```

You must include the modifier `sphinx(_network)` shown above for the deployment to work properly.

You'll notice that the function has a `Network _network` argument. This is an enum that you can optionally use to customize your deployments on different networks. For example:

```sol
function deploy(Network _network) public override sphinx(_network) {
    if (_network == Network.ethereum) {
      ...
    } else if (_network == Network.optimism) {
      ...
    }
}
```

## Deploying Contracts
To deploy a contract using Sphinx, you'll use slightly different syntax compared to a standard deployment. Instead of using the `new` keyword (e.g. `new MyContract(...)`), you'll need to call a deployment function.

For example, say you have a contract called `HelloSphinx` that you'd normally deploy via `new HelloSphinx("Hello!", 2)`. Using Sphinx, you'd deploy this contract by calling the function:

```
deployHelloSphinx("Hello!", 2);
```

Sphinx autogenerates a deployment function like this for each of your contracts. These autogenerated functions exist in your `SphinxClient` contract, which is inherited by your script. There is one deployment function per contract.

You can generate your client using:
```
npx sphinx generate
```

We use this custom syntax because your contracts are deployed via `CREATE3`, which results in different addresses than the `new` keyword.

Typically, the deployment function for a contract will follow the format: `deploy<ContractName>`. If your repository contains more than one contract with the same name, Sphinx will resolve this ambiguity by incorporating the full path to the contract with the format: `deploy<PathToContract>_<ContractName>`. For example, say your repository contains more than one contract with the name `ERC20`. If one of these contracts is located at `src/tokens/MyTokens.sol`, then its deployment function would be called: `deploySrcTokensMyTokens_ERC20`.

### Contract Deployment Options

Sometimes, it may be necessary to configure additional options when deploying contracts using Sphinx. For example, you may want to use a custom salt to determine your contract's `CREATE3` address, or you may want to deploy multiple instances of a contract. You can do this by entering a `DeployOptions` struct as the last argument of the appropriate deployment function. The structure of the `DeployOptions` struct is:

```sol
struct DeployOptions {
    string referenceName;
    bytes32 salt;
}
```

The fields of the `DeployOptions` struct are explained in detail below. Note that changing either of these fields will result in your contract having a different Create3 address.

#### Reference Name

A string label for the contract. The reference name is displayed in the deployment preview, website UI, etc. By default, the reference name is the name of the contract being deployed. It determines a contract's address along with the `salt`.

We recommend specifying the reference name when you want to deploy multiple instances of the same contract in the same deployment. For example, if you want to deploy two instances of `MyContract`, where one is called "MyFirstContract" and the other is called "MySecondContract", you can write:

```sol
deployMyContract(..., DeployOptions({ referenceName: "MyFirstContract", salt: bytes32(0) }));
deployMyContract(..., DeployOptions({ referenceName: "MySecondContract", salt: bytes32(0) }));
```

#### Salt

A `bytes32` salt value. Along with the reference name, the `salt` determines a contract's `CREATE3` address. The salt is `bytes32(0)` by default. We recommend changing the salt when you need to re-deploy a contract to a new address. Example usage:

```sol
deployMyContract(..., DeployOptions({ referenceName: "MyContract", salt: bytes32(123) }));
```

## Calling Contract Functions

You can call contract functions using Sphinx with standard Forge syntax. For example, if you call a Sphinx deployment function to deploy one of you're contracts then we'll return an instance of your contract which you can interact with however you want.

```
function deploy(Network _network) public override sphinx(_network) {
  HelloSphinx helloSphinx = deployHelloSphinx("Hello!", 2);
  helloSphinx.add(1);
}
```

You can also interact with a contract that was not deployed with Sphinx using an interface or contract type like you normally would:
```
function deploy(Network _network) public override sphinx(_network) {
  Box box = Box(0x...);
  box.setValue(1);
}
```

## Owned Contracts

There are two things to keep in mind when deploying contracts that use an ownership mechanism such as OpenZeppelin's `AccessControl` or `Ownable`.

1. You must explicitly set the owner of your contract in its constructor. When doing this, you *must not* use `msg.sender`. This is because the `msg.sender` of each contract is a minimal `CREATE3` proxy that has no logic to execute transactions. This means that if the `msg.sender` owns your contracts, you won't be able to execute any permissioned functions or transfer ownership to a new address.
2. If you need to call permissioned functions on your contract after it's deployed, you must grant the appropriate role to your [`SphinxManager`](https://github.com/sphinx-labs/sphinx/blob/develop/docs/sphinx-manager.md), which is the contract that executes your deployment. See [this guide](https://github.com/sphinx-labs/sphinx/blob/develop/docs/permissioned-functions.md) for instructions on how to do that.

## Learn more
You should now be able to write scripts to deploy and interact with your contracts using Sphinx. If you have questions, please reach out in the [Discord](https://discord.gg/7Gc3DK33Np).

If you'd like to try the Sphinx DevOps Platform, which includes features such as gasless and multichain deployments, see [this guide](https://github.com/sphinx-labs/sphinx/blob/develop/docs/ci-foundry-proposals.md).