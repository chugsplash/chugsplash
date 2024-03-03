// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import { Script } from "sphinx-forge-std/Script.sol";
import { Network } from "@sphinx-labs/contracts/contracts/foundry/SphinxPluginTypes.sol";
import { Sphinx } from "@sphinx-labs/contracts/contracts/foundry/Sphinx.sol";
import { CREATE3 } from "solady/utils/CREATE3.sol";
import { ConstructorDeploysContract } from "../../../contracts/test/ConstructorDeploysContract.sol";
import { Fallback } from "../../../contracts/test/Fallback.sol";
import { MyContract2 } from "../MyContracts.sol";

contract Simple is Script, Sphinx {
    function configureSphinx() public override {
        sphinxConfig.projectName = "Deployment_Cases_Project";
        sphinxConfig.owners = [0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266];
        sphinxConfig.threshold = 1;

        sphinxConfig.orgId = "test-org-id";
    }

    function run() public sphinx {
        // Deploy a contract, then call a function on it, then deploy another contract. it's
        // important to order the transactions this way to test that the Gnosis Safe's nonce is
        // incremented as a contract instead of an EOA.
        MyContract2 createContractOne = new MyContract2();
        createContractOne.incrementMyContract2(1);
        new MyContract2();

        // Deploy with Create2
        Fallback fallbackCreate2 = new Fallback{ salt: 0 }(-1);
        // Perform low level call to fallback function
        (bool success, ) = address(fallbackCreate2).call("");
        if (!success) {
            revert("Low level call to fallback function failed");
        }

        // Deploy with Create3
        bytes memory fallbackInitCode = abi.encodePacked(
            type(Fallback).creationCode,
            abi.encode(-1)
        );
        Fallback fallbackContract = Fallback(CREATE3.deploy(bytes32(0), fallbackInitCode, 0));
        fallbackContract.set(1);

        // Deploys contract that deploys another contract in its constructor using create2.
        // The deployed contract is automatically labeled because it has a source file.
        new ConstructorDeploysContract{ salt: bytes32(uint(1)) }(1);

        // Deploys contract that deploys another contract in its constructor using create3.
        // Both the parent and child are labeled because they both have source files.
        bytes memory constructorDeploysContractInitCode = abi.encodePacked(
            type(ConstructorDeploysContract).creationCode,
            abi.encode(2)
        );
        CREATE3.deploy(bytes32(uint(1)), constructorDeploysContractInitCode, 0);
    }
}
