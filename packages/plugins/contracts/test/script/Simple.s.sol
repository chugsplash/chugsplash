// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.0;

import { Script } from "sphinx-forge-std/Script.sol";
import { Network } from "../../../contracts/foundry/SphinxPluginTypes.sol";
import { MyContract1 } from "../../../contracts/test/MyContracts.sol";
import { Sphinx } from "../../foundry/Sphinx.sol";

contract Simple is Script, Sphinx {
    constructor() {
        sphinxConfig.projectName = "Simple Project";
        sphinxConfig.owners = [0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266];
        sphinxConfig.threshold = 1;

        sphinxConfig.proposers = [0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266];
        sphinxConfig.mainnets = [Network.ethereum, Network.optimism];
        sphinxConfig.testnets = [Network.goerli];
        sphinxConfig.orgId = "test-org-id";
    }

    function run() public override sphinx {
        MyContract1 myContract1;
        if (getSphinxNetwork(block.chainid) == Network.ethereum) {
            myContract1 = new MyContract1{ salt: 0 }(-1, 2, address(1), address(2));
        } else {
            myContract1 = new MyContract1{ salt: bytes32(uint(1)) }(-1, 2, address(1), address(2));
        }
        myContract1.incrementUint();
    }
}
