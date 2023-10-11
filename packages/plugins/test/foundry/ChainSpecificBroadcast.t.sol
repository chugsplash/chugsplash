// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import { AbstractChainSpecific_Test } from "./AbstractChainSpecific.t.sol";
import { Network } from "../../contracts/foundry/SphinxPluginTypes.sol";

contract BroadcastChainSpecificOptimismMainnet_Test is AbstractChainSpecific_Test {

    Network network = Network.optimism;

    function setUp() public {
        initializeBroadcastTests(network);
    }

    function testChainSpecificActionsExecuted() external override {
        assertOptimismMainnetActionsExecuted();
    }

    function testOtherNetworkActionsNotExecuted() external override {
        assertArbitrumMainnetNotExecuted();
        assertArbitrumGoerliNotExecuted();
    }

    function testBroadcastSuccess() external {
        assertBroadcastSuccess(6);
    }

    function testDeployWithCorrectConstructorArg() external {
        assertDeployWithCorrectConstructorArg(network);
    }

    function testSetFeeCorrectly() external {
        assertSetFeeCorrectly(network);
    }
}

contract BroadcastChainSpecificOptimismGoerli_Test is AbstractChainSpecific_Test {

    Network network = Network.optimism_goerli;

    function setUp() public {
        initializeBroadcastTests(network);
    }

    function testChainSpecificActionsExecuted() external override {
        assertOptimismGoerliActionsExecuted();
    }

    function testOtherNetworkActionsNotExecuted() external override {
        assertArbitrumMainnetNotExecuted();
        assertArbitrumGoerliNotExecuted();
    }

    function testBroadcastSuccess() external {
        assertBroadcastSuccess(6);
    }

    function testDeployWithCorrectConstructorArg() external {
        assertDeployWithCorrectConstructorArg(network);
    }

    function testSetFeeCorrectly() external {
        assertSetFeeCorrectly(network);
    }
}

contract BroadcastChainSpecificEthereum_Test is AbstractChainSpecific_Test {

    Network network = Network.ethereum;

    function setUp() public {
        initializeBroadcastTests(network);
    }

    // TODO(docs): nothing network-specific on this chain.
    function testChainSpecificActionsExecuted() external override {}

    function testOtherNetworkActionsNotExecuted() external override {
        assertArbitrumMainnetNotExecuted();
        assertArbitrumGoerliNotExecuted();

        assertEq(address(onlyOptimism).code.length, 0);
        assertEq(address(onlyOptimismGoerli).code.length, 0);
    }

    function testBroadcastSuccess() external {
        assertBroadcastSuccess(5);
    }

    function testDeployWithCorrectConstructorArg() external {
        assertDeployWithCorrectConstructorArg(network);
    }

    function testSetFeeCorrectly() external {
        assertSetFeeCorrectly(network);
    }
}

contract BroadcastChainSpecificGoerli_Test is AbstractChainSpecific_Test {

    Network network = Network.goerli;

    function setUp() public {
        initializeBroadcastTests(network);
    }

    // TODO(docs): nothing network-specific on this chain.
    function testChainSpecificActionsExecuted() external override {}

    function testOtherNetworkActionsNotExecuted() external override {
        assertArbitrumMainnetNotExecuted();
        assertArbitrumGoerliNotExecuted();

        assertEq(address(onlyOptimism).code.length, 0);
        assertEq(address(onlyOptimismGoerli).code.length, 0);
    }

    function testBroadcastSuccess() external {
        assertBroadcastSuccess(5);
    }

    function testDeployWithCorrectConstructorArg() external {
        assertDeployWithCorrectConstructorArg(network);
    }

    function testSetFeeCorrectly() external {
        assertSetFeeCorrectly(network);
    }
}

contract BroadcastChainSpecificArbitrum_Test is AbstractChainSpecific_Test {

    Network network = Network.arbitrum;

    function setUp() public {
        initializeBroadcastTests(network);
    }

    function testChainSpecificActionsExecuted() external override {
        assertArbitrumMainnetActionsExecuted();
    }

    function testOtherNetworkActionsNotExecuted() external override {
        assertArbitrumGoerliNotExecuted();

        assertEq(address(onlyOptimism).code.length, 0);
        assertEq(address(onlyOptimismGoerli).code.length, 0);
    }

    function testBroadcastSuccess() external {
        assertBroadcastSuccess(5);
    }

    function testDeployWithCorrectConstructorArg() external {
        assertDeployWithCorrectConstructorArg(network);
    }

    function testSetFeeCorrectly() external {
        assertSetFeeCorrectly(network);
    }
}

contract BroadcastChainSpecificArbitrumGoerli_Test is AbstractChainSpecific_Test {

    Network network = Network.arbitrum_goerli;

    function setUp() public {
        initializeBroadcastTests(network);
    }

    function testChainSpecificActionsExecuted() external override {
        assertArbitrumGoerliActionsExecuted();
    }

    function testOtherNetworkActionsNotExecuted() external override {
        assertArbitrumMainnetNotExecuted();

        assertEq(address(onlyOptimism).code.length, 0);
        assertEq(address(onlyOptimismGoerli).code.length, 0);
    }

    function testBroadcastSuccess() external {
        assertBroadcastSuccess(5);
    }

    function testDeployWithCorrectConstructorArg() external {
        assertDeployWithCorrectConstructorArg(network);
    }

    function testSetFeeCorrectly() external {
        assertSetFeeCorrectly(network);
    }
}
