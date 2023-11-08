// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import { console } from "forge-std/console.sol";
import "forge-std/Test.sol";
import { StdUtils } from "forge-std/StdUtils.sol";
import { SphinxModuleFactory } from "../contracts/SphinxModuleFactory.sol";
import { SphinxModule } from "../contracts/SphinxModule.sol";
import {
    GnosisSafeProxyFactory
} from "@gnosis.pm/safe-contracts/proxies/GnosisSafeProxyFactory.sol";
import { GnosisSafeProxy } from "@gnosis.pm/safe-contracts/proxies/GnosisSafeProxy.sol";
import { SimulateTxAccessor } from "@gnosis.pm/safe-contracts/accessors/SimulateTxAccessor.sol";
import {
    DefaultCallbackHandler
} from "@gnosis.pm/safe-contracts/handler/DefaultCallbackHandler.sol";
import {
    CompatibilityFallbackHandler
} from "@gnosis.pm/safe-contracts/handler/CompatibilityFallbackHandler.sol";
import { CreateCall } from "@gnosis.pm/safe-contracts/libraries/CreateCall.sol";
import { MultiSend } from "@gnosis.pm/safe-contracts/libraries/MultiSend.sol";
import { MultiSendCallOnly } from "@gnosis.pm/safe-contracts/libraries/MultiSendCallOnly.sol";
import { SignMessageLib } from "@gnosis.pm/safe-contracts/libraries/SignMessageLib.sol";
import { GnosisSafeL2 } from "@gnosis.pm/safe-contracts/GnosisSafeL2.sol";
import { GnosisSafe } from "@gnosis.pm/safe-contracts/GnosisSafe.sol";
import { Enum } from "@gnosis.pm/safe-contracts/common/Enum.sol";

contract SphinxModule_Test is Test, Enum {

    SphinxModule module;

    address[] owners = new address[](5);
    uint256 threshold = 3;

    function setUp() public {
        // Deploy all Gnosis Safe contracts
        new SimulateTxAccessor();
        GnosisSafeProxyFactory safeProxyFactory = new GnosisSafeProxyFactory();
        // Deploy handlers
        new DefaultCallbackHandler();
        CompatibilityFallbackHandler compatibilityFallbackHandler = new CompatibilityFallbackHandler();
        // Deploy libraries
        new CreateCall();
        MultiSend multiSend = new MultiSend();
        new MultiSendCallOnly();
        new SignMessageLib();
        // Deploy singletons
        new GnosisSafeL2();
        GnosisSafe gnosisSafeSingleton = new GnosisSafe();

        SphinxModuleFactory moduleFactory = new SphinxModuleFactory();

        owners[0] = address(0x1000);
        owners[1] = address(0x2000);
        owners[2] = address(0x3000);
        owners[3] = address(0x4000);
        owners[4] = address(0x5000);

        bytes memory encodedDeployModuleCall = abi.encodeWithSelector(moduleFactory.deploySphinxModuleFromSafe.selector, bytes32(0));
        bytes memory firstMultiSendData = abi.encodePacked(uint8(Operation.Call), moduleFactory, uint256(0), encodedDeployModuleCall.length, encodedDeployModuleCall);
        bytes memory encodedEnableModuleCall = abi.encodeWithSelector(moduleFactory.enableSphinxModule.selector, bytes32(0));
        bytes memory secondMultiSendData = abi.encodePacked(uint8(Operation.DelegateCall), moduleFactory, uint256(0), encodedEnableModuleCall.length, encodedEnableModuleCall);

        bytes memory multiSendData = abi.encodeWithSelector(multiSend.multiSend.selector, abi.encodePacked(firstMultiSendData, secondMultiSendData));

        bytes memory safeInitializerData = abi.encodePacked(
            gnosisSafeSingleton.setup.selector,
            abi.encode(
                owners,
                threshold,
                address(multiSend),
                multiSendData,
                address(compatibilityFallbackHandler),
                address(0),
                0,
                address(0)
            )
        );

        GnosisSafeProxy safeProxy = safeProxyFactory.createProxyWithNonce(
                address(gnosisSafeSingleton),
                safeInitializerData,
                0
            );
        GnosisSafe safe = GnosisSafe(payable(address(safeProxy)));

        module = SphinxModule(moduleFactory.computeSphinxModuleAddress(address(safe), bytes32(0)));
        console.log(safe.isModuleEnabled(address(module)));
    }

    function test_TODO_success() external {
    }
}