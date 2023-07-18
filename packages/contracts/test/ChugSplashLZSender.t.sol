// SPDX-License-Identifier: MIT
pragma solidity 0.8.15;

import { Test } from "forge-std/Test.sol";
import { ChugSplashLZSender } from "contracts/ChugSplashLZSender.sol";
import { LayerZeroFundingMessage } from "contracts/ChugSplashDataTypes.sol";
import { ChugSplashLZReceiver } from "contracts/ChugSplashLZReceiver.sol";
import {
    LZEndpointMock
} from "@layerzerolabs/solidity-examples/contracts/mocks/LZEndpointMock.sol";

contract ChugSplashLZSenderTest is Test {
    ChugSplashLZSender lzSender;
    ChugSplashLZReceiver receiverOne;
    ChugSplashLZReceiver receiverTwo;
    LZEndpointMock srcEndpoint;
    LZEndpointMock dstEndpointOne;
    LZEndpointMock dstEndpointTwo;
    address airdropReceiverOne = address(1);
    address airdropReceiverTwo = address(2);
    address airdropReceiverThree = address(3);
    address signer = address(4);
    uint airdropAmountOne = 0.08 ether;
    uint airdropAmountTwo = 0.16 ether;
    // In production, these are LayerZero chain IDs, not EVM chain IDs.
    uint16 dstLzChainIdOne = 1;
    uint16 destLzChainIdTwo = 2;
    uint16 srcLzChainId = uint16(3);

    function setUp() public {
        vm.deal(signer, 1 ether);

        srcEndpoint = new LZEndpointMock(srcLzChainId);
        dstEndpointOne = new LZEndpointMock(dstLzChainIdOne);
        dstEndpointTwo = new LZEndpointMock(destLzChainIdTwo);
        receiverOne = new ChugSplashLZReceiver(address(dstEndpointOne), msg.sender);
        receiverTwo = new ChugSplashLZReceiver(address(dstEndpointTwo), msg.sender);
    }

    function testDidReceiveFunds() public {
        assertEq(address(receiverOne).balance, 0);
        assertEq(address(receiverTwo).balance, 0);

        ChugSplashLZSender.DestinationChainInfo[]
            memory dstChainInfo = new ChugSplashLZSender.DestinationChainInfo[](2);
        dstChainInfo[0] = ChugSplashLZSender.DestinationChainInfo(
            dstLzChainIdOne,
            address(receiverOne)
        );
        dstChainInfo[1] = ChugSplashLZSender.DestinationChainInfo(
            destLzChainIdTwo,
            address(receiverTwo)
        );

        lzSender = new ChugSplashLZSender(address(srcEndpoint), dstChainInfo, msg.sender);

        // Add each destination endpoint to the source endpoint. We don't need to do this on live
        // networks.
        srcEndpoint.setDestLzEndpoint(address(receiverOne), address(dstEndpointOne));
        srcEndpoint.setDestLzEndpoint(address(receiverTwo), address(dstEndpointTwo));

        uint16 outboundProofType = 1;
        LayerZeroFundingMessage[] memory actions = new LayerZeroFundingMessage[](2);
        actions[0] = LayerZeroFundingMessage(
            dstLzChainIdOne,
            outboundProofType,
            200000,
            airdropReceiverOne,
            airdropAmountOne
        );
        actions[1] = LayerZeroFundingMessage(
            destLzChainIdTwo,
            outboundProofType,
            200000,
            airdropReceiverTwo,
            airdropAmountTwo
        );

        vm.prank(signer);
        lzSender.sendBatchFunds{ value: 1 ether }(actions);

        assertEq(
            address(airdropReceiverOne).balance,
            airdropAmountOne,
            "first airdrop receiver did not receive funds"
        );
        assertEq(
            address(airdropReceiverTwo).balance,
            airdropAmountTwo,
            "second airdrop receiver did not receive funds"
        );
        assertEq(address(lzSender).balance, 0, "LZSender contract should have no balance");
    }
}
