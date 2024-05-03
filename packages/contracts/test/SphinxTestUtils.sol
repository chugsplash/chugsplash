// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import { Vm, VmSafe } from "../contracts/forge-std/src/Vm.sol";
import {
    Network,
    NetworkInfo,
    FoundryDeploymentInfo,
    ParsedAccountAccess,
    SphinxLockProject
} from "../contracts/foundry/SphinxPluginTypes.sol";
import { StdCheatsSafe } from "../contracts/forge-std/src/StdCheats.sol";

import { SphinxConstants } from "../contracts/foundry/SphinxConstants.sol";
import { SphinxUtils } from "../contracts/foundry/SphinxUtils.sol";
import { IGnosisSafeProxyFactory } from
    "../contracts/foundry/interfaces/IGnosisSafeProxyFactory.sol";
import { IGnosisSafeProxy } from "../contracts/foundry/interfaces/IGnosisSafeProxy.sol";
import { IGnosisSafe } from "../contracts/foundry/interfaces/IGnosisSafe.sol";
import { SphinxInitCode, SystemContractInfo } from "./SphinxInitCode.sol";

/**
 * @notice Helper functions for testing the Sphinx plugin. This is separate from `SphinxUtils`
 *         because this file only contains helper functions for tests, whereas `SphinxUtils`
 *         contains helper functions for the plugin itself.
 */
contract SphinxTestUtils is SphinxConstants, StdCheatsSafe, SphinxUtils, SphinxInitCode {
    uint64 internal defaultCallDepth = 2;

    // Same as the `RawTx1559` struct defined in StdCheats.sol, except this struct has two
    // addditional fields: `additionalContracts` and `isFixedGasLimit`.
    struct AnvilBroadcastedTxn {
        address[] additionalContracts;
        string[] arguments;
        address contractAddress;
        string contractName;
        // Called 'function' in the JSON
        string functionSig;
        bytes32 hash;
        bool isFixedGasLimit;
        // Called 'transaction' in the JSON
        RawTx1559Detail txDetail;
        // Called 'transactionType' in the JSON
        string opcode;
    }

    Vm private constant vm = Vm(address(uint160(uint256(keccak256("hevm cheat code")))));

    /**
     * @notice The storage slot that holds the address of an EIP-1967 implementation.
     *         bytes32(uint256(keccak256('eip1967.proxy.implementation')) - 1)
     */
    bytes32 public constant EIP1967_IMPLEMENTATION_KEY =
        0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc;

    function readAnvilBroadcastedTxns(string memory _path)
        internal
        view
        returns (AnvilBroadcastedTxn[] memory)
    {
        string memory deployData = vm.readFile(_path);
        uint256 numTxns = vm.parseJsonStringArray(deployData, ".transactions").length;
        AnvilBroadcastedTxn[] memory txns = new AnvilBroadcastedTxn[](numTxns);
        for (uint256 i = 0; i < numTxns; i++) {
            txns[i] = readAnvilBroadcastedTxn(_path, i);
        }
        return txns;
    }

    function getNetwork(uint256 _chainId) public pure returns (Network) {
        NetworkInfo[] memory all = getNetworkInfoArray();
        for (uint256 i = 0; i < all.length; i++) {
            if (all[i].chainId == _chainId) {
                return all[i].network;
            }
        }
        revert(
            string(abi.encodePacked("No network found with the chain ID: ", vm.toString(_chainId)))
        );
    }

    function createSelectAlchemyFork(Network _network) internal {
        string memory alchemyAPIKey = vm.envString("ALCHEMY_API_KEY");

        string memory networkUrlStr;
        if (_network == Network.optimism) {
            networkUrlStr = "opt-mainnet";
        } else if (_network == Network.optimism_sepolia) {
            networkUrlStr = "opt-sepolia";
        } else if (_network == Network.arbitrum) {
            networkUrlStr = "arb-mainnet";
        } else if (_network == Network.arbitrum_sepolia) {
            networkUrlStr = "arb-sepolia";
        } else if (_network == Network.sepolia) {
            networkUrlStr = "eth-sepolia";
        } else if (_network == Network.ethereum) {
            networkUrlStr = "eth-mainnet";
        } else {
            revert("SphinxTestUtils: unknown network");
        }

        string memory rpcUrl =
            string(abi.encodePacked("https://", networkUrlStr, ".g.alchemy.com/v2/", alchemyAPIKey));
        vm.createSelectFork(rpcUrl);
    }

    /**
     * Converts input bytes to bytes32. This function will truncate the bytes if necessary.
     * Implicit conversion from bytes to bytes32 was added to Solidity, but we can't use it because
     * we compile with 0.8.4
     */
    function toBytes32(bytes memory data) public pure returns (bytes32) {
        require(data.length <= 32, "Data cannot be longer than 32 bytes");
        bytes32 converted;
        assembly {
            converted := mload(add(data, 32))
        }
        return converted;
    }

    /**
     * @notice Reads a transaction from the broadcast JSON generated by Forge when
     *         deploying on Anvil. Each field of the transaction is coerced to its
     *         proper type, unlike `StdJson`, which parses the entire data structure
     *         in one call. It's necessary to do it this way because of the way that
     *         Forge parses ambiguous fields. For example, if the first element of
     *         the `arguments` array is an address, then the code in `StdJson` will
     *         attempt to decode the array as an array of addresses instead of an array
     *         of strings. This is a byproduct of the way that Forge parses JSON. See
     *         here for more info:
     *         https://book.getfoundry.sh/cheatcodes/parse-json#json-encoding-rules
     *
     *         The other difference between this function and `StdJson` is that this
     *         function returns a struct that contains a couple extra fields that
     *         exist in the JSON for Anvil deployments: `isFixedGasLimit` and
     *         `additionalContracts`.
     */
    function readAnvilBroadcastedTxn(
        string memory _path,
        uint256 _index
    )
        internal
        view
        returns (AnvilBroadcastedTxn memory)
    {
        string memory deployData = vm.readFile(_path);
        string memory key = string(abi.encodePacked(".transactions[", vm.toString(_index), "]"));
        bytes32 hash = vm.parseJsonBytes32(deployData, string(abi.encodePacked(key, ".hash")));
        string memory opcode =
            vm.parseJsonString(deployData, string(abi.encodePacked(key, ".transactionType")));
        string memory contractName =
            vm.parseJsonString(deployData, string(abi.encodePacked(key, ".contractName")));
        string memory functionSig =
            vm.parseJsonString(deployData, string(abi.encodePacked(key, ".function")));

        // Parse the `arguments` array. Since this field may be `null` in the JSON, we can't use
        // `vm.parseJsonStringArray` right away. Instead, we must first check if the returned
        // `argumentsBytes` is a 32-byte array of zeros, which is how `null` is encoded.
        bytes memory argumentsBytes =
            vm.parseJson(deployData, string(abi.encodePacked(key, ".arguments")));
        string[] memory arguments = argumentsBytes.length == 32
            && toBytes32(argumentsBytes) == bytes32(0)
            ? new string[](0)
            : vm.parseJsonStringArray(deployData, string(abi.encodePacked(key, ".arguments")));

        RawTx1559Detail memory txDetail = abi.decode(
            vm.parseJson(deployData, string(abi.encodePacked(key, ".transaction"))),
            (RawTx1559Detail)
        );
        address[] memory additionalContracts = vm.parseJsonAddressArray(
            deployData, string(abi.encodePacked(key, ".additionalContracts"))
        );
        bool isFixedGasLimit =
            vm.parseJsonBool(deployData, string(abi.encodePacked(key, ".isFixedGasLimit")));
        return AnvilBroadcastedTxn({
            additionalContracts: additionalContracts,
            arguments: arguments,
            contractAddress: txDetail.to,
            contractName: contractName,
            functionSig: functionSig,
            hash: hash,
            isFixedGasLimit: isFixedGasLimit,
            txDetail: txDetail,
            opcode: opcode
        });
    }

    // Workaround for converting bytes memory to bytes calldata which is necessary to use index
    // slicing
    // If we call this with this.sliceBytes(bytes memory) then the input is converted to bytes
    // calldata
    // and properly sliced
    function sliceBytes(
        bytes calldata b,
        uint256 start,
        uint256 end
    )
        public
        pure
        returns (bytes memory)
    {
        return b[start:end];
    }

    /**
     * @notice Executes a single transaction that deploys a Gnosis Safe, deploys a Sphinx Module,
     *         and enables the Sphinx Module in the Gnosis Safe
     */
    function deploySphinxModuleAndGnosisSafe() public returns (IGnosisSafe) {
        IGnosisSafeProxyFactory safeProxyFactory = IGnosisSafeProxyFactory(safeFactoryAddress);

        (
            bytes memory safeInitializerData,
            SphinxLockProject memory project
        ) = getGnosisSafeInitializerData(address(this));

        // This is the transaction that deploys the Gnosis Safe, deploys the Sphinx Module,
        // and enables the Sphinx Module in the Gnosis Safe.
        IGnosisSafeProxy safeProxy = safeProxyFactory.createProxyWithNonce(
            safeSingletonAddress,
            safeInitializerData,
            project.defaultSafe.saltNonce
        );

        return IGnosisSafe(address(safeProxy));
    }

    /**
     * @notice Returns the stringified `AccountAccessKind`. Useful for debugging.
     */
    function accessKindToString(VmSafe.AccountAccessKind kind)
        public
        pure
        returns (string memory)
    {
        if (kind == VmSafe.AccountAccessKind.Call) return "Call";
        if (kind == VmSafe.AccountAccessKind.DelegateCall) return "DelegateCall";
        if (kind == VmSafe.AccountAccessKind.CallCode) return "CallCode";
        if (kind == VmSafe.AccountAccessKind.StaticCall) return "StaticCall";
        if (kind == VmSafe.AccountAccessKind.Create) return "Create";
        if (kind == VmSafe.AccountAccessKind.SelfDestruct) return "SelfDestruct";
        if (kind == VmSafe.AccountAccessKind.Resume) return "Resume";
        if (kind == VmSafe.AccountAccessKind.Balance) return "Balance";
        if (kind == VmSafe.AccountAccessKind.Extcodesize) return "Extcodesize";
        if (kind == VmSafe.AccountAccessKind.Extcodehash) return "Extcodehash";
        if (kind == VmSafe.AccountAccessKind.Extcodecopy) return "Extcodecopy";

        revert("Invalid AccountAccessKind");
    }

    /**
     * @notice Decodes and returns the ParsedAccountAccess array in the passed in
     * FoundryDeploymentInfo struct.
     */
    function decodeParsedAccountAcccesses(FoundryDeploymentInfo memory _deploymentInfo)
        public
        pure
        returns (ParsedAccountAccess[] memory)
    {
        ParsedAccountAccess[] memory parsedAccesses =
            new ParsedAccountAccess[](_deploymentInfo.encodedAccountAccesses.length);
        for (uint256 i = 0; i < parsedAccesses.length; i++) {
            parsedAccesses[i] =
                abi.decode(_deploymentInfo.encodedAccountAccesses[i], (ParsedAccountAccess));
        }

        return parsedAccesses;
    }
}
