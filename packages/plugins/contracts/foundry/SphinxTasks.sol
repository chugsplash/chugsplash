// SPDX-License-Identifier: MIT
pragma solidity >=0.7.4 <0.9.0;

import { Vm } from "forge-std/Vm.sol";
import { console } from "forge-std/console.sol";
import { StdStyle } from "forge-std/StdStyle.sol";
import { Sphinx } from "./Sphinx.sol";
import {
    Configs,
    FoundryConfig,
    FoundryContractConfig,
    ContractKindEnum
} from "./SphinxPluginTypes.sol";
import { ISphinxRegistry } from "@sphinx-labs/contracts/contracts/interfaces/ISphinxRegistry.sol";
import { ISphinxManager } from "@sphinx-labs/contracts/contracts/interfaces/ISphinxManager.sol";
import { SphinxConstants } from "./SphinxConstants.sol";

contract SphinxTasks is Sphinx, SphinxConstants {
    Vm private constant vm = Vm(address(uint160(uint256(keccak256("hevm cheat code")))));

    // TODO: Test once we are officially supporting upgradable contracts
    function importProxy(
        string memory _configPath,
        address _proxy,
        string memory _rpcUrl
    ) internal noBroadcastOrPrank {
        initializeSphinx(_rpcUrl);
        address signer = utils.msgSender();

        Configs memory configs = ffiGetConfigs(_configPath, signer);

        ISphinxManager manager = ISphinxManager(payable(configs.minimalConfig.manager));

        require(address(manager) != address(0), "Sphinx: No project found");

        // check bytecode compatible with either UUPS or Transparent
        require(
            ffiCheckProxyBytecodeCompatible(_proxy.code),
            "Sphinx does not support your proxy type. Currently Sphinx only supports UUPS and Transparent proxies that implement EIP-1967 which yours does not appear to do. If you believe this is a mistake, please reach out to the developers or open an issue on GitHub."
        );

        // check if we can fetch the owner address from the expected slot
        // and that the caller is in fact the owner
        address ownerAddress = utils.getEIP1967ProxyAdminAddress(_proxy);

        address deployer = utils.msgSender();
        require(ownerAddress == deployer, "Sphinx: You are not the owner of this proxy.");

        // TODO: transfer ownership of the proxy
        // We need to use an interface here instead of importing the Proxy contract from Optimism b/c
        // it requires a specific solidity compiler version.
    }

    // TODO: Test once we are officially supporting upgradable contracts
    // We may need to do something more complex here to handle ensuring the proxy is fully
    // compatible with the users selected type.
    function ffiCheckProxyBytecodeCompatible(bytes memory bytecode) private returns (bool) {
        string[] memory cmds = new string[](5);
        cmds[0] = "npx";
        cmds[1] = "node";
        cmds[2] = mainFfiScriptPath;
        cmds[3] = "checkProxyBytecodeCompatible";
        cmds[4] = vm.toString(bytecode);

        bytes memory result = vm.ffi(cmds);
        return keccak256(result) == keccak256("true");
    }

    // TODO: Test once we are officially supporting upgradable contracts
    function exportProxy(
        string memory _configPath,
        string memory _referenceName,
        address _newOwner,
        string memory _rpcUrl
    ) internal noBroadcastOrPrank {
        initializeSphinx(_rpcUrl);
        address signer = utils.msgSender();

        Configs memory configs = ffiGetConfigs(_configPath, signer);
        FoundryConfig memory minimalConfig = configs.minimalConfig;

        ISphinxManager manager = ISphinxManager(payable(configs.minimalConfig.manager));

        require(address(manager) != address(0), "Sphinx: No project found for organization ID");

        FoundryContractConfig memory targetContractConfig;

        for (uint256 i = 0; i < minimalConfig.contracts.length; i++) {
            if (
                keccak256(abi.encodePacked(minimalConfig.contracts[i].referenceName)) ==
                keccak256(abi.encodePacked(_referenceName))
            ) {
                targetContractConfig = minimalConfig.contracts[i];
                break;
            }
        }

        bytes32 contractKindHash;
        if (targetContractConfig.kind == ContractKindEnum.INTERNAL_DEFAULT) {
            contractKindHash = defaultProxyTypeHash;
        } else if (targetContractConfig.kind == ContractKindEnum.OZ_TRANSPARENT) {
            contractKindHash = externalTransparentProxyTypeHash;
        } else if (targetContractConfig.kind == ContractKindEnum.OZ_OWNABLE_UUPS) {
            contractKindHash = ozUUPSOwnableProxyTypeHash;
        } else if (targetContractConfig.kind == ContractKindEnum.OZ_ACCESS_CONTROL_UUPS) {
            contractKindHash = ozUUPSAccessControlProxyTypeHash;
        } else if (targetContractConfig.kind == ContractKindEnum.EXTERNAL_DEFAULT) {
            contractKindHash = externalTransparentProxyTypeHash;
        } else if (targetContractConfig.kind == ContractKindEnum.IMMUTABLE) {
            revert("Cannot export a proxy for a contract that does not use a proxy.");
        } else {
            revert("Unknown contract kind.");
        }

        manager.exportProxy(payable(targetContractConfig.addr), contractKindHash, _newOwner);
    }

    function cancel(string memory _configPath, string memory _rpcUrl) internal noBroadcastOrPrank {
        initializeSphinx(_rpcUrl);
        address signer = utils.msgSender();

        Configs memory configs = ffiGetConfigs(_configPath, signer);

        ISphinxManager manager = ISphinxManager(payable(configs.minimalConfig.manager));

        manager.cancelActiveSphinxDeployment();
    }
}
