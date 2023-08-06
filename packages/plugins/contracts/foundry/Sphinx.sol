// SPDX-License-Identifier: MIT
pragma solidity >=0.7.4 <0.9.0;
pragma experimental ABIEncoderV2;

import { VmSafe, Vm } from "forge-std/Vm.sol";
import { console } from "forge-std/console.sol";

import { ISphinxRegistry } from "@sphinx-labs/contracts/contracts/interfaces/ISphinxRegistry.sol";
import { ISphinxManager } from "@sphinx-labs/contracts/contracts/interfaces/ISphinxManager.sol";
import { IOwnable } from "@sphinx-labs/contracts/contracts/interfaces/IOwnable.sol";
import {
    DeploymentState,
    Version,
    DeploymentStatus,
    BundledSphinxAction,
    RawSphinxAction,
    SphinxTarget,
    BundledSphinxTarget,
    SphinxActionBundle,
    SphinxTargetBundle
} from "@sphinx-labs/contracts/contracts/SphinxDataTypes.sol";
import {
    FoundryConfig,
    Configs,
    BundleInfo,
    FoundryContractConfig,
    ConfigCache,
    DeployContractCost,
    OptionalAddress
} from "./SphinxPluginTypes.sol";
import { ISphinxUtils } from "./interfaces/ISphinxUtils.sol";

abstract contract Sphinx {
    Vm private constant vm = Vm(address(uint160(uint256(keccak256("hevm cheat code")))));

    VmSafe.Log[] private executionLogs;
    bool private initialized;

    // Maps a Sphinx config path to a deployed contract's reference name to the deployed
    // contract's address.
    mapping(string => mapping(string => address)) private deployed;

    ISphinxUtils internal utils;

    // Get owner address
    uint private key = vm.envOr("SPHINX_INTERNAL__OWNER_PRIVATE_KEY", uint(0));
    address private systemOwnerAddress =
        key != 0 ? vm.rememberKey(key) : 0x226F14C3e19788934Ff37C653Cf5e24caD198341;

    string private rootPath =
        vm.envOr("DEV_FILE_PATH", string("./node_modules/@sphinx-labs/plugins/"));
    string private rootFfiPath = string(abi.encodePacked(rootPath, "dist/foundry/"));
    string internal mainFfiScriptPath = string(abi.encodePacked(rootFfiPath, "index.js"));

    modifier noBroadcastOrPrank() {
        (VmSafe.CallerMode callerMode, , ) = vm.readCallers();
        require(
            callerMode != VmSafe.CallerMode.Broadcast,
            "Cannot call Sphinx using vm.broadcast. Please use vm.startBroadcast instead."
        );
        require(
            callerMode != VmSafe.CallerMode.Prank,
            "Cannot call Sphinx using vm.prank. Please use vm.startPrank instead."
        );
        _;
    }

    /**
     * @notice This is the entry point for the Sphinx deploy command. It makes a few FFI calls to
     *         TypeScript logic that's shared with the Hardhat plugin. Note that this command must
     *         perform all read and write operations to the blockchain from within Solidity instead
     *         of using a provider object in TypeScript. Otherwise, an error will be thrown because
     *         we can't create a provider object for the in-process Anvil node from outside of
     *         Solidity.
     */
    function deploy(string memory _configPath, string memory _rpcUrl) public {
        OptionalAddress memory newOwner;
        newOwner.exists = false;
        deploy(_configPath, _rpcUrl, newOwner, false);
    }

    function deployVerbose(string memory _configPath, string memory _rpcUrl) internal {
        OptionalAddress memory newOwner;
        newOwner.exists = false;
        deploy(_configPath, _rpcUrl, newOwner, true);
    }

    function initializeSphinx(string memory _rpcUrl) internal {
        if (initialized) return;

        (VmSafe.CallerMode callerMode, address msgSender, ) = vm.readCallers();
        // Next, we deploy and initialize the Sphinx contracts. If we're in a recurrent broadcast or prank,
        // we temporarily stop it before we initialize the contracts. We disable broadcasting because
        // we can't call vm.etch from within a broadcast. We disable pranking because we need to prank
        // the owner of the Sphinx contracts when initializing the Sphinx contracts.
        if (callerMode == VmSafe.CallerMode.RecurrentBroadcast) vm.stopBroadcast();
        else if (callerMode == VmSafe.CallerMode.RecurrentPrank) vm.stopPrank();

        // Get the creation bytecode of the SphinxUtils contract. We load the creation code
        // directly from a JSON file instead of importing it into this contract because this
        // speeds up the compilation process of contracts that inherit from this contract.
        bytes memory utilsCreationCode = vm.getCode(
            string(abi.encodePacked(rootPath, "out/artifacts/SphinxUtils.sol/SphinxUtils.json"))
        );
        address utilsAddr;
        assembly {
            utilsAddr := create2(0, add(utilsCreationCode, 0x20), mload(utilsCreationCode), 0)
        }
        require(utilsAddr != address(0), "Sphinx: failed to deploy SphinxUtils contract");
        utils = ISphinxUtils(utilsAddr);

        (bool success, bytes memory retdata) = address(utils).delegatecall(
            abi.encodeWithSelector(
                ISphinxUtils.initialize.selector,
                _rpcUrl,
                callerMode == VmSafe.CallerMode.RecurrentBroadcast,
                mainFfiScriptPath,
                systemOwnerAddress
            )
        );
        require(success, string(utils.removeSelector(retdata)));
        // If we were in a recurrent broadcast or prank, we restart it.
        if (callerMode == VmSafe.CallerMode.RecurrentBroadcast) vm.startBroadcast(msgSender);
        else if (callerMode == VmSafe.CallerMode.RecurrentPrank) vm.startPrank(msgSender);

        initialized = true;
    }

    function deploy(
        string memory _configPath,
        string memory _rpcUrl,
        OptionalAddress memory _newOwner,
        bool _verbose
    ) private noBroadcastOrPrank {
        initializeSphinx(_rpcUrl);
        address owner = utils.msgSender();

        Configs memory configs = ffiGetConfigs(_configPath, owner);

        ISphinxRegistry registry = utils.getSphinxRegistry();
        ISphinxManager manager = ISphinxManager(payable(configs.minimalConfig.manager));

        (bool success, bytes memory retdata) = address(utils).delegatecall(
            abi.encodeWithSelector(
                ISphinxUtils.getConfigCache.selector,
                configs.minimalConfig,
                registry,
                manager,
                _rpcUrl,
                mainFfiScriptPath,
                executionLogs
            )
        );
        require(success, string(utils.removeSelector(retdata)));
        ConfigCache memory configCache = abi.decode(retdata, (ConfigCache));

        BundleInfo memory bundleInfo = getBundleInfo(configCache, configs.userConfigStr, owner);

        require(
            owner == configs.minimalConfig.owner,
            string(
                abi.encodePacked(
                    "The signer must match the 'owner' in the Sphinx config.\n",
                    "Signer: ",
                    vm.toString(owner),
                    "\n",
                    "Owner:",
                    vm.toString(configs.minimalConfig.owner)
                )
            )
        );

        // Claim the project with the signer as the owner. Once we've completed the deployment
        // we'll transfer ownership to the new owner specified by the user, if it exists.
        register(configs.minimalConfig.projectName, registry, manager, owner);

        if (
            bundleInfo.actionBundle.actions.length == 0 &&
            bundleInfo.targetBundle.targets.length == 0
        ) {
            // This string is used in the off-chain deploy task to detect whether or not a
            // deployment is empty. Make sure to update the deploy task if you change this log
            // message.
            console.log("Nothing to execute in this deployment. Exiting early.");
            return;
        }

        bytes32 deploymentId = utils.getDeploymentId(
            bundleInfo.actionBundle,
            bundleInfo.targetBundle,
            bundleInfo.configUri
        );

        DeploymentState memory deploymentState = manager.deployments(deploymentId);

        if (deploymentState.status == DeploymentStatus.CANCELLED) {
            revert(
                string(
                    abi.encodePacked(
                        configs.minimalConfig.projectName,
                        " was previously cancelled."
                    )
                )
            );
        }

        if (deploymentState.status == DeploymentStatus.EMPTY) {
            (uint256 numImmutableContracts, ) = utils.getNumActions(
                bundleInfo.actionBundle.actions
            );
            manager.approve{ gas: 1000000 }(
                bundleInfo.actionBundle.root,
                bundleInfo.targetBundle.root,
                bundleInfo.actionBundle.actions.length,
                bundleInfo.targetBundle.targets.length,
                numImmutableContracts,
                bundleInfo.configUri,
                false
            );

            deploymentState.status = DeploymentStatus.APPROVED;
        }

        if (
            deploymentState.status == DeploymentStatus.APPROVED ||
            deploymentState.status == DeploymentStatus.PROXIES_INITIATED
        ) {
            bool executionSuccess = executeDeployment(
                manager,
                bundleInfo.actionBundle,
                bundleInfo.targetBundle,
                configCache.blockGasLimit,
                bundleInfo.deployContractCosts
            );

            if (!executionSuccess) {
                revert(
                    string(
                        abi.encodePacked(
                            "Sphinx: failed to execute ",
                            configs.minimalConfig.projectName,
                            "likely because one of the user's contract constructors reverted."
                        )
                    )
                );
            }
        }

        if (_newOwner.exists) {
            transferProjectOwnership(manager, _newOwner.value, owner);
        }

        updateDeploymentMapping(_configPath, configs.minimalConfig.contracts);

        if (_verbose) {
            console.log("Success!");
            for (uint i = 0; i < configs.minimalConfig.contracts.length; i++) {
                FoundryContractConfig memory contractConfig = configs.minimalConfig.contracts[i];
                console.log(
                    string(
                        abi.encodePacked(
                            contractConfig.referenceName,
                            ": ",
                            vm.toString(contractConfig.addr)
                        )
                    )
                );
            }
        }
    }

    function getBundleInfo(
        ConfigCache memory _configCache,
        string memory _userConfigStr,
        address _owner
    ) private returns (BundleInfo memory) {
        (bool success, bytes memory retdata) = address(utils).delegatecall(
            abi.encodeWithSelector(
                ISphinxUtils.ffiGetEncodedBundleInfo.selector,
                _configCache,
                _userConfigStr,
                rootFfiPath,
                _owner
            )
        );
        require(success, string(utils.removeSelector(retdata)));
        bytes memory data = abi.decode(retdata, (bytes));
        return utils.decodeBundleInfo(data);
    }

    function register(
        string memory _projectName,
        ISphinxRegistry _registry,
        ISphinxManager _manager,
        address _newOwner
    ) private {
        if (!_registry.isManagerDeployed(address(_manager))) {
            _registry.register{ gas: 1000000 }(_newOwner, _projectName, new bytes(0));
        } else {
            address existingOwner = IOwnable(address(_manager)).owner();
            if (existingOwner != _newOwner) {
                revert(
                    string(
                        abi.encodePacked(
                            "Sphinx: project already owned by: ",
                            vm.toString(existingOwner)
                        )
                    )
                );
            }
        }
    }

    function transferProjectOwnership(
        ISphinxManager _manager,
        address _newOwner,
        address _currOwner
    ) private {
        if (_newOwner != _currOwner) {
            if (_newOwner == address(0)) {
                IOwnable(address(_manager)).renounceOwnership();
            } else {
                IOwnable(address(_manager)).transferOwnership(_newOwner);
            }
        }
    }

    function updateDeploymentMapping(
        string memory _configPath,
        FoundryContractConfig[] memory _contractConfigs
    ) private {
        for (uint i = 0; i < _contractConfigs.length; i++) {
            FoundryContractConfig memory contractConfig = _contractConfigs[i];
            require(
                deployed[_configPath][contractConfig.referenceName] == address(0),
                "Sphinx: Attempted to overwrite a contract that was already deployed. Should never happen."
            );
            deployed[_configPath][contractConfig.referenceName] = contractConfig.addr;
        }
    }

    // This function returns the user config string as a performance optimization. Reading
    // TypeScript user configs can be slow, so we read it once here and pass it in to
    // future FFI calls.
    function ffiGetConfigs(
        string memory _configPath,
        address _owner
    ) internal returns (Configs memory) {
        string memory ffiScriptPath = string(abi.encodePacked(rootFfiPath, "get-configs.js"));

        string[] memory cmds = new string[](6);
        cmds[0] = "npx";
        // We use ts-node here to support TypeScript Sphinx config files.
        cmds[1] = "ts-node";
        // Using SWC speeds up the process of transpiling TypeScript into JavaScript
        cmds[2] = "--swc";
        cmds[3] = ffiScriptPath;
        cmds[4] = _configPath;
        cmds[5] = vm.toString(_owner);

        bytes memory result = vm.ffi(cmds);

        // The success boolean is the last 32 bytes of the result.
        bytes memory successBytes = utils.slice(result, result.length - 32, result.length);
        bool success = abi.decode(successBytes, (bool));
        bytes memory data = utils.slice(result, 0, result.length - 32);

        if (success) {
            (FoundryConfig memory minimalConfig, string memory userConfigStr) = abi.decode(
                result,
                (FoundryConfig, string)
            );
            return Configs(minimalConfig, userConfigStr);
        } else {
            (string memory errors, ) = abi.decode(data, (string, string));
            revert(errors);
        }
    }

    function getAddress(
        string memory _configPath,
        string memory _referenceName
    ) public view returns (address) {
        address addr = deployed[_configPath][_referenceName];

        require(
            utils.getCodeSize(addr) > 0,
            string(
                abi.encodePacked(
                    "Sphinx: Could not find contract: ",
                    _referenceName,
                    " in ",
                    _configPath,
                    ". ",
                    "Did you misspell the contract's reference name or forget to deploy the config?"
                )
            )
        );

        return addr;
    }

    /**
     * Helper function for executing a list of actions in batches.
     */
    function executeBatchActions(
        BundledSphinxAction[] memory actions,
        ISphinxManager manager,
        uint bufferedGasLimit,
        DeployContractCost[] memory deployContractCosts
    ) private returns (DeploymentStatus) {
        // Pull the deployment state from the contract to make sure we're up to date
        bytes32 activeDeploymentId = manager.activeDeploymentId();
        DeploymentState memory state = manager.deployments(activeDeploymentId);
        // Filter out actions that have already been executed
        uint length = 0;
        for (uint i = 0; i < actions.length; i++) {
            BundledSphinxAction memory action = actions[i];
            if (state.actions[action.proof.actionIndex] == false) {
                length += 1;
            }
        }
        BundledSphinxAction[] memory filteredActions = new BundledSphinxAction[](length);
        uint filteredActionIndex = 0;
        for (uint i = 0; i < actions.length; i++) {
            BundledSphinxAction memory action = actions[i];
            if (state.actions[action.proof.actionIndex] == false) {
                filteredActions[filteredActionIndex] = action;
                filteredActionIndex += 1;
            }
        }

        // Exit early if there are no actions to execute
        if (filteredActions.length == 0) {
            return state.status;
        }

        uint executed = 0;
        while (executed < filteredActions.length) {
            // Figure out the maximum number of actions that can be executed in a single batch
            uint batchSize = utils.findMaxBatchSize(
                utils.inefficientSlice(filteredActions, executed, filteredActions.length),
                bufferedGasLimit - ((bufferedGasLimit) * 20) / 100,
                deployContractCosts
            );
            BundledSphinxAction[] memory batch = utils.inefficientSlice(
                filteredActions,
                executed,
                executed + batchSize
            );
            (
                RawSphinxAction[] memory rawActions,
                uint256[] memory _actionIndexes,
                bytes32[][] memory _proofs
            ) = utils.disassembleActions(batch);
            manager.executeActions{ gas: bufferedGasLimit }(rawActions, _actionIndexes, _proofs);

            // Return early if the deployment failed
            state = manager.deployments(activeDeploymentId);
            if (state.status == DeploymentStatus.FAILED) {
                return state.status;
            }

            // Move to next batch if necessary
            executed += batchSize;
        }

        // Return the final status
        return state.status;
    }

    function executeDeployment(
        ISphinxManager manager,
        SphinxActionBundle memory actionBundle,
        SphinxTargetBundle memory targetBundle,
        uint256 blockGasLimit,
        DeployContractCost[] memory deployContractCosts
    ) private returns (bool) {
        vm.recordLogs();

        // Get number of deploy contract and set state actions
        (
            BundledSphinxAction[] memory deployContractActions,
            BundledSphinxAction[] memory setStorageActions
        ) = utils.getActionsByType(actionBundle);

        uint bufferedGasLimit = ((blockGasLimit / 2) * 120) / 100;
        // Execute all the deploy contract actions and exit early if the deployment failed
        DeploymentStatus status = executeBatchActions(
            deployContractActions,
            manager,
            bufferedGasLimit,
            deployContractCosts
        );
        if (status == DeploymentStatus.FAILED) {
            return false;
        } else if (status == DeploymentStatus.COMPLETED) {
            return true;
        }

        // Dissemble the set storage actions
        SphinxTarget[] memory targets = new SphinxTarget[](targetBundle.targets.length);
        bytes32[][] memory proofs = new bytes32[][](targetBundle.targets.length);
        for (uint i = 0; i < targetBundle.targets.length; i++) {
            BundledSphinxTarget memory target = targetBundle.targets[i];
            targets[i] = target.target;
            proofs[i] = target.siblings;
        }

        // Start the upgrade
        manager.initiateUpgrade{ gas: 1000000 }(targets, proofs);

        // Execute all the set storage actions
        executeBatchActions(setStorageActions, manager, bufferedGasLimit, deployContractCosts);

        // Complete the upgrade
        manager.finalizeUpgrade{ gas: 1000000 }(targets, proofs);

        pushRecordedLogs();

        return true;
    }

    function pushRecordedLogs() private {
        VmSafe.Log[] memory logs = vm.getRecordedLogs();
        for (uint i = 0; i < logs.length; i++) {
            executionLogs.push(logs[i]);
        }
    }
}
