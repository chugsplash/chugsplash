// SPDX-License-Identifier: MIT
pragma solidity ^0.8.15;

import {
    DeploymentState,
    RawSphinxAction,
    SphinxTarget,
    SphinxActionType,
    DeploymentStatus
} from "./SphinxDataTypes.sol";
import {
    OwnableUpgradeable
} from "@openzeppelin/contracts-upgradeable/access/OwnableUpgradeable.sol";
import { ISphinxRegistry } from "./interfaces/ISphinxRegistry.sol";
import { ISphinxManager } from "./interfaces/ISphinxManager.sol";
import { IProxyAdapter } from "./interfaces/IProxyAdapter.sol";
import {
    Lib_MerkleTree as MerkleTree
} from "@eth-optimism/contracts/libraries/utils/Lib_MerkleTree.sol";
import {
    ReentrancyGuardUpgradeable
} from "@openzeppelin/contracts-upgradeable/security/ReentrancyGuardUpgradeable.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";
import { ICreate3 } from "./interfaces/ICreate3.sol";
import { Semver, Version } from "./Semver.sol";
import {
    ContextUpgradeable
} from "@openzeppelin/contracts-upgradeable/utils/ContextUpgradeable.sol";
import { SphinxManagerEvents } from "./SphinxManagerEvents.sol";

/**
 * @title SphinxManager
 * @custom:version 1.0.0
 * @notice This contract contains the logic for managing the entire lifecycle of a project's
   deployments. It contains the functionality for approving and executing deployments and
   exporting proxies out of the Sphinx system if desired. It exists as a single implementation
   contract behind SphinxManagerProxy contracts, which are each owned by a single project team.

   After a deployment is approved, it is executed in the following steps, which must occur in order.
    1. Execute all of the `DEPLOY_CONTRACT` actions using the `executeActions` function. This is
       first because it's possible for the constructor of a deployed contract to revert. If this
       happens, we cancel the deployment before the proxies are modified in any way.
    2. The `initiateProxies` function.
    3. Execute all of the `SET_STORAGE` actions using the `executeActions` function.
    4. The `completeUpgrade` function.
 */
contract SphinxManager is
    OwnableUpgradeable,
    ReentrancyGuardUpgradeable,
    Semver,
    ISphinxManager,
    SphinxManagerEvents
{
    /**
     * @notice Role required to be a remote executor for a deployment.
     */
    bytes32 internal constant REMOTE_EXECUTOR_ROLE = keccak256("REMOTE_EXECUTOR_ROLE");

    /**
     * @notice The contract kind hash for immutable contracts. This does not include
     *         implementation contracts that exist behind proxies.
     */
    bytes32 internal constant IMMUTABLE_CONTRACT_KIND_HASH = keccak256("immutable");

    /**
     * @notice The contract kind hash for implementation contracts, which exist behind proxies.
     */
    bytes32 internal constant IMPLEMENTATION_CONTRACT_KIND_HASH = keccak256("implementation");

    /**
     * @notice Address of the SphinxRegistry.
     */
    ISphinxRegistry public immutable registry;

    string public projectName;

    /**
     * @notice Address of the Create3 contract.
     */
    address internal immutable create3;

    /**
     * @notice Address of the ManagedService contract.
     */
    IAccessControl internal immutable managedService;

    /**
     * @notice Amount of time for a remote executor to finish executing a deployment once they have
       claimed it.
     */
    uint256 internal immutable executionLockTime;

    /**
     * @notice Mapping of deployment IDs to deployment state.
     */
    mapping(bytes32 => DeploymentState) internal _deployments;

    /**
     * @notice ID of the currently active deployment.
     */
    bytes32 public activeDeploymentId;

    /**
     * @notice Reverts if the caller is not a remote executor.
     */
    error CallerIsNotRemoteExecutor();

    /**
     * @notice Reverts if the deployment state cannot be approved.
     */
    error DeploymentStateIsNotApprovable();

    /**
     * @notice Reverts if there is another active deployment ID.
     */
    error DeploymentInProgress();

    /**
     * @notice Reverts if there is currently no active deployment ID.
     */
    error NoActiveDeployment();

    /**
     * @notice Reverts if a deployment can only be self-executed by the owner.
     */
    error RemoteExecutionDisabled();

    /**
     * @notice Reverts if the deployment has already been claimed by another remote executor.
     */
    error DeploymentAlreadyClaimed();

    /**
     * @notice Reverts if there is no bytecode at a given address.
     */
    error ContractDoesNotExist();

    /**
     * @notice Reverts if an invalid contract kind is provided.
     */
    error InvalidContractKind();

    /**
     * @notice Reverts if the call to export ownership of a proxy from this contract fails.
     */
    error ProxyExportFailed();

    /**
     * @notice Reverts if an empty actions array is provided as input to the transaction.
     */
    error EmptyActionsArray();

    /**
     * @notice Reverts if the action has already been executed in this deployment.
     */
    error ActionAlreadyExecuted();

    /**
     * @notice Reverts if an invalid Merkle proof is provided.
     */
    error InvalidMerkleProof();

    /**
     * @notice Reverts if the action type is not `DEPLOY_CONTRACT` or `SET_STORAGE`.
     */
    error InvalidActionType();

    /**
     * @notice Reverts if an upgrade is initiated before all of the contracts are deployed via
       `executeActions`.
     */
    error InitiatedUpgradeTooEarly();

    /**
     * @notice Reverts if the deployment is not in the `APPROVED` state.
     */
    error DeploymentIsNotApproved();

    /**
     * @notice Reverts if the provided number of targets does not match the actual number of targets
       in the deployment.
     */
    error IncorrectNumberOfTargets();

    /**
     * @notice Reverts if a non-proxy contract type is used instead of a proxy type.
     */
    error OnlyProxiesAllowed();

    /**
     * @notice Reverts if the call to initiate an upgrade on a proxy fails.
     */
    error FailedToInitiateUpgrade();

    /**
     * @notice Reverts if an upgrade is completed before all of the actions have been executed.
     */
    error FinalizedUpgradeTooEarly();

    /**
     * @notice Reverts if the call to finalize an upgrade on a proxy fails.
     */
    error FailedToFinalizeUpgrade();

    /**
     * @notice Reverts if the deployment is not in the `PROXIES_INITIATED` state.
     */
    error ProxiesAreNotInitiated();

    /**
     * @notice Reverts if the call to modify a proxy's storage slot value fails.
     */
    error SetStorageFailed();

    /**
     * @notice Reverts if the caller is not a selected executor.
     */
    error CallerIsNotSelectedExecutor();

    /**
     * @notice Reverts if the caller is not the owner.
     */
    error CallerIsNotOwner();

    /**
     * @notice Reverts if the low-level delegatecall to get an address fails.
     */
    error FailedToGetAddress();

    error EmptyProjectName();
    error ProjectNameCannotBeEmpty();
    error InvalidAddress();

    /**
     * @notice Modifier that reverts if the caller is not a remote executor.
     */
    modifier onlyExecutor() {
        if (!managedService.hasRole(REMOTE_EXECUTOR_ROLE, msg.sender)) {
            revert CallerIsNotRemoteExecutor();
        }
        _;
    }

    /**
     * @param _registry                  Address of the SphinxRegistry.
     * @param _create3                   Address of the Create3 contract.
     * @param _managedService            Address of the ManagedService contract.
     * @param _executionLockTime         Amount of time for a remote executor to completely execute
       a deployment after claiming it.
     * @param _version                   Version of this contract.
     */
    constructor(
        ISphinxRegistry _registry,
        address _create3,
        IAccessControl _managedService,
        uint256 _executionLockTime,
        Version memory _version
    ) Semver(_version.major, _version.minor, _version.patch) {
        registry = _registry;
        create3 = _create3;
        managedService = _managedService;
        executionLockTime = _executionLockTime;

        _disableInitializers();
    }

    /**
     * @inheritdoc ISphinxManager

     * @return Empty bytes.
     */
    function initialize(
        address _owner,
        string memory _projectName,
        bytes memory
    ) external initializer returns (bytes memory) {
        if (bytes(_projectName).length == 0) revert EmptyProjectName();

        projectName = _projectName;

        __ReentrancyGuard_init();
        __Ownable_init();
        _transferOwnership(_owner);

        return "";
    }

    /**
     * @notice Approve a deployment. Only callable by the owner of this contract.
     *
     * @param _actionRoot Root of the Merkle tree containing the actions for the deployment.
     * This may be `bytes32(0)` if there are no actions in the deployment.
     * @param _targetRoot Root of the Merkle tree containing the targets for the deployment.
     * This may be `bytes32(0)` if there are no targets in the deployment.
     * @param _numImmutableContracts Number of non-proxy contracts in the deployment.
     * @param _numActions Number of actions in the deployment.
     * @param _numTargets Number of targets in the deployment.
     * @param _configUri  URI pointing to the config file for the deployment.
     * @param _remoteExecution Whether or not to allow remote execution of the deployment.
     */
    function approve(
        bytes32 _actionRoot,
        bytes32 _targetRoot,
        uint256 _numActions,
        uint256 _numTargets,
        uint256 _numImmutableContracts,
        string memory _configUri,
        bool _remoteExecution
    ) public onlyOwner {
        if (activeDeploymentId != bytes32(0)) {
            revert DeploymentInProgress();
        }

        // Compute the deployment ID.
        bytes32 deploymentId = keccak256(
            abi.encode(
                _actionRoot,
                _targetRoot,
                _numActions,
                _numTargets,
                _numImmutableContracts,
                _configUri
            )
        );

        DeploymentState storage deployment = _deployments[deploymentId];

        DeploymentStatus status = deployment.status;
        if (
            status != DeploymentStatus.EMPTY &&
            status != DeploymentStatus.COMPLETED &&
            status != DeploymentStatus.CANCELLED &&
            status != DeploymentStatus.FAILED
        ) {
            revert DeploymentStateIsNotApprovable();
        }

        activeDeploymentId = deploymentId;

        deployment.status = DeploymentStatus.APPROVED;
        deployment.actionRoot = _actionRoot;
        deployment.targetRoot = _targetRoot;
        deployment.numImmutableContracts = _numImmutableContracts;
        deployment.actions = new bool[](_numActions);
        deployment.targets = _numTargets;
        deployment.remoteExecution = _remoteExecution;
        deployment.configUri = _configUri;

        emit SphinxDeploymentApproved(
            deploymentId,
            _actionRoot,
            _targetRoot,
            _numActions,
            _numTargets,
            _numImmutableContracts,
            _configUri,
            _remoteExecution,
            msg.sender
        );
        registry.announceWithData("SphinxDeploymentApproved", abi.encodePacked(msg.sender));
    }

    /**
     * @notice Helper function that executes an entire upgrade in a single transaction. This allows
       the proxies in smaller upgrades to have zero downtime. This must occur after all of the
       `DEPLOY_CONTRACT` actions have been executed.

     * @param _targets Array of SphinxTarget structs containing the targets for the deployment.
     * @param _targetProofs Array of Merkle proofs for the targets.
     * @param _actions Array of RawSphinxAction structs containing the actions for the
     *                 deployment.
     * @param _actionIndexes Array of indexes into the actions array for each target.
     * @param _actionProofs Array of Merkle proofs for the actions.
     */
    function executeEntireUpgrade(
        SphinxTarget[] memory _targets,
        bytes32[][] memory _targetProofs,
        RawSphinxAction[] memory _actions,
        uint256[] memory _actionIndexes,
        bytes32[][] memory _actionProofs
    ) external {
        initiateUpgrade(_targets, _targetProofs);

        // Execute the `SET_STORAGE` actions if there are any.
        if (_actions.length > 0) {
            executeActions(_actions, _actionIndexes, _actionProofs);
        }

        finalizeUpgrade(_targets, _targetProofs);
    }

    /**
     * @notice **WARNING**: Cancellation is a potentially dangerous action and should not be
     *         executed unless in an emergency.
     *
     *         Allows the owner to cancel an active deployment that was approved.
     */
    function cancelActiveSphinxDeployment() external onlyOwner {
        if (activeDeploymentId == bytes32(0)) {
            revert NoActiveDeployment();
        }

        DeploymentState storage deployment = _deployments[activeDeploymentId];

        bytes32 cancelledDeploymentId = activeDeploymentId;
        activeDeploymentId = bytes32(0);
        deployment.status = DeploymentStatus.CANCELLED;

        emit SphinxDeploymentCancelled(
            cancelledDeploymentId,
            msg.sender,
            deployment.actionsExecuted
        );
        registry.announce("SphinxDeploymentCancelled");
    }

    /**
     * @notice Allows a remote executor to claim the sole right to execute a deployment over a
               period of `executionLockTime`. Executors must finish executing the deployment within
               `executionLockTime` or else another executor may claim the deployment.
     */
    function claimDeployment() external onlyExecutor {
        if (activeDeploymentId == bytes32(0)) {
            revert NoActiveDeployment();
        }

        DeploymentState storage deployment = _deployments[activeDeploymentId];

        if (!deployment.remoteExecution) {
            revert RemoteExecutionDisabled();
        }

        if (block.timestamp <= deployment.timeClaimed + executionLockTime) {
            revert DeploymentAlreadyClaimed();
        }

        deployment.timeClaimed = block.timestamp;
        deployment.selectedExecutor = msg.sender;

        emit SphinxDeploymentClaimed(activeDeploymentId, msg.sender);
        registry.announce("SphinxDeploymentClaimed");
    }

    /**
     * @notice Transfers ownership of a proxy away from this contract to a specified address. Only
       callable by the owner. Note that this function allows the owner to send ownership of their
       proxy to address(0), which would make their proxy non-upgradeable.
     *
     * @param _proxy  Address of the proxy to transfer ownership of.
     * @param _contractKindHash  Hash of the contract kind, which represents the proxy type.
     * @param _newOwner  Address of the owner to receive ownership of the proxy.
     */
    function exportProxy(
        address payable _proxy,
        bytes32 _contractKindHash,
        address _newOwner
    ) external onlyOwner {
        if (_proxy.code.length == 0) {
            revert ContractDoesNotExist();
        }

        if (activeDeploymentId != bytes32(0)) {
            revert DeploymentInProgress();
        }

        // Get the adapter that corresponds to this contract type.
        address adapter = registry.adapters(_contractKindHash);
        if (adapter == address(0)) {
            revert InvalidContractKind();
        }

        emit ProxyExported(_proxy, _contractKindHash, _newOwner);

        // Delegatecall the adapter to change ownership of the proxy.
        // slither-disable-next-line controlled-delegatecall
        (bool success, ) = adapter.delegatecall(
            abi.encodeCall(IProxyAdapter.changeProxyAdmin, (_proxy, _newOwner))
        );
        if (!success) {
            revert ProxyExportFailed();
        }

        registry.announce("ProxyExported");
    }

    function transferOwnership(address _newOwner) public override onlyOwner {
        if (_newOwner == address(0)) revert InvalidAddress();
        _transferOwnership(_newOwner);
        registry.announceWithData("OwnershipTransferred", abi.encodePacked(_newOwner));
    }

    function renounceOwnership() public override onlyOwner {
        _transferOwnership(address(0));
        registry.announceWithData("OwnershipTransferred", abi.encodePacked(address(0)));
    }

    /**
     * @notice Gets the DeploymentState struct for a given deployment ID. Note that we explicitly
     *         define this function because the getter function auto-generated by Solidity doesn't
               return
     *         array members of structs: https://github.com/ethereum/solidity/issues/12792. Without
     *         this function, we wouldn't be able to retrieve the full `DeploymentState.actions`
               array.
     *
     * @param _deploymentId Deployment ID.
     *
     * @return DeploymentState struct.
     */
    function deployments(bytes32 _deploymentId) external view returns (DeploymentState memory) {
        return _deployments[_deploymentId];
    }

    /**
     * @inheritdoc ISphinxManager
     */
    function isExecuting() external view returns (bool) {
        return activeDeploymentId != bytes32(0);
    }

    /**
     * @notice Deploys non-proxy contracts and sets proxy state variables. If the deployment does
       not contain any proxies, it will be completed after all of the non-proxy contracts have been
       deployed in this function.
     *
     * @param _actions Array of RawSphinxAction structs containing the actions for the
     *                 deployment.
     * @param _actionIndexes Array of action indexes.
     * @param _proofs Array of Merkle proofs for the actions.
     */
    function executeActions(
        RawSphinxAction[] memory _actions,
        uint256[] memory _actionIndexes,
        bytes32[][] memory _proofs
    ) public nonReentrant {
        DeploymentState storage deployment = _deployments[activeDeploymentId];

        _assertCallerIsOwnerOrSelectedExecutor(deployment.remoteExecution);

        uint256 numActions = _actions.length;

        // Prevents the executor from repeatedly sending an empty array of `_actions`, which would
        // cause the executor to be paid for doing nothing.
        if (numActions == 0) {
            revert EmptyActionsArray();
        }

        RawSphinxAction memory action;
        uint256 actionIndex;
        bytes32[] memory proof;
        for (uint256 i = 0; i < numActions; i++) {
            action = _actions[i];
            actionIndex = _actionIndexes[i];
            proof = _proofs[i];

            if (deployment.actions[actionIndex]) {
                revert ActionAlreadyExecuted();
            }

            if (
                !MerkleTree.verify(
                    deployment.actionRoot,
                    keccak256(
                        abi.encode(
                            action.referenceName,
                            action.addr,
                            action.actionType,
                            action.contractKindHash,
                            action.data
                        )
                    ),
                    actionIndex,
                    proof,
                    deployment.actions.length
                )
            ) {
                revert InvalidMerkleProof();
            }

            // Mark the action as executed and update the total number of executed actions.
            deployment.actionsExecuted++;
            deployment.actions[actionIndex] = true;

            if (action.actionType == SphinxActionType.DEPLOY_CONTRACT) {
                if (deployment.status != DeploymentStatus.APPROVED) {
                    revert DeploymentIsNotApproved();
                }

                _attemptContractDeployment(deployment, action, actionIndex);

                if (
                    deployment.actionsExecuted == deployment.actions.length &&
                    deployment.targets == 0 &&
                    deployment.status != DeploymentStatus.FAILED
                ) {
                    _completeDeployment(deployment);
                }
            } else if (action.actionType == SphinxActionType.SET_STORAGE) {
                _setProxyStorage(deployment, action, actionIndex);
            } else {
                revert InvalidActionType();
            }
        }
    }

    /**
     * @notice Initiate the proxies in an upgrade. This must be called after the contracts are
       deployment is approved, and before the rest of the execution process occurs. In this
       function, all of the proxies in the deployment are disabled by setting their implementations
       to a contract that can only be called by the team's SphinxManagerProxy. This must occur
       in a single transaction to make the process atomic, which means the proxies are upgraded as a
       single unit.

     * @param _targets Array of SphinxTarget structs containing the targets for the deployment.
     * @param _proofs Array of Merkle proofs for the targets.
     */
    function initiateUpgrade(
        SphinxTarget[] memory _targets,
        bytes32[][] memory _proofs
    ) public nonReentrant {
        DeploymentState storage deployment = _deployments[activeDeploymentId];

        _assertCallerIsOwnerOrSelectedExecutor(deployment.remoteExecution);

        if (deployment.actionsExecuted != deployment.numImmutableContracts) {
            revert InitiatedUpgradeTooEarly();
        }

        // Ensures that the deployment status isn't `FAILED`.
        if (deployment.status != DeploymentStatus.APPROVED) {
            revert DeploymentIsNotApproved();
        }

        uint256 numTargets = _targets.length;
        if (numTargets != deployment.targets) {
            revert IncorrectNumberOfTargets();
        }

        SphinxTarget memory target;
        bytes32[] memory proof;
        for (uint256 i = 0; i < numTargets; i++) {
            target = _targets[i];
            proof = _proofs[i];

            if (
                target.contractKindHash == IMMUTABLE_CONTRACT_KIND_HASH ||
                target.contractKindHash == IMPLEMENTATION_CONTRACT_KIND_HASH
            ) {
                revert OnlyProxiesAllowed();
            }

            if (
                !MerkleTree.verify(
                    deployment.targetRoot,
                    keccak256(
                        abi.encode(target.addr, target.implementation, target.contractKindHash)
                    ),
                    i,
                    proof,
                    deployment.targets
                )
            ) {
                revert InvalidMerkleProof();
            }

            address adapter = registry.adapters(target.contractKindHash);
            if (adapter == address(0)) {
                revert InvalidContractKind();
            }

            // Set the proxy's implementation to be a ProxyUpdater. Updaters ensure that only the
            // SphinxManager can interact with a proxy that is in the process of being updated.
            // Note that we use the Updater contract to provide a generic interface for updating a
            // variety of proxy types. Note no adapter is necessary for non-proxied contracts as
            // they are not upgradable and cannot have state.
            // slither-disable-next-line controlled-delegatecall
            (bool success, ) = adapter.delegatecall(
                abi.encodeCall(IProxyAdapter.initiateUpgrade, (target.addr))
            );
            if (!success) {
                revert FailedToInitiateUpgrade();
            }
        }

        // Mark the deployment as initiated.
        deployment.status = DeploymentStatus.PROXIES_INITIATED;

        emit ProxiesInitiated(activeDeploymentId, msg.sender);
        registry.announce("ProxiesInitiated");
    }

    /**
     * @notice Finalizes the upgrade by upgrading all proxies to their new implementations. This
     *         occurs in a single transaction to ensure that the upgrade is atomic.
     *
     * @param _targets Array of SphinxTarget structs containing the targets for the deployment.
     * @param _proofs Array of Merkle proofs for the targets.
     */
    function finalizeUpgrade(
        SphinxTarget[] memory _targets,
        bytes32[][] memory _proofs
    ) public nonReentrant {
        DeploymentState storage deployment = _deployments[activeDeploymentId];

        _assertCallerIsOwnerOrSelectedExecutor(deployment.remoteExecution);

        if (activeDeploymentId == bytes32(0)) {
            revert NoActiveDeployment();
        }

        if (deployment.actionsExecuted != deployment.actions.length) {
            revert FinalizedUpgradeTooEarly();
        }

        uint256 numTargets = _targets.length;
        if (numTargets != deployment.targets) {
            revert IncorrectNumberOfTargets();
        }

        SphinxTarget memory target;
        bytes32[] memory proof;
        for (uint256 i = 0; i < numTargets; i++) {
            target = _targets[i];
            proof = _proofs[i];

            if (
                target.contractKindHash == IMMUTABLE_CONTRACT_KIND_HASH ||
                target.contractKindHash == IMPLEMENTATION_CONTRACT_KIND_HASH
            ) {
                revert OnlyProxiesAllowed();
            }

            if (
                !MerkleTree.verify(
                    deployment.targetRoot,
                    keccak256(
                        abi.encode(target.addr, target.implementation, target.contractKindHash)
                    ),
                    i,
                    proof,
                    deployment.targets
                )
            ) {
                revert InvalidMerkleProof();
            }

            // Get the proxy type and adapter for this reference name.
            address adapter = registry.adapters(target.contractKindHash);
            if (adapter == address(0)) {
                revert InvalidContractKind();
            }

            // Upgrade the proxy's implementation contract.
            (bool success, ) = adapter.delegatecall(
                abi.encodeCall(IProxyAdapter.finalizeUpgrade, (target.addr, target.implementation))
            );
            if (!success) {
                revert FailedToFinalizeUpgrade();
            }

            emit ProxyUpgraded(activeDeploymentId, target.addr);
            registry.announceWithData("ProxyUpgraded", abi.encodePacked(target.addr));
        }

        _completeDeployment(deployment);
    }

    /**
     * @notice Queries the selected executor for a given project/deployment. This will return
       address(0) if the deployment is being self-executed by the owner.
     *
     * @param _deploymentId ID of the deployment to query.
     *
     * @return Address of the selected executor.
     */
    function getSelectedExecutor(bytes32 _deploymentId) public view returns (address) {
        DeploymentState storage deployment = _deployments[_deploymentId];
        return deployment.selectedExecutor;
    }

    /**
     * @notice Modifies a storage slot value within a proxy contract.
     *
     * @param _deployment The current deployment state struct.
     * @param _action The `SET_STORAGE` action to execute.
     * @param _actionIndex The index of the action.
     */
    function _setProxyStorage(
        DeploymentState memory _deployment,
        RawSphinxAction memory _action,
        uint256 _actionIndex
    ) internal {
        if (_deployment.status != DeploymentStatus.PROXIES_INITIATED) {
            revert ProxiesAreNotInitiated();
        }

        if (
            _action.contractKindHash == IMMUTABLE_CONTRACT_KIND_HASH ||
            _action.contractKindHash == IMPLEMENTATION_CONTRACT_KIND_HASH
        ) {
            revert OnlyProxiesAllowed();
        }

        // Get the adapter for this reference name.
        address adapter = registry.adapters(_action.contractKindHash);

        (bytes32 key, uint8 offset, bytes memory val) = abi.decode(
            _action.data,
            (bytes32, uint8, bytes)
        );
        // Delegatecall the adapter to call `setStorage` on the proxy.
        // slither-disable-next-line controlled-delegatecall
        (bool success, ) = adapter.delegatecall(
            abi.encodeCall(IProxyAdapter.setStorage, (_action.addr, key, offset, val))
        );
        if (!success) {
            revert SetStorageFailed();
        }

        emit SetProxyStorage(activeDeploymentId, _action.addr, msg.sender, _actionIndex);
        registry.announce("SetProxyStorage");
    }

    /**
     * @notice Attempts to deploy a non-proxy contract. The deployment will be skipped if a contract
     * already exists at the Create3 address. The entire deployment will be cancelled if the
       contract fails to be deployed, which should only occur if its constructor reverts.
     *
     * @param _deployment The current deployment state struct. The data location is "storage"
       because we
     * may modify one of the struct's fields.
     * @param _action The `DEPLOY_CONTRACT` action to execute.
     * @param _actionIndex The index of the action.
     */
    function _attemptContractDeployment(
        DeploymentState storage _deployment,
        RawSphinxAction memory _action,
        uint256 _actionIndex
    ) internal {
        (bytes32 salt, bytes memory creationCodeWithConstructorArgs) = abi.decode(
            _action.data,
            (bytes32, bytes)
        );

        string memory referenceName = _action.referenceName;
        address expectedAddress = _action.addr;

        // Check if the contract has already been deployed.
        if (expectedAddress.code.length > 0) {
            // Skip deploying the contract if it already exists. Execution would halt if we attempt
            // to deploy a contract that has already been deployed at the same address.
            emit ContractDeploymentSkipped(
                referenceName,
                expectedAddress,
                activeDeploymentId,
                referenceName,
                _actionIndex
            );
            registry.announce("ContractDeploymentSkipped");
        } else {
            // We delegatecall the Create3 contract so that the SphinxManager address is used in the
            // address calculation of the deployed contract. If we call the Create3 contract instead
            // of delegatecalling it, it'd be possible for an attacker to snipe a user's contract by
            // calling the `deploy` function on the Create3 contract directly.
            (bool deploySuccess, bytes memory actualAddressBytes) = create3.delegatecall(
                abi.encodeCall(ICreate3.deploy, (salt, creationCodeWithConstructorArgs, 0))
            );

            require(deploySuccess, string.concat("Failed to deploy: ", referenceName));

            address actualAddress = abi.decode(actualAddressBytes, (address));

            if (expectedAddress == actualAddress) {
                // Contract was deployed successfully.

                emit ContractDeployed(
                    referenceName,
                    actualAddress,
                    activeDeploymentId,
                    referenceName,
                    _action.contractKindHash,
                    keccak256(creationCodeWithConstructorArgs)
                );
                registry.announce("ContractDeployed");
            } else {
                // Contract deployment failed. Could happen if insufficient gas is supplied to this
                // transaction or if the creation bytecode has logic that causes the call to fail
                // (e.g. a constructor that reverts).

                emit DeploymentFailed(referenceName, activeDeploymentId, referenceName);
                registry.announceWithData("DeploymentFailed", abi.encodePacked(activeDeploymentId));

                activeDeploymentId = bytes32(0);
                _deployment.status = DeploymentStatus.FAILED;
            }
        }
    }

    /**
     * @notice Mark the deployment as completed and reset the active deployment ID.

     * @param _deployment The current deployment state struct. The data location is "s  rage"
       because we modify the struct.
     */
    function _completeDeployment(DeploymentState storage _deployment) internal {
        _deployment.status = DeploymentStatus.COMPLETED;

        emit SphinxDeploymentCompleted(activeDeploymentId, msg.sender);
        registry.announce("SphinxDeploymentCompleted");

        activeDeploymentId = bytes32(0);
    }

    /**
     * @notice If the deployment is being executed remotely, this function will check that the
     * caller is the selected executor. If the deployment is being executed locally, this function
     * will check that the caller is the owner. Throws an error otherwise.

       @param _remoteExecution True if the deployment is being executed remotely, otherwise false.

     */
    function _assertCallerIsOwnerOrSelectedExecutor(bool _remoteExecution) internal view {
        if (_remoteExecution == true && getSelectedExecutor(activeDeploymentId) != msg.sender) {
            revert CallerIsNotSelectedExecutor();
        } else if (_remoteExecution == false && owner() != msg.sender) {
            revert CallerIsNotOwner();
        }
    }
}
