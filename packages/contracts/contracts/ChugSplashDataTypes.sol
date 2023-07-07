// SPDX-License-Identifier: MIT
pragma solidity >=0.7.4 <0.9.0;

/**
 * @notice Struct representing the state of a deployment.
 *
 * @custom:field projectName The name of the project.
 * @custom:field status The status of the deployment.
 * @custom:field actions An array of booleans representing whether or not an action has been
   executed.
 * @custom:field targets The number of targets in the deployment.
 * @custom:field actionRoot The root of the Merkle tree of actions.
 * @custom:field targetRoot The root of the Merkle tree of targets.
 * @custom:field numImmutableContracts The number of non-proxy contracts in the deployment.
 * @custom:field actionsExecuted The number of actions that have been executed so far in the
   deployment.
 * @custom:field timeClaimed The time at which the deployment was claimed by a remote executor.
 * @custom:field selectedExecutor The address of the selected remote executor.
 * @custom:field remoteExecution Whether or not the deployment is being executed remotely.
 * @custom:field configUri URI pointing to the config file for the deployment.
 */
struct DeploymentState {
    string projectName;
    DeploymentStatus status;
    bool[] actions;
    uint256 targets;
    bytes32 actionRoot;
    bytes32 targetRoot;
    uint256 numImmutableContracts;
    uint256 actionsExecuted;
    uint256 timeClaimed;
    address selectedExecutor;
    bool remoteExecution;
    string configUri;
}

/**
 * @notice Struct representing a ChugSplash action.
 *
 * @custom:field actionType The type of action.
 * @custom:field data The ABI-encoded data associated with the action.
 * @custom:field addr The address of the contract to which the action applies.
 * @custom:field contractKindHash The hash of the contract kind associated with this contract.
 * @custom:field referenceName The reference name associated with the contract.
 */
struct RawChugSplashAction {
    ChugSplashActionType actionType;
    bytes data;
    address payable addr;
    bytes32 contractKindHash;
    string referenceName;
}

/**
 * @notice Struct representing a target.
 *
 * @custom:field addr The address of the proxy associated with this target.
 * @custom:field implementation The address that will be the proxy's implementation at the end of
   the deployment.
 * @custom:field contractKindHash The hash of the contract kind associated with this contract.
 */
struct ChugSplashTarget {
    address payable addr;
    address implementation;
    bytes32 contractKindHash;
}

/**
 * @notice Struct representing a LayerZero cross-chain message that includes sending
 *         funds to an address on the destination chain.
 *
 * @custom:field dstChainId The destination chain id.
 * @custom:field outboundProofType The endpoint outbound proof type.
 * @custom:field destGas The amount of gas to send to the destination chain for the `lzReceive`
 *               function.
 * @custom:field airdropAddress The address receiving the airdrop on the destination chain.
 * @custom:field airdropAmount The amount to airdrop on the destination chain.
 */
struct LayerZeroFundingMessage {
    uint16 dstChainId;
    uint16 outboundProofType;
    uint256 destGas;
    address airdropAddress;
    uint airdropAmount;
}

/**
 * @notice Struct representing a LayerZero cross-chain message.
 *
 * @custom:field dstChainId The destination chain id.
 * @custom:field outboundProofType The endpoint outbound proof type.
 * @custom:field destGas The amount of gas to send to the destination chain for the `lzReceive`
 *               function.
 * @custom:field payload The message payload to send to the receiver on the destination chain.
 */
struct LayerZeroMessage {
    uint16 dstChainId;
    uint16 outboundProofType;
    uint256 destGas;
    bytes payload;
}

/**
 * @notice Enum representing possible action types.
 *
 * @custom:value SET_STORAGE Set a storage slot value in a proxy contract.
 * @custom:value DEPLOY_CONTRACT Deploy a contract.
 */
enum ChugSplashActionType {
    SET_STORAGE,
    DEPLOY_CONTRACT
}

/**
 * @notice Enum representing the status of the deployment. These steps occur in sequential order,
   with the `CANCELLED` status being an exception.
 *
 * @custom:value EMPTY The deployment does not exist.
 * @custom:value APPROVED The deployment has been approved by the owner.
 * @custom:value PROXIES_INITIATED The proxies in the deployment have been initiated.
 * @custom:value COMPLETED The deployment has been completed.
 * @custom:value CANCELLED The deployment has been cancelled.
 * @custom:value FAILED The deployment has failed.
 */
enum DeploymentStatus {
    EMPTY,
    APPROVED,
    PROXIES_INITIATED,
    COMPLETED,
    CANCELLED,
    FAILED
}

/**
 * @notice Version number as a struct.
 *
 * @custom:field major Major version number.
 * @custom:field minor Minor version number.
 * @custom:field patch Patch version number.
 */
struct Version {
    uint256 major;
    uint256 minor;
    uint256 patch;
}

struct RegistrationInfo {
    Version version;
    address owner;
    bytes managerInitializerData;
}

struct ChugSplashBundles {
    ChugSplashActionBundle actionBundle;
    ChugSplashTargetBundle targetBundle;
}

struct ChugSplashActionBundle {
    bytes32 root;
    BundledChugSplashAction[] actions;
}

struct ChugSplashTargetBundle {
    bytes32 root;
    BundledChugSplashTarget[] targets;
}

struct BundledChugSplashAction {
    RawChugSplashAction action;
    ActionProof proof;
}

struct BundledChugSplashTarget {
    ChugSplashTarget target;
    bytes32[] siblings;
}

struct ActionProof {
    uint256 actionIndex;
    bytes32[] siblings;
}

struct ContractInfo {
    string referenceName;
    address addr;
}

/**
 * @notice Struct representing a leaf in an auth Merkle tree. This represents an arbitrary
   authenticated action taken by a permissioned account such as an organization owner or proposer.
 *
 * @custom:field chainId The chain ID for the leaf to be executed on.
 * @custom:field to The address that is the subject of the data in this leaf. This should always be
                 a ChugSplashManager.
 * @custom:field index The index of the leaf. Each index must be unique on a chain, and start from
                 zero. Leafs must be executed in ascending order according to their index. This
                 makes it possible to ensure that leafs in an Auth tree will be executed in a
                 certain order, e.g. creating a proposal then approving it.
 */
struct AuthLeaf {
    uint256 chainId;
    address to;
    uint256 index;
    bytes data;
}

/**
 * @notice Struct representing the state of an auth Merkle tree.
 *
 * @custom:field status The status of the auth Merkle tree.
 * @custom:field leafsExecuted The number of auth leafs that have been executed.
 * @custom:field numLeafs The total number of leafs in the auth Merkle tree on a chain.
 */
struct AuthState {
    AuthStatus status;
    uint256 leafsExecuted;
    uint256 numLeafs;
}

enum AuthStatus {
    EMPTY,
    SETUP,
    PROPOSED,
    COMPLETED
}

struct SetRoleMember {
    address member;
    bool add;
}
