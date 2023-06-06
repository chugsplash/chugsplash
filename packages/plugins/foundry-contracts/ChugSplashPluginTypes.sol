// SPDX-License-Identifier: MIT
pragma solidity ^0.8.15;

struct ChugSplashContract {
    string referenceName;
    string contractName;
    address contractAddress;
}

struct DeploymentBytecode {
    bytes bootloaderOne;
    bytes bootloaderTwo;
}

struct MinimalParsedConfig {
    bytes32 organizationID;
    string projectName;
    MinimalParsedContractConfig[] contracts;
}

struct MinimalParsedContractConfig {
    string referenceName;
    bytes creationCodeWithConstructorArgs;
    address targetAddress;
    uint256 estDeployContractCost;
    ContractKindEnum kind;
    bytes32 salt;
}

struct ConfigCache {
    uint256 blockGasLimit;
    bool localNetwork;
    string networkName;
    ContractConfigCache[] contractConfigCache;
}

struct ContractConfigCache {
    string referenceName;
    bool isTargetDeployed;
    DeploymentRevert deploymentRevert;
    ImportCache importCache;
    OptionalBytes32 deployedCreationCodeWithArgsHash;
    OptionalBool isImplementationDeployed;
    OptionalString previousConfigUri;
}

struct DeploymentRevert {
    bool deploymentReverted;
    OptionalString revertString;
}

struct ImportCache {
    bool requiresImport;
    OptionalAddress currProxyAdmin;
}

enum ContractKindEnum {
    INTERNAL_DEFAULT,
    OZ_TRANSPARENT,
    OZ_OWNABLE_UUPS,
    OZ_ACCESS_CONTROL_UUPS,
    EXTERNAL_DEFAULT,
    NO_PROXY
}

enum ProposalRoute {
    RELAY,
    REMOTE_EXECUTION,
    LOCAL_EXECUTION
}

struct ConfigContractInfo {
    string referenceName;
    address contractAddress;
}

struct OptionalAddress {
    address value;
    bool exists;
}

struct OptionalBool {
    bool value;
    bool exists;
}

struct OptionalString {
    string value;
    bool exists;
}

struct OptionalBytes32 {
    bytes32 value;
    bool exists;
}


