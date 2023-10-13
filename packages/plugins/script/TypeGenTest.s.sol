// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import {
    SphinxConfig,
    Network,
    DeployOptions,
    DefineOptions
} from "@sphinx-labs/plugins/SphinxPluginTypes.sol";
import { SphinxClient } from "../client/SphinxClient.sol";
import { Version } from "@sphinx-labs/contracts/contracts/SphinxDataTypes.sol";
import {
    ConflictingNameContract as ConflictingNameContractFirst
} from "../contracts/test/typegen/conflictingNameContracts/First.sol";
import {
    ConflictingNameContract as ConflictingNameContractSecond
} from "../contracts/test/typegen/conflictingNameContracts/Second.sol";
import { BasicInputTypes } from "../contracts/test/typegen/BasicInputTypes.sol";
import { BasicInputTypesClient } from "../client/typegen/BasicInputTypes.c.sol";
import { ImmutableInputTypes } from "../contracts/test/typegen/ImmutableInputTypes.sol";
import { ArrayInputTypes } from "../contracts/test/typegen/ArrayInputTypes.sol";
import { ArrayInputTypesClient } from "../client/typegen/ArrayInputTypes.c.sol";
import {
    NoAliasImportsOne,
    NoAliasImportsTwo
} from "../contracts/test/typegen/imports/NoAlias.sol";
import { AliasImports } from "../contracts/test/typegen/imports/Alias.sol";
import { MyTypeLibrary } from "../contracts/test/typegen/imports/Types.sol";
import { MyTypeContract } from "../contracts/test/typegen/imports/Types.sol";
import {
    MyTopLevelType,
    MyTopLevelStruct,
    MyTopLevelEnum
} from "../contracts/test/typegen/imports/Types.sol";
import {
    MyLocalType,
    MyLocalStruct,
    MyLocalEnum
} from "../contracts/test/typegen/imports/NoAlias.sol";
import { MyTypeLibrary as MyTypeLibraryAlias } from "../contracts/test/typegen/imports/Types.sol";
import { MyTypeContract as MyTypeContractAlias } from "../contracts/test/typegen/imports/Types.sol";
import {
    MyTopLevelType as MyTopLevelTypeAlias,
    MyTopLevelStruct as MyTopLevelStructAlias,
    MyTopLevelEnum as MyTopLevelEnumAlias
} from "../contracts/test/typegen/imports/Types.sol";
import { LocalParentTypes } from "../contracts/test/typegen/imports/LocalParent.sol";
import {
    MyLocalTypeLibrary,
    MyLocalTypeContract
} from "../contracts/test/typegen/imports/LocalParent.sol";
import { FunctionContract } from "../contracts/test/typegen/contractInputs/FunctionContract.sol";
import { MyImportContract } from "../contracts/test/typegen/contractInputs/ImportContract.sol";
import { LocalContract } from "../contracts/test/typegen/contractInputs/FunctionContract.sol";
import {
    FunctionContractClient
} from "../client/typegen/contractInputs/FunctionContract.c.sol";
import { FunctionInputContract } from "../contracts/test/typegen/FunctionInputType.sol";
import { ExternalContract } from "../testExternalContracts/ExternalContract.sol";
import { ExternalContractClient } from "../client/ExternalContract.c.sol";
import {
    ConflictingTypeNameContractFirst
} from "../contracts/test/typegen/conflictingTypeNames/First.sol";
import {
    ConflictingTypeNameContractSecond
} from "../contracts/test/typegen/conflictingTypeNames/Second.sol";
import { ConflictingType } from "../contracts/test/typegen/conflictingTypeNames/First.sol";
import { ConflictingStruct } from "../contracts/test/typegen/conflictingTypeNames/First.sol";
import { ConflictingEnum } from "../contracts/test/typegen/conflictingTypeNames/First.sol";
import {
    ConflictingType as TypegenConflictingNameContractsSecond_ConflictingType
} from "../contracts/test/typegen/conflictingTypeNames/Second.sol";
import {
    ConflictingStruct as TypegenConflictingNameContractsSecond_ConflictingStruct
} from "../contracts/test/typegen/conflictingTypeNames/Second.sol";
import {
    ConflictingEnum as TypegenConflictingNameContractsSecond_ConflictingEnum
} from "../contracts/test/typegen/conflictingTypeNames/Second.sol";
import {
    ConflictingTypeNameContractFirst
} from "../contracts/test/typegen/conflictingTypeNames/First.sol";
import {
    ConflictingTypeNameContractSecond
} from "../contracts/test/typegen/conflictingTypeNames/Second.sol";
import {
    ConflictingTypeNameContractFirstClient
} from "../client/typegen/conflictingTypeNames/First.c.sol";
import { MsgSender } from "../contracts/test/MsgSender.sol";
import { MsgSenderClient } from "../client/MsgSender.c.sol";
import { UnnamedParameters } from "../contracts/test/typegen/UnnamedParameters.sol";
import {
    UnnamedParametersClient
} from "../client/typegen/UnnamedParameters.c.sol";
import { MyEnum, MyType, MyStruct } from "../contracts/test/typegen/ArrayInputTypes.sol";
import { NoAliasArrayImportsOne, NoAliasArrayImportsTwo } from "../contracts/test/typegen/imports/NoAliasArray.sol";
import { AliasImportsArray } from "../contracts/test/typegen/imports/AliasArray.sol";
import {
    MyLocalTypeArray,
    MyLocalStructArray,
    MyLocalEnumArray
} from "../contracts/test/typegen/imports/NoAliasArray.sol";
import { Child } from "../contracts/test/typegen/inheritance/Child.sol";
import { Grandchild } from "../contracts/test/typegen/inheritance/Alias.sol";
import { ChildClient } from "../client/typegen/inheritance/Child.c.sol";
import { GrandchildClient } from "../client/typegen/inheritance/Alias.c.sol";
import { ChildInSameFile } from "../contracts/test/typegen/inheritance/SameFile.sol";
import { ChildInSameFileClient } from "../client/typegen/inheritance/SameFile.c.sol";
import { ConflictingQualifiedNames } from "../contracts/test/typegen/conflictingQualifiedNames/ConflictingQualifiedNames.sol";
import { ConflictingQualifiedNames as ConflictingQualifiedNamesA } from "../contracts/test/typegen/conflictingQualifiedNames/A/ConflictingQualifiedNames.sol";
import { ConflictingQualifiedNameChild } from "../contracts/test/typegen/conflictingQualifiedNames/ConflictingNameChild.sol";
import { ConflictingQualifiedNameChildClient } from "../client/typegen/conflictingQualifiedNames/ConflictingNameChild.c.sol";
import { ConflictingQualifiedNameChildInSameFile } from "../contracts/test/typegen/conflictingQualifiedNames/ConflictingQualifiedNames.sol";
import { ConflictingQualifiedNameChildInSameFileClient } from "../client/typegen/conflictingQualifiedNames/ConflictingQualifiedNames.c.sol";
import { ChildParentImportsTypesClient } from "../client/typegen/imports/ChildParentImportsTypes.c.sol";
import { ChildParentImportsTypes } from "../contracts/test/typegen/imports/ChildParentImportsTypes.sol";
import { ChildOverrides } from "../contracts/test/typegen/inheritance/Overrides.sol";
import { ChildOverridesClient } from "../client/typegen/inheritance/Overrides.c.sol";

import "forge-std/Test.sol";

contract TypeGenTestConfig is Test, SphinxClient {
    ConflictingNameContractFirst firstConflictingNameContract;
    ConflictingNameContractSecond secondConflictingNameContract;
    BasicInputTypes basicInputTypes;
    BasicInputTypes basicInputTypesTwo;
    ImmutableInputTypes immutableInputTypes;
    ArrayInputTypes arrayInputTypes;
    ArrayInputTypes arrayInputTypesTwo;
    NoAliasImportsOne noAliasImportsOne;
    NoAliasImportsTwo noAliasImportsTwo;
    NoAliasArrayImportsOne noAliasArrayImportsOne;
    NoAliasArrayImportsTwo noAliasArrayImportsTwo;
    AliasImports aliasImports;
    AliasImportsArray aliasImportsArray;
    LocalParentTypes localParentTypes;
    FunctionContract functionContract;
    FunctionContract functionContractTwo;
    FunctionInputContract functionInputContract;
    ExternalContract externalContract;
    ExternalContract alreadyDeployedExternalContract;
    address alreadyDeployedContractAddress;
    ConflictingTypeNameContractFirst conflictingTypeNameContractFirst;
    ConflictingTypeNameContractSecond conflictingTypeNameContractSecond;
    ConflictingTypeNameContractFirst conflictingTypeNameContractFirstTwo;
    ConflictingTypeNameContractFirstClient conflictingTypeNameContractClient;
    MsgSender msgSender;
    UnnamedParameters unnamedParameters;
    Child child;
    Grandchild grandchild;
    ChildInSameFile childInSameFile;
    ConflictingQualifiedNames conflictingQualifiedNames;
    ConflictingQualifiedNamesA conflictingQualifiedNamesA;
    ConflictingQualifiedNameChild conflictingQualifiedNameChild;
    ConflictingQualifiedNameChildInSameFile conflictingQualifiedNameChildInSameFile;
    ChildParentImportsTypes childParentImportsTypes;
    ChildOverrides childOverrides;

    uint8[] public intialUintDynamicArray;
    bytes32[][] public initialUintNestedDynamicArray;
    address[3] public initialUintStaticArray;
    MyStruct[] public initialMyStructArray;
    MyType[] public initialMyTypeArray;
    address[] public initialMyContractTypeArray;
    MyEnum[] public initialMyEnumArray;

    uint8[] public updatedUintDynamicArray;
    bytes32[][] public updatedUintNestedDynamicArray;
    address[3] public updatedUintStaticArray;
    MyStruct[] public updatedMyStructArray;
    MyType[] public updatedMyTypeArray;
    address[] public updatedMyContractTypeArray;
    MyEnum[] public updatedMyEnumArray;

    MyTypeLibraryAlias.MyEnumInLibrary[] public libraryEnumArray;
    MyTypeLibraryAlias.MyStructInLibrary[] public libraryStruct;
    MyTypeLibraryAlias.MyTypeInLibrary[] public libraryType;
    MyTypeContractAlias.MyEnumInContract[] public contractEnum;
    MyTypeContractAlias.MyStructInContract[] public contractStruct;
    MyTypeContractAlias.MyTypeInContract[] public contractType;
    MyTopLevelEnumAlias[] public topLevelEnum;
    MyTopLevelStructAlias[] public topLevelStruct;
    MyTopLevelTypeAlias[] public topLevelType;

    MyTypeLibrary.MyEnumInLibrary[] public noAliasLibraryEnumArray;
    MyTypeLibrary.MyStructInLibrary[] public noAliasLibraryStruct;
    MyTypeLibrary.MyTypeInLibrary[] public noAliasLibraryType;
    MyTypeContract.MyEnumInContract[] public noAliasContractEnum;
    MyTypeContract.MyStructInContract[] public noAliasContractStruct;
    MyTypeContract.MyTypeInContract[] public noAliasContractType;

    MyTopLevelEnum[] public noAliasTopLevelEnum;
    MyTopLevelStruct[] public noAliasTopLevelStruct;
    MyTopLevelType[] public noAliasTopLevelType;
    MyLocalEnumArray[] public noAliasLocalEnum;
    MyLocalStructArray[] public noAliasLocalStruct;
    MyLocalTypeArray[] public noAliasLocalType;

    function setupVariables() internal {
        intialUintDynamicArray = new uint8[](2);
        intialUintDynamicArray[0] = 1;
        intialUintDynamicArray[1] = 2;
        initialUintNestedDynamicArray = new bytes32[][](2);
        initialUintNestedDynamicArray[0] = new bytes32[](2);
        initialUintNestedDynamicArray[0][0] = keccak256("3");
        initialUintNestedDynamicArray[0][1] = keccak256("4");
        initialUintNestedDynamicArray[1] = new bytes32[](2);
        initialUintNestedDynamicArray[1][0] = keccak256("5");
        initialUintNestedDynamicArray[1][1] = keccak256("6");
        initialUintStaticArray = [address(7), address(8), address(9)];
        initialMyStructArray.push(MyStruct({ myNumber: 10 }));
        initialMyStructArray.push(MyStruct({ myNumber: 11 }));
        initialMyTypeArray = new MyType[](2);
        initialMyTypeArray[0] = MyType.wrap(12);
        initialMyTypeArray[1] = MyType.wrap(13);
        initialMyContractTypeArray = new address[](2);
        initialMyContractTypeArray[0] = address(14);
        initialMyContractTypeArray[1] = address(15);
        initialMyEnumArray = new MyEnum[](2);
        initialMyEnumArray[0] = MyEnum.A;
        initialMyEnumArray[1] = MyEnum.B;

        updatedUintDynamicArray = new uint8[](2);
        updatedUintDynamicArray[0] = 10;
        updatedUintDynamicArray[1] = 11;
        updatedUintNestedDynamicArray = new bytes32[][](2);
        updatedUintNestedDynamicArray[0] = new bytes32[](2);
        updatedUintNestedDynamicArray[0][0] = keccak256("12");
        updatedUintNestedDynamicArray[0][1] = keccak256("13");
        updatedUintNestedDynamicArray[1] = new bytes32[](2);
        updatedUintNestedDynamicArray[1][0] = keccak256("14");
        updatedUintNestedDynamicArray[1][1] = keccak256("15");
        updatedUintStaticArray = [address(16), address(17), address(18)];
        updatedMyStructArray.push(MyStruct({ myNumber: 19 }));
        updatedMyStructArray.push(MyStruct({ myNumber: 20 }));
        updatedMyTypeArray = new MyType[](2);
        updatedMyTypeArray[0] = MyType.wrap(21);
        updatedMyTypeArray[1] = MyType.wrap(22);
        updatedMyContractTypeArray = new address[](2);
        updatedMyContractTypeArray[0] = address(23);
        updatedMyContractTypeArray[1] = address(24);
        updatedMyEnumArray = new MyEnum[](2);
        updatedMyEnumArray[0] = MyEnum.C;
        updatedMyEnumArray[1] = MyEnum.D;

        libraryEnumArray.push(MyTypeLibraryAlias.MyEnumInLibrary.Library);
        libraryStruct.push(MyTypeLibraryAlias.MyStructInLibrary({ a: 1 }));
        libraryType.push(MyTypeLibraryAlias.MyTypeInLibrary.wrap(3));
        contractEnum.push(MyTypeContractAlias.MyEnumInContract.Contract);
        contractStruct.push(MyTypeContractAlias.MyStructInContract({ a: keccak256("5") }));
        contractType.push(MyTypeContractAlias.MyTypeInContract.wrap(keccak256("7")));
        topLevelEnum.push(MyTopLevelEnumAlias.TopLevel);
        topLevelStruct.push(MyTopLevelStructAlias({ a: true }));
        topLevelType.push(MyTopLevelTypeAlias.wrap(true));

        noAliasLibraryEnumArray.push(MyTypeLibrary.MyEnumInLibrary.Library);
        noAliasLibraryStruct.push(MyTypeLibrary.MyStructInLibrary({ a: 1 }));
        noAliasLibraryType.push(MyTypeLibrary.MyTypeInLibrary.wrap(3));
        noAliasContractEnum.push(MyTypeContract.MyEnumInContract.Contract);
        noAliasContractStruct.push(MyTypeContract.MyStructInContract({ a: keccak256("5") }));
        noAliasContractType.push(MyTypeContract.MyTypeInContract.wrap(keccak256("7")));

        noAliasTopLevelEnum.push(MyTopLevelEnum.TopLevel);
        noAliasTopLevelStruct.push(MyTopLevelStruct({ a: true }));
        noAliasTopLevelType.push(MyTopLevelType.wrap(true));
        noAliasLocalEnum.push(MyLocalEnumArray.Local);
        noAliasLocalStruct.push(MyLocalStructArray({ a: -1 }));
        noAliasLocalType.push(MyLocalTypeArray.wrap(-2));
    }

    constructor() {
        sphinxConfig.projectName = "TypeGenTest";
        sphinxConfig.owners = [0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266];
        sphinxConfig.mainnets = [Network.ethereum];
        sphinxConfig.testnets = [Network.goerli];
        sphinxConfig.threshold = 1;
    }

    function deploy(Network _network) public override sphinx(_network) {
        setupVariables();

        // Deploy two contracts with conflicting names
        firstConflictingNameContract = ConflictingNameContractFirst(
            address(deployConflictingNameContract(5))
        );
        secondConflictingNameContract = ConflictingNameContractSecond(
            address(deployTypegenConflictingNameContractsSecond_ConflictingNameContract(address(5)))
        );

        // Deploy contract testing basic types
        basicInputTypes = BasicInputTypes(
            address(
                deployBasicInputTypes(
                    1,
                    2,
                    3,
                    4,
                    address(5),
                    keccak256("6"),
                    bytes("hello"),
                    true,
                    "world"
                )
            )
        );

        // Deploy contract with basic types, then call a function to update those values
        BasicInputTypesClient basicInputTypesTwoClient = deployBasicInputTypes(
            1,
            2,
            3,
            4,
            address(5),
            keccak256("6"),
            bytes("hello"),
            true,
            "world",
            DeployOptions({ salt: 0, referenceName: "basicInputTypesTwo" })
        );
        basicInputTypesTwoClient.setValues(
            2,
            3,
            4,
            5,
            address(6),
            keccak256("7"),
            bytes("goodbye"),
            false,
            "world"
        );
        basicInputTypesTwo = BasicInputTypes(address(basicInputTypesTwoClient));

        // Deploy contract with immutable input types
        immutableInputTypes = ImmutableInputTypes(
            address(deployImmutableInputTypes(1, 2, 3, 4, address(5), keccak256("6"), true))
        );

        // Deploy contract with array input types
        arrayInputTypes = ArrayInputTypes(
            address(
                deployArrayInputTypes(
                    intialUintDynamicArray,
                    initialUintNestedDynamicArray,
                    initialUintStaticArray,
                    initialMyStructArray,
                    initialMyTypeArray,
                    initialMyContractTypeArray,
                    initialMyEnumArray
                )
            )
        );

        // Deploy contract with array input types, then call function to update those values
        ArrayInputTypesClient arrayInputTypesTwoClient = deployArrayInputTypes(
            intialUintDynamicArray,
            initialUintNestedDynamicArray,
            initialUintStaticArray,
            initialMyStructArray,
            initialMyTypeArray,
            initialMyContractTypeArray,
            initialMyEnumArray,
            DeployOptions({ salt: 0, referenceName: "arrayInputTypesTwo" })
        );
        arrayInputTypesTwoClient.setValues(
            updatedUintDynamicArray,
            updatedUintNestedDynamicArray,
            updatedUintStaticArray,
            updatedMyStructArray,
            updatedMyTypeArray,
            updatedMyContractTypeArray,
            updatedMyEnumArray
        );
        arrayInputTypesTwo = ArrayInputTypes(address(arrayInputTypesTwoClient));

        // Deploy contracts which requires all types of imports without any aliasing
        noAliasImportsOne = NoAliasImportsOne(
            address(
                deployNoAliasImportsOne(
                    MyTypeLibrary.MyEnumInLibrary.Library,
                    MyTypeLibrary.MyStructInLibrary({ a: 1 }),
                    MyTypeLibrary.MyTypeInLibrary.wrap(2),
                    MyTypeContract.MyEnumInContract.Contract,
                    MyTypeContract.MyStructInContract({ a: keccak256("3") }),
                    MyTypeContract.MyTypeInContract.wrap(keccak256("4"))
                )
            )
        );

        noAliasImportsTwo = NoAliasImportsTwo(
            address(
                deployNoAliasImportsTwo(
                    MyTopLevelEnum.TopLevel,
                    MyTopLevelStruct({ a: true }),
                    MyTopLevelType.wrap(true),
                    MyLocalEnum.Local,
                    MyLocalStruct({ a: -1 }),
                    MyLocalType.wrap(-2)
                )
            )
        );

        // Deploy contract which requires all types of imports with aliasing
        aliasImports = AliasImports(
            address(
                deployAliasImports(
                    MyTypeLibraryAlias.MyEnumInLibrary.Library,
                    MyTypeLibraryAlias.MyStructInLibrary({ a: 1 }),
                    MyTypeLibraryAlias.MyTypeInLibrary.wrap(2),
                    MyTypeContractAlias.MyEnumInContract.Contract,
                    MyTypeContractAlias.MyStructInContract({ a: keccak256("3") }),
                    MyTypeContractAlias.MyTypeInContract.wrap(keccak256("4")),
                    MyTopLevelEnumAlias.TopLevel,
                    MyTopLevelStructAlias({ a: true }),
                    MyTopLevelTypeAlias.wrap(true)
                )
            )
        );

        // Deploy contract which requires all types imported from a locally defined parent object
        localParentTypes = LocalParentTypes(
            address(
                deployLocalParentTypes(
                    MyLocalTypeLibrary.MyEnumInLibrary.Library,
                    MyLocalTypeLibrary.MyStructInLibrary({ a: true }),
                    MyLocalTypeLibrary.MyTypeInLibrary.wrap(true),
                    MyLocalTypeContract.MyEnumInContract.Contract,
                    MyLocalTypeContract.MyStructInContract({ a: keccak256("1") }),
                    MyLocalTypeContract.MyTypeInContract.wrap(keccak256("2"))
                )
            )
        );

        // Deploy contract which requires types imported with aliasing and used in arrays
        aliasImportsArray = AliasImportsArray(
            address(
                deployAliasImportsArray(
                    libraryEnumArray,
                    libraryStruct,
                    libraryType,
                    contractEnum,
                    contractStruct,
                    contractType,
                    topLevelEnum,
                    topLevelStruct,
                    topLevelType
                )
            )
        );

        // Deploy contracts which requires types imported without aliasing and used in arrays
        noAliasArrayImportsOne = NoAliasArrayImportsOne(
            address(
                deployNoAliasArrayImportsOne(
                    noAliasLibraryEnumArray,
                    noAliasLibraryStruct,
                    noAliasLibraryType,
                    noAliasContractEnum,
                    noAliasContractStruct,
                    noAliasContractType
                )
            )
        );
        noAliasArrayImportsTwo = NoAliasArrayImportsTwo(
            address(
                deployNoAliasArrayImportsTwo(
                    noAliasTopLevelEnum,
                    noAliasTopLevelStruct,
                    noAliasTopLevelType,
                    noAliasLocalEnum,
                    noAliasLocalStruct,
                    noAliasLocalType
                )
            )
        );

        // Deploy contracts to be used as input
        address myImportContractOne = address(deployMyImportContract(1));
        address localContractOne = address(deployLocalContract(1));

        address myImportContractTwo = address(
            deployMyImportContract(
                2,
                DeployOptions({ salt: 0, referenceName: "myImportContractTwo" })
            )
        );
        address localContractTwo = address(
            deployLocalContract(2, DeployOptions({ salt: 0, referenceName: "localContractTwo" }))
        );

        // Deploy contract which requires contract inputs
        functionContract = FunctionContract(
            address(deployFunctionContract(myImportContractOne, localContractOne))
        );

        // Deploy contract which requires contract inputs, then call functions to update those values
        FunctionContractClient functionContractClient = deployFunctionContract(
            myImportContractOne,
            localContractOne,
            DeployOptions({ salt: 0, referenceName: "functionContractTwo" })
        );
        functionContractClient.setImportContract(myImportContractTwo);
        functionContractClient.setLocalContract(localContractTwo);
        functionContractTwo = FunctionContract(address(functionContractClient));

        // Deploy contract which has function inputs
        functionInputContract = FunctionInputContract(address(deployFunctionInputContract()));

        // Deploy external contract
        ExternalContractClient externalContractClient = deployExternalContract(5);
        externalContractClient.setNumber(6);
        externalContract = ExternalContract(address(externalContractClient));

        // Define external contract and interact with it
        ExternalContractClient alreadyDeployedExternalContractClient = defineExternalContract(
            alreadyDeployedContractAddress,
            DefineOptions({ referenceName: "MyExternalContract" })
        );
        alreadyDeployedExternalContractClient.setNumber(7);
        alreadyDeployedExternalContract = ExternalContract(
            address(alreadyDeployedExternalContractClient)
        );

        // Deploy contracts with conflicting type names
        conflictingTypeNameContractFirst = ConflictingTypeNameContractFirst(
            address(
                deployConflictingTypeNameContractFirst(
                    ConflictingType.wrap(true),
                    ConflictingStruct({ a: true }),
                    ConflictingEnum.Third
                )
            )
        );

        conflictingTypeNameContractSecond = ConflictingTypeNameContractSecond(
            address(
                deployConflictingTypeNameContractSecond(
                    TypegenConflictingNameContractsSecond_ConflictingType.wrap(1),
                    TypegenConflictingNameContractsSecond_ConflictingStruct({ a: 1 }),
                    TypegenConflictingNameContractsSecond_ConflictingEnum.Second
                )
            )
        );

        // Deploy contract with conflicting type names, then call functions to update those values
        conflictingTypeNameContractClient = deployConflictingTypeNameContractFirst(
            ConflictingType.wrap(true),
            ConflictingStruct({ a: true }),
            ConflictingEnum.Third,
            DeployOptions({ salt: 0, referenceName: "conflictingTypeNameContractFirstTwo" })
        );
        conflictingTypeNameContractClient.setConflictingTypes(
            ConflictingType.wrap(false),
            ConflictingStruct({ a: false }),
            ConflictingEnum.Second
        );
        conflictingTypeNameContractFirstTwo = ConflictingTypeNameContractFirst(
            address(conflictingTypeNameContractClient)
        );

        // Deploy contract that uses msg.sender
        MsgSenderClient msgSenderClient = deployMsgSender();
        msgSenderClient.setSender();
        msgSender = MsgSender(address(msgSenderClient));

        // Deploy contract that has unnamed parameters
        UnnamedParametersClient unnamedParametersClient = deployUnnamedParameters(1, 2);
        unnamedParametersClient.increment(1, 3);
        unnamedParameters = UnnamedParameters(address(unnamedParametersClient));

        // Deploy inherited contract and interact with it
        ChildClient childClient = deployChild(1, false, address(2));
        childClient.add(childClient.myPureB());
        childClient.add(childClient.myPureB(), 2);
        childClient.setMyAddress(address(3));
        child = Child(address(childClient));

        // Deploy multi-inherited contract that uses an alias and interact with it
        GrandchildClient grandchildClient = deployGrandchild(
            1,
            false,
            address(2),
            keccak256("3")
        );
        grandchildClient.setMyBytes32(grandchildClient.myPureC());
        grandchildClient.setMyAddress(address(4));
        grandchildClient.add(grandchildClient.myPureB());
        grandchild = Grandchild(address(grandchildClient));

        // Deploy contract that inherits from a contract in the same file
        ChildInSameFileClient childInSameFileClient = deployChildInSameFile(1, false);
        childInSameFileClient.setBool(true);
        childInSameFileClient.add(2);
        childInSameFile = ChildInSameFile(address(childInSameFileClient));

        // Deploy two contracts with conflicting qualified names
        conflictingQualifiedNames = ConflictingQualifiedNames(
            address(deployTypegenConflictingQualifiedNamesConflictingQualifiedNames_ConflictingQualifiedNames(1))
        );
        conflictingQualifiedNamesA = ConflictingQualifiedNamesA(
            address(
                deployConflictingQualifiedNames(
                    true,
                    DeployOptions({ salt: 0, referenceName: "conflictingQualifiedNamesA" })
                )
            )
        );

        // Deploy contract that inherits from a contract with a conflicting qualified name
        ConflictingQualifiedNameChildClient conflictingQualifiedNameChildClient = deployConflictingQualifiedNameChild(
            1,
            true
        );
        conflictingQualifiedNameChildClient.add(2);
        conflictingQualifiedNameChildClient.set(false);
        conflictingQualifiedNameChild = ConflictingQualifiedNameChild(
            address(conflictingQualifiedNameChildClient)
        );

        // Deploy contract that inherits from a contract in the same file which has a conflicting qualified name
        ConflictingQualifiedNameChildInSameFileClient conflictingQualifiedNameChildInSameFileClient = deployConflictingQualifiedNameChildInSameFile(
            1,
            2
        );
        conflictingQualifiedNameChildInSameFileClient.addY(4);
        conflictingQualifiedNameChildInSameFileClient.add(4);
        conflictingQualifiedNameChildInSameFile = ConflictingQualifiedNameChildInSameFile(
            address(conflictingQualifiedNameChildInSameFileClient)
        );

        // Deploy and interact with a contract that inherits from a contract that uses user defined types
        ChildParentImportsTypesClient childParentImportsTypesClient = deployChildParentImportsTypes(
          MyLocalTypeLibrary.MyEnumInLibrary.Library,
          MyLocalTypeLibrary.MyStructInLibrary({ a: true }),
          MyLocalTypeLibrary.MyTypeInLibrary.wrap(true),
          MyLocalTypeContract.MyEnumInContract.Contract,
          MyLocalTypeContract.MyStructInContract({ a: keccak256("1") }),
          MyLocalTypeContract.MyTypeInContract.wrap(keccak256("2"))
        );
        childParentImportsTypesClient.updateValues(
          MyLocalTypeLibrary.MyEnumInLibrary.Local,
          MyLocalTypeLibrary.MyStructInLibrary({ a: false }),
          MyLocalTypeLibrary.MyTypeInLibrary.wrap(false),
          MyLocalTypeContract.MyEnumInContract.Enum,
          MyLocalTypeContract.MyStructInContract({ a: keccak256("3") }),
          MyLocalTypeContract.MyTypeInContract.wrap(keccak256("4"))
        );
        childParentImportsTypes = ChildParentImportsTypes(address(childParentImportsTypesClient));

        // Deploy and interact with a contract that overrides a function from a parent contract
        ChildOverridesClient childOverridesClient = deployChildOverrides(2);
        childOverridesClient.add(2);
        childOverrides = ChildOverrides(address(childOverridesClient));
    }
}
