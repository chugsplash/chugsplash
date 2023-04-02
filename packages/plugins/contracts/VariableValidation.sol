// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

contract VariableValidation {
    int8 public arrayInt8;
    int8 public int8OutsideRange;
    uint8 public uint8OutsideRange;
    address public intAddress;
    address public arrayAddress;
    address public shortAddress;
    bytes32 public intBytes32;
    bytes32 public arrayBytes32;
    bytes32 public shortBytes32;
    bytes8 public longBytes8;
    bytes16 public malformedBytes16;
    bytes8 public oddStaticBytes;
    bytes public oddDynamicBytes;
    bool public intBoolean;
    bool public stringBoolean;
    bool public arrayBoolean;
    int8[2] public oversizedArray;
    int8[2][2] public oversizedNestedArray;
    bool[2] public invalidBoolArray;
    bytes32[2] public invalidBytes32Array;
    address[2] public invalidAddressArray;
    mapping(string => string) public invalidStringStringMapping;
    mapping(string => int) public invalidStringIntMapping;
    mapping(string => mapping(string => int)) public invalidNestedStringIntBoolMapping;

    // Variables that are not set in the config
    uint public notSetUint;
    string public notSetString;
}
