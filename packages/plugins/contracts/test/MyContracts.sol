// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

contract MyContract1 {
    int public intArg;
    int public secondIntArg;
    int public thirdIntArg;

    uint public uintArg;

    address public addressArg;
    address public otherAddressArg;

    constructor(
        int _intArg,
        uint _uintArg,
        address _addressArg,
        address _otherAddressArg
    ) {
        intArg = _intArg;
        uintArg = _uintArg;
        addressArg = _addressArg;
        otherAddressArg = _otherAddressArg;
    }

    function incrementUint() external {
        uintArg += 1;
    }

    function set(int _int, int _secondInt) external {
        intArg = _int;
        secondIntArg = _secondInt;
    }

    function set(address _addr, address _otherAddr) external {
        addressArg = _addr;
        otherAddressArg = _otherAddr;
    }

    function setInts(int _a, int _b, int _c) external {
        intArg = _a;
        secondIntArg = _b;
        thirdIntArg = _c;
    }
}

contract MyContract2 {

    uint public number;

    function incrementMyContract2(uint _num) external {
        number += _num;
    }
}