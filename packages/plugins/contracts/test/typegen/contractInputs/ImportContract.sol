// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

contract MyImportContract {
    uint256 public number;

    constructor(uint256 _number) {
        number = _number;
    }
}
