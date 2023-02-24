// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

import { IProxyAdapter } from "../interfaces/IProxyAdapter.sol";
import { IProxyUpdater } from "../interfaces/IProxyUpdater.sol";
import { Proxy } from "../libraries/Proxy.sol";

/**
 * @title DefaultAdapter
 * @notice Adapter for the default EIP-1967 proxy used by ChugSplash.
 */
contract DefaultAdapter is IProxyAdapter {
    address public immutable proxyUpdater;

    constructor(address _proxyUpdater) {
        proxyUpdater = _proxyUpdater;
    }

    /**
     * @inheritdoc IProxyAdapter
     */
    function initiateExecution(address payable _proxy) external {
        Proxy(_proxy).upgradeTo(proxyUpdater);
    }

    /**
     * @inheritdoc IProxyAdapter
     */
    function completeExecution(
        address payable _proxy,
        address _implementation,
        bytes memory
    ) external {
        Proxy(_proxy).upgradeTo(_implementation);
    }

    /**
     * @inheritdoc IProxyAdapter
     */
    function setStorage(
        address payable _proxy,
        bytes32 _key,
        uint8 _offset,
        bytes memory _segment
    ) external {
        IProxyUpdater(_proxy).setStorage(_key, _offset, _segment);
    }

    /**
     * @inheritdoc IProxyAdapter
     */
    function changeProxyAdmin(address payable _proxy, address _newAdmin) external {
        Proxy(_proxy).changeAdmin(_newAdmin);
    }
}
