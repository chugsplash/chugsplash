// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

import { ChugSplashManager } from "./ChugSplashManager.sol";
import { ChugSplashManagerProxy } from "./ChugSplashManagerProxy.sol";
import { Initializable } from "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import {
    OwnableUpgradeable
} from "@openzeppelin/contracts-upgradeable/access/OwnableUpgradeable.sol";
import { Proxy } from "./libraries/Proxy.sol";

/**
 * @title ChugSplashRecorder
 * @notice The ChugSplashRecorder announces events emitted by the ChugSplash system and also keeps a
 *         record of ProxyAdapter and ChugSplashManager contracts. This functionality will be merged
 *         into the ChugSplashRegistry when ChugSplash becomes non-upgradeable.
 */
contract ChugSplashRecorder {
    /**
     * @notice Emitted whenever a ChugSplashManager contract wishes to announce an event on the
     *         registry. We use this to avoid needing a complex indexing system when we're trying
     *         to find events emitted by the various manager contracts.
     *
     * @param eventNameHash Hash of the name of the event being announced.
     * @param manager       Address of the ChugSplashManager announcing an event.
     * @param eventName     Name of the event being announced.
     */
    event EventAnnounced(string indexed eventNameHash, address indexed manager, string eventName);

    /**
     * @notice Emitted whenever a ChugSplashManager contract wishes to announce an event on the
     *         registry, including a field for arbitrary data. We use this to avoid needing a
     *         complex indexing system when we're trying to find events emitted by the various
     *         manager contracts.
     *
     * @param eventNameHash Hash of the name of the event being announced.
     * @param manager       Address of the ChugSplashManager announcing an event.
     * @param dataHash      Hash of the extra data.
     * @param eventName     Name of the event being announced.
     * @param data          The extra data.
     */
    event EventAnnouncedWithData(
        string indexed eventNameHash,
        address indexed manager,
        bytes indexed dataHash,
        string eventName,
        bytes data
    );

    /**
     * @notice Emitted whenever a new proxy type is added.
     *
     * @param proxyType Hash representing the proxy type.
     * @param adapter   Address of the adapter for the proxy.
     */
    event ProxyTypeAdded(bytes32 proxyType, address adapter);

    /**
     * @notice Mapping of proxy types to adapters.
     */
    mapping(bytes32 => address) public adapters;

    /**
     * @notice Mapping of created manager contracts.
     */
    mapping(ChugSplashManager => bool) public managers;

    address public immutable registryProxy;

    address public immutable registryImpl;

    /**
     * @param _registryProxy Address of the ChugSplashRegistry proxy.
     */
    constructor(address _registryProxy, address _registryImpl) {
        registryProxy = _registryProxy;
        registryImpl = _registryImpl;
    }

    function addManager(address _manager) external {
        require(
            msg.sender == registryProxy || msg.sender == registryImpl,
            "ChugSplashRecorder: caller must be registry proxy or impl"
        );
        managers[ChugSplashManager(payable(address(_manager)))] = true;
    }

    /**
     * @notice Allows ChugSplashManager contracts to announce events.
     *
     * @param _event Name of the event to announce.
     */
    function announce(string memory _event) public {
        require(
            managers[ChugSplashManager(payable(msg.sender))] == true,
            "ChugSplashRecorder: events can only be announced by ChugSplashManager contracts"
        );

        emit EventAnnounced(_event, msg.sender, _event);
    }

    /**
     * @notice Allows ChugSplashManager contracts to announce events, including a field for
     *         arbitrary data.
     *
     * @param _event Name of the event to announce.
     * @param _data  Arbitrary data to include in the announced event.
     */
    function announceWithData(string memory _event, bytes memory _data) public {
        require(
            managers[ChugSplashManager(payable(msg.sender))] == true,
            "ChugSplashRecorder: events can only be announced by ChugSplashManager contracts"
        );

        emit EventAnnouncedWithData(_event, msg.sender, _data, _event, _data);
    }

    /**
     * @notice Adds a new proxy type with a corresponding adapter.
     *
     * @param _proxyType Hash representing the proxy type
     * @param _adapter   Address of the adapter for this proxy type.
     */
    function addProxyType(bytes32 _proxyType, address _adapter) external {
        require(
            adapters[_proxyType] == address(0),
            "ChugSplashRegistry: proxy type has an existing adapter"
        );

        adapters[_proxyType] = _adapter;

        emit ProxyTypeAdded(_proxyType, _adapter);
    }
}
