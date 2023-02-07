// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

import { IProxyUpdater } from "../IProxyUpdater.sol";

 /**
 * @title DefaultUpdater
 * @notice Proxy Updater for an OpenZeppelin Transparent Upgradeable proxy. This is the proxy updater used by
 *         default proxies in the ChugSplash system. To learn more about the transparent proxy
 *         pattern, see: https://docs.openzeppelin.com/contracts/4.x/api/proxy#transparent_proxy
 */
contract OZUUPSUpdater is IProxyUpdater {
    /**
     * @notice The storage slot that holds the address of the implementation.
     *         bytes32(uint256(keccak256('eip1967.proxy.implementation')) - 1)
     */
    bytes32 internal constant IMPLEMENTATION_KEY =
        0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc;

    /**
     * @notice The storage slot that holds the address of the implementation.
     *         bytes32(uint256(keccak256('chugsplash.proxy.admin')) - 1)
     */
    bytes32 internal constant CHUGSPLASH_ADMIN_KEY =
        0xadf644ee9e2068b2c186f6b9a2f688d3450c4110b8018da281fbbd8aa6c34996;

    /**
     * @notice An event that is emitted each time the implementation is changed. This event is part
     *         of the EIP-1967 specification.
     *
     * @param implementation The address of the implementation contract
     */
    event Upgraded(address indexed implementation);

    constructor() {}

    function proxiableUUID() external view virtual returns (bytes32) {
        return 0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc;
    }

    /**
     * @notice Set the implementation contract address. The code at the given address will execute
     *         when this contract is called.
     *
     * @param _implementation Address of the implementation contract.
     */
    function upgradeTo(address _implementation) external ifChugSplashAdmin {
        _setImplementation(_implementation);
    }

    /**
     * @notice Set the implementation and call a function in a single transaction. Useful to ensure
     *         atomic execution of initialization-based upgrades.
     *
     * @param _implementation Address of the implementation contract.
     * @param _data           Calldata to delegatecall the new implementation with.
     */
    function upgradeToAndCall(
        address _implementation,
        bytes calldata _data
    ) external payable ifChugSplashAdmin returns (bytes memory) {
        _setImplementation(_implementation);
        (bool success, bytes memory returndata) = _implementation.delegatecall(_data);
        require(success, "Proxy: delegatecall to new implementation contract failed");
        return returndata;
    }

    /**
     * @notice Queries the implementation address.
     *
     * @return Implementation address.
     */
    function implementation() external view returns (address) {
        return _getImplementation();
    }

    /**
     * @notice Sets the implementation address.
     *
     * @param _implementation New implementation address.
     */
    function _setImplementation(address _implementation) internal {
        assembly {
            sstore(IMPLEMENTATION_KEY, _implementation)
        }
        emit Upgraded(_implementation);
    }

    /**
     * @notice Queries the implementation address.
     *
     * @return Implementation address.
     */
    function _getImplementation() internal view virtual returns (address) {
        address impl;
        assembly {
            impl := sload(IMPLEMENTATION_KEY)
        }
        return impl;
    }

    /**
     * @notice Sets the chugsplash specific admin address.
     *
     * @param _newAdmin New admin address.
     */
    function _setChugSplashAdmin(address _newAdmin) internal {
        assembly {
            sstore(CHUGSPLASH_ADMIN_KEY, _newAdmin)
        }
    }

    /**
     * @notice Queries the chugsplash specific admin address.
     *
     * @return Owner address.
     */
    function _getChugSplashAdmin() internal view returns (address) {
        address chugsplashAdmin;
        assembly {
            chugsplashAdmin := sload(CHUGSPLASH_ADMIN_KEY)
        }
        return chugsplashAdmin;
    }

    /**
     * @notice A modifier that reverts if not called by the owner or by address(0) to allow
     *         eth_call to interact with this proxy without needing to use low-level storage
     *         inspection. We assume that nobody is able to trigger calls from address(0) during
     *         normal EVM execution.
     */
    modifier ifChugSplashAdmin() {
        if (msg.sender == _getChugSplashAdmin() || msg.sender == address(0)) {
            _;
        } else {
            revert("unauthorized");
        }
    }

    /**
     * @notice Modifies some storage slot within the proxy contract. Gives us a lot of power to
     *         perform upgrades in a more transparent way.
     *
     * @param _key   Storage key to modify.
     * @param _value New value for the storage key.
     */
    function setStorage(bytes32 _key, bytes32 _value) external ifChugSplashAdmin {
        assembly {
            sstore(_key, _value)
        }
    }

    /**
     * @notice Sets up the proxy updater. In this case, there is no setup required.
     */
    function setup() public {
        if (_getChugSplashAdmin() == address(0)) {
            _setChugSplashAdmin(msg.sender);
        }
    }

    /**
     * @notice Tears down the proxy updater. In this case, there is no setup required.
     */
    function teardown(address _implementation) external ifChugSplashAdmin {
        _setChugSplashAdmin(address(0));
        _setImplementation(_implementation);
    }

    fallback() external {
        revert();
    }
}
