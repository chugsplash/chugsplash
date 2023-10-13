// SPDX-License-Identifier: MIT
pragma solidity >=0.7.4 <0.9.0;

import "./ISphinxAccessControl.sol";

// TODO: remove this and iaccesscontrol.sol, since we don't need them anymore

/**
 * @notice Clone of `IAccessControlEnumerable.sol` in OpenZeppelin Contracts v4.4.1. The only
 *         modification is that this interface supports a wider Solidity version range.
 *         We've also added the `Sphinx` prefix to the interface name to avoid potential conflicts
 *         with other interfaces the user may have in their project. `See plugins/contracts/foundry/Sphinx.sol`
 *         for more details.
 * @dev External interface of AccessControlEnumerable declared to support ERC165 detection.
 */
interface ISphinxAccessControlEnumerable is ISphinxAccessControl {
    /**
     * @dev Returns one of the accounts that have `role`. `index` must be a
     * value between 0 and {getRoleMemberCount}, non-inclusive.
     *
     * Role bearers are not sorted in any particular way, and their ordering may
     * change at any point.
     *
     * WARNING: When using {getRoleMember} and {getRoleMemberCount}, make sure
     * you perform all queries on the same block. See the following
     * https://forum.openzeppelin.com/t/iterating-over-elements-on-enumerableset-in-openzeppelin-contracts/2296[forum post]
     * for more information.
     */
    function getRoleMember(bytes32 role, uint256 index) external view returns (address);

    /**
     * @dev Returns the number of accounts that have `role`. Can be used
     * together with {getRoleMember} to enumerate all bearers of a role.
     */
    function getRoleMemberCount(bytes32 role) external view returns (uint256);
}
