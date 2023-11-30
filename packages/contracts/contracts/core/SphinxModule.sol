// SPDX-License-Identifier: MIT
pragma solidity ^0.8.2;

import { Enum } from "@gnosis.pm/safe-contracts-1.3.0/common/Enum.sol";
// We import `GnosisSafe` v1.3.0 here, but this contract also supports `GnosisSafeL2.sol` (v1.3.0)
// as well as `Safe.sol` and `SafeL2.sol` from Safe v1.4.1. All of these contracts share the same
// interface for the functions used in this contract.
import { GnosisSafe } from "@gnosis.pm/safe-contracts-1.3.0/GnosisSafe.sol";
// Likewise, we deploy `IProxy` v1.3.0 here, but this contract also supports `IProxy` v1.4.1.
import { IProxy } from "@gnosis.pm/safe-contracts-1.3.0/proxies/GnosisSafeProxy.sol";
import { Initializable } from "@openzeppelin/contracts/proxy/utils/Initializable.sol";
import { ReentrancyGuard } from "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import { MerkleProof } from "@openzeppelin/contracts/utils/cryptography/MerkleProof.sol";
import {
    SphinxLeafType,
    SphinxLeaf,
    SphinxLeafWithProof,
    MerkleRootState,
    MerkleRootStatus
} from "./SphinxDataTypes.sol";
import { ISphinxModule } from "./interfaces/ISphinxModule.sol";

/**
 * @title SphinxModule
 * @notice The `SphinxModule` contains the logic that executes deployments in a Gnosis Safe and
 *         verifies that the Gnosis Safe owners have signed the Merkle root that contains
 *         the deployment. It also contains logic for cancelling active Merkle roots.
 *
 *         The `SphinxModule` exists as an implementation contract, which is delegatecalled
 *         by minimal, non-upgradeable EIP-1167 proxy contracts. We use this architecture
 *         because it's considerably cheaper to deploy an EIP-1167 proxy than a `SphinxModule`.
 */
contract SphinxModule is ReentrancyGuard, Enum, ISphinxModule, Initializable {
    /**
     * @inheritdoc ISphinxModule
     */
    string public constant override VERSION = "1.0.0";

    /**
     * @dev The code hash for the Gnosis Safe proxy v1.3.0.
     */
    bytes32 internal constant SAFE_PROXY_CODE_HASH_1_3_0 =
        0xb89c1b3bdf2cf8827818646bce9a8f6e372885f8c55e5c07acbd307cb133b000;

    /**
     * @dev The code hash for the Gnosis Safe proxy v1.4.1.
     */
    bytes32 internal constant SAFE_PROXY_CODE_HASH_1_4_1 =
        0xd7d408ebcd99b2b70be43e20253d6d92a8ea8fab29bd3be7f55b10032331fb4c;

    /**
     * @dev The code hash for the L1 Gnosis Safe singleton v1.3.0.
     */
    bytes32 internal constant SAFE_SINGLETON_CODE_HASH_L1_1_3_0 =
        0xbba688fbdb21ad2bb58bc320638b43d94e7d100f6f3ebaab0a4e4de6304b1c2e;

    /**
     * @dev The code hash for the L2 Gnosis Safe singleton v1.3.0.
     */
    bytes32 internal constant SAFE_SINGLETON_CODE_HASH_L2_1_3_0 =
        0x21842597390c4c6e3c1239e434a682b054bd9548eee5e9b1d6a4482731023c0f;

    /**
     * @dev The code hash for the L1 Gnosis Safe singleton v1.4.1.
     */
    bytes32 internal constant SAFE_SINGLETON_CODE_HASH_L1_1_4_1 =
        0x1fe2df852ba3299d6534ef416eefa406e56ced995bca886ab7a553e6d0c5e1c4;

    /**
     * @dev The code hash for the L2 Gnosis Safe singleton v1.4.1.
     */
    bytes32 internal constant SAFE_SINGLETON_CODE_HASH_L2_1_4_1 =
        0xb1f926978a0f44a2c0ec8fe822418ae969bd8c3f18d61e5103100339894f81ff;

    /**
     * @dev TODO(docs): keccak256("fallback_manager.handler.address")
     */
    uint256 internal constant SAFE_FALLBACK_HANDLER_STORAGE_SLOT = 0x6c9a6c4a39284e37ed1cf53d337577d14212a4870fb976a4366c693b939918d5;

    /**
     * @dev TODO(docs)
     */
    uint256 internal constant GUARD_STORAGE_SLOT = 0x4a204f620c8c5ccdca3fd54d003badd85ba500436a431f0cbda4f558c93c34c8;

    /**
     * @dev The EIP-712 domain separator, which displays a bit of context to the user
     *      when they sign the Merkle root off-chain.
     */
    bytes32 internal constant DOMAIN_SEPARATOR =
        keccak256(abi.encode(keccak256("EIP712Domain(string name)"), keccak256(bytes("Sphinx"))));

    /**
     * @dev The EIP-712 type hash, which just contains the Merkle root.
     */
    bytes32 internal constant TYPE_HASH = keccak256("MerkleRoot(bytes32 root)");

    bytes32 internal constant BYTES32_ZERO_HASH = keccak256(new bytes(32));

    /**
     * @inheritdoc ISphinxModule
     */
    mapping(bytes32 => MerkleRootState) public override merkleRootStates;

    /**
     * @inheritdoc ISphinxModule
     */
    uint256 public override merkleRootNonce;

    /**
     * @inheritdoc ISphinxModule
     */
    bytes32 public override activeMerkleRoot;

    /**
     * @inheritdoc ISphinxModule
     */
    address payable public override safeProxy;

    /**
     * @notice Locks the `SphinxModule` implementation contract so it can't be
     *         initialized directly.
     */
    constructor() {
        _disableInitializers();
    }

    /**
     * @inheritdoc ISphinxModule
     */
    function initialize(address _safeProxy) external override initializer {
        require(_safeProxy != address(0), "SphinxModule: invalid Safe address");

        // Check that the Gnosis Safe proxy's address has a valid code hash.
        bytes32 safeProxyCodeHash = _safeProxy.codehash;
        // require(
        //     safeProxyCodeHash == SAFE_PROXY_CODE_HASH_1_3_0 ||
        //         safeProxyCodeHash == SAFE_PROXY_CODE_HASH_1_4_1,
        //     "SphinxModule: invalid Safe proxy"
        // );

        // Check that the Gnosis Safe proxy has a singleton with a valid code hash.
        GnosisSafe safeSingleton = GnosisSafe(payable(IProxy(_safeProxy).masterCopy()));
        bytes32 safeSingletonCodeHash = address(safeSingleton).codehash;
        // require(
        //     safeSingletonCodeHash == SAFE_SINGLETON_CODE_HASH_L1_1_3_0 ||
        //         safeSingletonCodeHash == SAFE_SINGLETON_CODE_HASH_L2_1_3_0 ||
        //         safeSingletonCodeHash == SAFE_SINGLETON_CODE_HASH_L1_1_4_1 ||
        //         safeSingletonCodeHash == SAFE_SINGLETON_CODE_HASH_L2_1_4_1,
        //     "SphinxModule: invalid Safe singleton"
        // );

        require(abi.decode(safeSingleton.getStorageAt(0, 1), (address)) == address(0), "TODO");
        (, address next) = safeSingleton.getModulesPaginated(address(1), 100);
        require(next == address(0), "TODO");
        // TODO(docs): we can't use `safeSingleton.getOwners()` because...
        require(abi.decode(safeSingleton.getStorageAt(3, 1), (uint256)) == 0, "TODO");
        require(safeSingleton.getThreshold() == 1, "TODO");
        require(safeSingleton.nonce() == 0, "TODO");
        require(abi.decode(safeSingleton.getStorageAt(SAFE_FALLBACK_HANDLER_STORAGE_SLOT, 1), (address)) == address(0), "TODO");
        require(abi.decode(safeSingleton.getStorageAt(GUARD_STORAGE_SLOT, 1), (address)) == address(0), "TODO");

        // TODO: undo changes in OwnerManager.sol 1.3.0
        // TODO: undo changes in ModuleManager 1.4.1

        safeProxy = payable(_safeProxy);
    }

    /**
     * @inheritdoc ISphinxModule
     */
    function approve(
        bytes32 _root,
        SphinxLeafWithProof memory _leafWithProof,
        bytes memory _signatures
    )
        public
        override
        // We add a re-entrancy guard out of an abundance of caution. It's possible for the call to
        // the Gnosis Safe's `checkSignatures` function to call into another contract when
        // validating an EIP-1271 contract signature.
        nonReentrant
    {
        require(activeMerkleRoot == bytes32(0), "SphinxModule: active merkle root");

        require(_root != bytes32(0), "SphinxModule: invalid root");

        // Check that the Merkle root hasn't been used before.
        MerkleRootState storage state = merkleRootStates[_root];
        require(state.status == MerkleRootStatus.EMPTY, "SphinxModule: root already used");

        SphinxLeaf memory leaf = _leafWithProof.leaf;
        // Revert if the Merkle leaf does not yield the Merkle root, given the Merkle proof.
        require(
            MerkleProof.verify(_leafWithProof.proof, _root, _getLeafHash(leaf)),
            "SphinxModule: failed to verify leaf"
        );

        require(leaf.leafType == SphinxLeafType.APPROVE, "SphinxModule: invalid leaf type");
        // The `APPROVE` leaf must always have an index of 0.
        require(leaf.index == 0, "SphinxModule: invalid leaf index");

        // Decode the `APPROVE` leaf data.
        (
            address leafSafeProxy,
            address moduleProxy,
            uint256 leafMerkleRootNonce,
            uint256 numLeaves,
            address executor,
            string memory uri,
            bool arbitraryChain
        ) = abi.decode(leaf.data, (address, address, uint256, uint256, address, string, bool));

        require(leafSafeProxy == address(safeProxy), "SphinxModule: invalid SafeProxy");
        require(moduleProxy == address(this), "SphinxModule: invalid SphinxModuleProxy");
        require(leafMerkleRootNonce == merkleRootNonce, "SphinxModule: invalid nonce");
        // The `numLeaves` must be at least `1` because there must always at least be an `APPROVE` leaf.
        require(numLeaves > 0, "SphinxModule: numLeaves cannot be 0");
        require(executor == msg.sender, "SphinxModule: caller isn't executor");
        // The current chain ID must match the leaf's chain ID, or the Merkle root must
        // be executable on an arbitrary chain.
        require(leaf.chainId == block.chainid || arbitraryChain, "SphinxModule: invalid chain id");
        // If the Merkle root can be executable on an arbitrary chain, the leaf must have a chain ID
        // of 0. This isn't strictly necessary; it just enforces a convention.
        require(!arbitraryChain || leaf.chainId == 0, "SphinxModule: leaf chain id must be 0");
        // We don't validate the `uri` because it we allow it to be empty.

        emit SphinxMerkleRootApproved(
            _root,
            activeMerkleRoot,
            merkleRootNonce,
            executor,
            numLeaves,
            uri
        );

        // Assign values to all fields of the new Merkle root's `MerkleRootState` except for the
        // `status` field, which will be assigned below.
        state.numLeaves = numLeaves;
        state.leavesExecuted = 1;
        state.uri = uri;
        state.executor = executor;
        state.arbitraryChain = arbitraryChain;

        merkleRootNonce += 1;

        // If there is only an `APPROVE` leaf, mark the Merkle root as completed. The purpose of
        // this is to allow the Gnosis Safe owners to cancel a different Merkle root that has been
        // signed off-chain, but has not been approved in this contract. The owners can do this by
        // by signing a new Merkle root that has the same Merkle root nonce and approving it
        // on-chain. This prevents the old Merkle root from ever being approved. In the event that
        // the Gnosis Safe owners want to cancel a Merkle root without approving a new deployment,
        // they can simply approve a Merkle root that contains a single `APPROVE` leaf.
        if (numLeaves == 1) {
            emit SphinxMerkleRootCompleted(_root);
            state.status = MerkleRootStatus.COMPLETED;
            // Set the active Merkle root to be `bytes32(0)` so that a new approval can occur in the
            // future.
            activeMerkleRoot = bytes32(0);
        } else {
            // We set the status to `APPROVED` because there are `EXECUTE` leaves in this Merkle tree.
            state.status = MerkleRootStatus.APPROVED;
            activeMerkleRoot = _root;
        }

        // Check that a sufficient number of Gnosis Safe owners have signed the Merkle root (or,
        // more specifically, EIP-712 data that includes the Merkle root). We do this last to
        // follow the checks-effects-interactions pattern, since it's possible for `checkSignatures`
        // to call into another contract if it's validating an EIP-1271 contract signature.
        bytes memory typedData = abi.encodePacked(
            "\x19\x01",
            DOMAIN_SEPARATOR,
            keccak256(abi.encode(TYPE_HASH, _root))
        );
        GnosisSafe(safeProxy).checkSignatures(keccak256(typedData), typedData, _signatures);
    }

    /**
     * @inheritdoc ISphinxModule
     */
    function cancel(
        bytes32 _root,
        SphinxLeafWithProof memory _leafWithProof,
        bytes memory _signatures
    )
        public
        override
        // We add a re-entrancy guard out of an abundance of caution. It's possible for the call to
        // the Gnosis Safe's `checkSignatures` function to call into another contract when
        // validating an EIP-1271 contract signature.
        nonReentrant
    {
        require(activeMerkleRoot != bytes32(0), "SphinxModule: no root to cancel");

        require(_root != bytes32(0), "SphinxModule: invalid root");

        // Check that the Merkle root hasn't been used before.
        MerkleRootState storage state = merkleRootStates[_root];
        require(state.status == MerkleRootStatus.EMPTY, "SphinxModule: root already used");

        SphinxLeaf memory leaf = _leafWithProof.leaf;
        // Revert if the Merkle leaf does not yield the Merkle root, given the Merkle proof.
        require(
            MerkleProof.verify(_leafWithProof.proof, _root, _getLeafHash(leaf)),
            "SphinxModule: failed to verify leaf"
        );

        require(leaf.leafType == SphinxLeafType.CANCEL, "SphinxModule: invalid leaf type");
        // The `CANCEL` leaf must always have an index of 0.
        require(leaf.index == 0, "SphinxModule: invalid leaf index");

        // Decode the `CANCEL` leaf data.
        (
            address leafSafeProxy,
            address moduleProxy,
            uint256 leafMerkleRootNonce,
            bytes32 merkleRootToCancel,
            address executor,
            string memory uri
        ) = abi.decode(leaf.data, (address, address, uint256, bytes32, address, string));

        require(leafSafeProxy == address(safeProxy), "SphinxModule: invalid SafeProxy");
        require(moduleProxy == address(this), "SphinxModule: invalid SphinxModuleProxy");
        require(leafMerkleRootNonce == merkleRootNonce, "SphinxModule: invalid nonce");
        require(merkleRootToCancel == activeMerkleRoot, "SphinxModule: invalid root to cancel");
        require(executor == msg.sender, "SphinxModule: caller isn't executor");
        // The current chain ID must match the leaf's chain ID. We don't allow `arbitraryChain` to
        // be `true` here because we don't think there's a use case for cancelling Merkle roots
        // across arbitrary networks.
        require(leaf.chainId == block.chainid, "SphinxModule: invalid chain id");
        // We don't validate the `uri` because it we allow it to be empty.

        // Cancel the active Merkle root.
        emit SphinxMerkleRootCanceled(_root, activeMerkleRoot, merkleRootNonce, executor, uri);
        merkleRootStates[activeMerkleRoot].status = MerkleRootStatus.CANCELED;
        activeMerkleRoot = bytes32(0);

        // Mark the input Merkle root as `COMPLETED`.
        emit SphinxMerkleRootCompleted(_root);
        // Assign values to all fields of the new Merkle root's `MerkleRootState` except for the
        // `arbitraryChain` field, which is `false` for this Merkle root.
        state.numLeaves = 1;
        state.leavesExecuted = 1;
        state.uri = uri;
        state.executor = executor;
        state.status = MerkleRootStatus.COMPLETED;

        merkleRootNonce += 1;

        // Check that a sufficient number of Gnosis Safe owners have signed the Merkle root (or,
        // more specifically, EIP-712 data that includes the Merkle root). We do this last to
        // follow the checks-effects-interactions pattern, since it's possible for `checkSignatures`
        // to call into another contract if it's validating an EIP-1271 contract signature.
        bytes memory typedData = abi.encodePacked(
            "\x19\x01",
            DOMAIN_SEPARATOR,
            keccak256(abi.encode(TYPE_HASH, _root))
        );
        GnosisSafe(safeProxy).checkSignatures(keccak256(typedData), typedData, _signatures);
    }

    /**
     * @inheritdoc ISphinxModule
     */
    function execute(SphinxLeafWithProof[] memory _leavesWithProofs) public override nonReentrant {
        uint256 numActions = _leavesWithProofs.length;
        require(numActions > 0, "SphinxModule: no leaves to execute");
        require(activeMerkleRoot != bytes32(0), "SphinxModule: no active root");

        MerkleRootState storage state = merkleRootStates[activeMerkleRoot];

        require(state.executor == msg.sender, "SphinxModule: caller isn't executor");

        // Revert if the number of previously executed leaves plus the number of leaves in the current
        // array is greater than the `numLeaves` specified in the `approve` function.
        require(
            state.numLeaves >= state.leavesExecuted + numActions,
            "SphinxModule: extra leaves not allowed"
        );

        SphinxLeaf memory leaf;
        bytes32[] memory proof;
        // Iterate through each of the Merkle leaves in the array.
        for (uint256 i = 0; i < numActions; i++) {
            leaf = _leavesWithProofs[i].leaf;
            proof = _leavesWithProofs[i].proof;

            require(
                MerkleProof.verify(proof, activeMerkleRoot, _getLeafHash(leaf)),
                "SphinxModule: failed to verify leaf"
            );
            require(leaf.leafType == SphinxLeafType.EXECUTE, "SphinxModule: invalid leaf type");
            // Revert if the current leaf is being executed in the incorrect order.
            require(leaf.index == state.leavesExecuted, "SphinxModule: invalid leaf index");
            // The current chain ID must match the leaf's chain ID, or the Merkle root must
            // be executable on an arbitrary chain.
            require(
                leaf.chainId == block.chainid || state.arbitraryChain,
                "SphinxModule: invalid chain id"
            );
            // If the Merkle root can be executable on an arbitrary chain, the leaf must have a chain ID
            // of 0. This isn't strictly necessary; it just enforces a convention.
            require(
                !state.arbitraryChain || leaf.chainId == 0,
                "SphinxModule: leaf chain id must be 0"
            );

            // Decode the Merkle leaf's data.
            (
                address to,
                uint256 value,
                uint256 gas,
                bytes memory txData,
                Enum.Operation operation,
                bool requireSuccess
            ) = abi.decode(leaf.data, (address, uint256, uint256, bytes, Enum.Operation, bool));

            state.leavesExecuted += 1;

            // Declare a `success` boolean, which we'll assign to the outcome of the call to the
            // Gnosis Safe. Slither thinks that it's possible for this variable to remain
            // unassigned, which is not true. It's always either assigned in the body of the `try`
            // statement or the `catch` statement below.
            // slither-disable-next-line uninitialized-local
            bool success;

            // Check that the amount of gas forwarded to the Gnosis Safe will be *equal* to the
            // `gas` specified by the user. If you'd like to understand the specifics of this
            // `require` statement, you'll need some background about the EVM first:
            // - When hard-coding a gas amount to an external call, the EVM will forward *at most*
            //   the specified gas amount. It's possible to forward less gas if there isn't enough
            //   gas available in the current scope.
            // - We can only forward 63/64 of the available gas to the external call (as of
            //   EIP-150). In other words, if we want to forward 100k gas, there must be at least
            //   100k * (64 / 63) gas available in the current scope.
            // So, without this `require` statement, it'd be possible for the executor to send an
            // insufficient amount of gas to the Gnosis Safe, which could cause the user's
            // transaction to fail. We multiply the `gas` by (64 / 63) to account for the fact that
            // we can only forward 63/64 of the available gas to the external call. Lastly, we add
            // 10k as a buffer to account for:
            // 1. The cold `SLOAD` that occurs for the `safeProxy` variable shortly after this
            //    `require` statement. This costs 2100 gas.
            // 2. Several thousand gas to account for any future changes in the EVM.
            require(gasleft() >= ((gas * 64) / 63) + 10000, "SphinxModule: insufficient gas");

            // Slither warns that a call inside of a loop can lead to a denial-of-service
            // attack if the call reverts. However, this isn't a concern because the call to the
            // Gnosis Safe is wrapped in a try/catch, and because we restrict the amount of gas sent
            // along with the call. Slither also warns of a re-entrancy vulnerability here, which
            // isn't a concern because we've included a `nonReentrant` modifier in this function.
            // slither-disable-start calls-loop
            // slither-disable-start reentrancy-no-eth

            // Call the Gnosis Safe. We wrap it in a try/catch in case there's an EVM error that
            // occurs when making the call, which would otherwise cause the current context to
            // revert. This could happen if the user supplies an extremely low `gas` value (e.g.
            // 1000).
            try
                GnosisSafe(safeProxy).execTransactionFromModule{ gas: gas }(
                    to,
                    value,
                    txData,
                    operation
                )
            returns (bool execSuccess) {
                // The `execSuccess` returns whether or not the user's transaction reverted. We
                // don't use a low-level call to make it easy to retrieve this value.
                success = execSuccess;
            } catch {
                // An EVM error occurred when making the call. This can happen if the user supplies
                // an extremely low `gas` value (e.g. 1000).
                success = false;
            }
            // slither-disable-end calls-loop
            // slither-disable-end reentrancy-no-eth

            if (success) emit SphinxActionSucceeded(activeMerkleRoot, leaf.index);
            else emit SphinxActionFailed(activeMerkleRoot, leaf.index);

            // Mark the active Merkle root as failed if the Gnosis Safe transaction failed and the
            // current leaf requires that it must succeed.
            if (!success && requireSuccess) {
                emit SphinxMerkleRootFailed(activeMerkleRoot, leaf.index);
                state.status = MerkleRootStatus.FAILED;
                activeMerkleRoot = bytes32(0);
                return;
            }
        }

        // Mark the Merkle root as completed if all of the Merkle leaves have been executed.
        if (state.leavesExecuted == state.numLeaves) {
            emit SphinxMerkleRootCompleted(activeMerkleRoot);
            state.status = MerkleRootStatus.COMPLETED;
            activeMerkleRoot = bytes32(0);
        }
    }

    /**
     * @notice Hash a Merkle leaf. We do this before attempting to prove that the leaf
     *         belongs to a Merkle root. We double-hash the leaf to prevent second preimage attacks,
     *         as recommended by OpenZeppelin's Merkle Tree library.
     *
     * @param _leaf The Merkle leaf to hash.
     */
    function _getLeafHash(SphinxLeaf memory _leaf) internal pure returns (bytes32) {
        return keccak256(abi.encodePacked(keccak256(abi.encode(_leaf))));
    }
}
