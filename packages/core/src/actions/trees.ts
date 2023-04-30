import { fromHexString, toHexString } from '@eth-optimism/core-utils'
import { ethers, providers } from 'ethers'
import MerkleTree from 'merkletreejs'
import { astDereferencer } from 'solidity-ast/utils'

import {
  ConfigArtifacts,
  ParsedChugSplashConfig,
  contractKindHashes,
} from '../config/types'
import { Integration } from '../constants'
import {
  computeStorageSegments,
  extendStorageLayout,
} from '../languages/solidity/storage'
import { ArtifactPaths } from '../languages/solidity/types'
import {
  getContractAddress,
  readContractArtifact,
  getCreationCodeWithConstructorArgs,
  readBuildInfo,
  getChugSplashManagerAddress,
} from '../utils'
import {
  ChugSplashAction,
  ChugSplashActionTree,
  ChugSplashActionType,
  ChugSplashMerkleTrees,
  ChugSplashTarget,
  ChugSplashTargetTree,
  DeployContractAction,
  RawChugSplashAction,
  SetStorageAction,
} from './types'
import { getStorageLayout } from './artifacts'

/**
 * Checks whether a given action is a SetStorage action.
 *
 * @param action ChugSplash action to check.
 * @return `true` if the action is a SetStorage action, `false` otherwise.
 */
export const isSetStorageAction = (
  action: ChugSplashAction
): action is SetStorageAction => {
  return (
    (action as SetStorageAction).key !== undefined &&
    (action as SetStorageAction).value !== undefined &&
    (action as SetStorageAction).offset !== undefined
  )
}

/**
 * Checks whether a given action is a DeployContract action.
 *
 * @param action ChugSplash action to check.
 * @returns `true` if the action is a DeployContract action, `false` otherwise.
 */
export const isDeployContractAction = (
  action: ChugSplashAction
): action is DeployContractAction => {
  return (action as DeployContractAction).code !== undefined
}

/**
 * Converts the "nice" action structs into a "raw" action struct (better for Solidity but
 * worse for users here).
 *
 * @param action ChugSplash action to convert.
 * @return Converted "raw" ChugSplash action.
 */
export const toRawChugSplashAction = (
  action: ChugSplashAction
): RawChugSplashAction => {
  if (isSetStorageAction(action)) {
    return {
      actionType: ChugSplashActionType.SET_STORAGE,
      addr: action.addr,
      contractKindHash: action.contractKindHash,
      referenceName: action.referenceName,
      data: ethers.utils.defaultAbiCoder.encode(
        ['bytes32', 'uint8', 'bytes'],
        [action.key, action.offset, action.value]
      ),
    }
  } else if (isDeployContractAction(action)) {
    return {
      actionType: ChugSplashActionType.DEPLOY_CONTRACT,
      addr: action.addr,
      contractKindHash: action.contractKindHash,
      referenceName: action.referenceName,
      data: action.code,
    }
  } else {
    throw new Error(`unknown action type`)
  }
}

/**
 * Converts a raw ChugSplash action into a "nice" action struct.
 *
 * @param rawAction Raw ChugSplash action to convert.
 * @returns Converted "nice" ChugSplash action.
 */
export const fromRawChugSplashAction = (
  rawAction: RawChugSplashAction
): ChugSplashAction => {
  if (rawAction.actionType === ChugSplashActionType.SET_STORAGE) {
    const [key, offset, value] = ethers.utils.defaultAbiCoder.decode(
      ['bytes32', 'uint8', 'bytes'],
      rawAction.data
    )
    return {
      referenceName: rawAction.referenceName,
      addr: rawAction.addr,
      contractKindHash: rawAction.contractKindHash,
      key,
      offset,
      value,
    }
  } else if (rawAction.actionType === ChugSplashActionType.DEPLOY_CONTRACT) {
    return {
      referenceName: rawAction.referenceName,
      addr: rawAction.addr,
      contractKindHash: rawAction.contractKindHash,
      code: rawAction.data,
    }
  } else {
    throw new Error(`unknown action type`)
  }
}

/**
 * Computes the hash of an action.
 *
 * @param action Action to compute the hash of.
 * @return Hash of the action.
 */
export const getActionHash = (action: RawChugSplashAction): string => {
  return ethers.utils.keccak256(
    ethers.utils.defaultAbiCoder.encode(
      ['string', 'address', 'uint8', 'bytes32', 'bytes'],
      [
        action.referenceName,
        action.addr,
        action.actionType,
        action.contractKindHash,
        action.data,
      ]
    )
  )
}

/**
 * Computes the hash of a target.
 *
 * @param target Target to compute the hash of.
 * @return Hash of the action.
 */
export const getTargetHash = (target: ChugSplashTarget): string => {
  return ethers.utils.keccak256(
    ethers.utils.defaultAbiCoder.encode(
      ['string', 'string', 'address', 'address', 'bytes32'],
      [
        target.projectName,
        target.referenceName,
        target.addr,
        target.implementation,
        target.contractKindHash,
      ]
    )
  )
}

export const makeTargetTree = (
  targets: ChugSplashTarget[]
): ChugSplashTargetTree => {
  // Compute the hash for each action.
  const elements = targets.map((target) => {
    return getTargetHash(target)
  })

  const tree = makeMerkleTree(elements)

  return {
    root: toHexString(tree.getRoot()),
    targets: targets.map((target, idx) => {
      return {
        target,
        siblings: tree.getProof(getTargetHash(target), idx).map((element) => {
          return element.data
        }),
      }
    }),
  }
}

/**
 * Generates an action tree from a set of actions. Effectively encodes the inputs that will be
 * provided to the ChugSplashManager contract.
 *
 * @param actions Series of DeployContract and SetStorage actions to deployment.
 * @return Merkle tree of actions.
 */
export const makeActionTree = (
  actions: ChugSplashAction[]
): ChugSplashActionTree => {
  // Turn the "nice" action structs into raw actions.
  const rawActions = actions.map((action) => {
    return toRawChugSplashAction(action)
  })

  // Now compute the hash for each action.
  const elements = rawActions.map((action) => {
    return getActionHash(action)
  })

  const tree = makeMerkleTree(elements)

  return {
    root: toHexString(tree.getRoot()),
    actions: rawActions.map((action, idx) => {
      return {
        action,
        proof: {
          actionIndex: idx,
          siblings: tree.getProof(getActionHash(action), idx).map((element) => {
            return element.data
          }),
        },
      }
    }),
  }
}

export const makeMerkleTree = (elements: string[]): MerkleTree => {
  // Pad the list of elements out with default hashes if len < a power of 2.
  const filledElements: string[] = []
  for (let i = 0; i < Math.pow(2, Math.ceil(Math.log2(elements.length))); i++) {
    if (i < elements.length) {
      filledElements.push(elements[i])
    } else {
      filledElements.push(ethers.utils.keccak256(ethers.constants.HashZero))
    }
  }

  // merkletreejs expects things to be buffers.
  return new MerkleTree(
    filledElements.map((element) => {
      return fromHexString(element)
    }),
    (el: Buffer | string): Buffer => {
      return fromHexString(ethers.utils.keccak256(el))
    }
  )
}

export const createMerkleTreesLocal = async (
  provider: providers.Provider,
  parsedConfig: ParsedChugSplashConfig,
  artifactPaths: ArtifactPaths,
  integration: Integration
): Promise<ChugSplashMerkleTrees> => {
  const artifacts: ConfigArtifacts = {}
  for (const referenceName of Object.keys(parsedConfig.contracts)) {
    const buildInfo = readBuildInfo(artifactPaths[referenceName].buildInfoPath)

    const artifact = readContractArtifact(
      artifactPaths[referenceName].contractArtifactPath,
      integration
    )
    artifacts[referenceName] = {
      buildInfo,
      artifact,
    }
  }

  return makeMerkleTreesFromConfig(provider, parsedConfig, artifacts)
}

export const makeMerkleTreesFromConfig = async (
  provider: providers.Provider,
  parsedConfig: ParsedChugSplashConfig,
  artifacts: ConfigArtifacts
): Promise<ChugSplashMerkleTrees> => {
  const actionTree = await makeActionTreeFromConfig(
    provider,
    parsedConfig,
    artifacts
  )
  const targetTree = makeTargetTreeFromConfig(parsedConfig, artifacts)
  return { actionTree, targetTree }
}

/**
 * Generates a ChugSplash action tree from a config file.
 *
 * @param config Config file to convert into a deployment.
 * @param env Environment variables to inject into the config file.
 * @returns Action tree generated from the parsed config file.
 */
export const makeActionTreeFromConfig = async (
  provider: providers.Provider,
  parsedConfig: ParsedChugSplashConfig,
  artifacts: ConfigArtifacts
): Promise<ChugSplashActionTree> => {
  const managerAddress = getChugSplashManagerAddress(
    parsedConfig.options.claimer,
    parsedConfig.options.organizationID
  )

  const actions: ChugSplashAction[] = []
  for (const [referenceName, contractConfig] of Object.entries(
    parsedConfig.contracts
  )) {
    const { buildInfo, artifact } = artifacts[referenceName]
    const { sourceName, contractName, abi, bytecode } = artifact

    // Skip adding a `DEPLOY_CONTRACT` action if the contract has already been deployed.
    if (
      (await provider.getCode(
        getContractAddress(
          managerAddress,
          contractConfig.constructorArgs,
          artifact
        )
      )) === '0x'
    ) {
      // Add a DEPLOY_CONTRACT action.
      actions.push({
        referenceName,
        addr: contractConfig.address,
        contractKindHash: contractKindHashes[contractConfig.kind],
        code: getCreationCodeWithConstructorArgs(
          bytecode,
          contractConfig.constructorArgs,
          abi
        ),
      })
    }

    const storageLayout = getStorageLayout(
      buildInfo.output,
      sourceName,
      contractName
    )
    const dereferencer = astDereferencer(buildInfo.output)
    const extendedLayout = extendStorageLayout(storageLayout, dereferencer)

    // Compute our storage segments.
    const segments = computeStorageSegments(
      extendedLayout,
      contractConfig,
      dereferencer
    )

    // Add SET_STORAGE actions for each storage slot that we want to modify.
    for (const segment of segments) {
      actions.push({
        referenceName,
        addr: contractConfig.address,
        contractKindHash: contractKindHashes[contractConfig.kind],
        key: segment.key,
        offset: segment.offset,
        value: segment.val,
      })
    }
  }

  // Generate a deployment from the list of actions.
  return makeActionTree(actions)
}

/**
 * Generates a ChugSplash target tree from a config file. Note that non-proxied contract types are
 * not included in the target tree.
 *
 * @param config Config file to convert into a deployment.
 * @param env Environment variables to inject into the config file.
 * @returns target tree generated from the parsed config file.
 */
export const makeTargetTreeFromConfig = (
  parsedConfig: ParsedChugSplashConfig,
  artifacts: ConfigArtifacts
): ChugSplashTargetTree => {
  const { projectName, organizationID, claimer } = parsedConfig.options

  const managerAddress = getChugSplashManagerAddress(claimer, organizationID)

  const targets: ChugSplashTarget[] = []
  for (const [referenceName, contractConfig] of Object.entries(
    parsedConfig.contracts
  )) {
    const { artifact } = artifacts[referenceName]

    // Only add targets for proxies.
    if (contractConfig.kind !== 'no-proxy') {
      targets.push({
        projectName,
        referenceName,
        contractKindHash: contractKindHashes[contractConfig.kind],
        addr: contractConfig.address,
        implementation: getContractAddress(
          managerAddress,
          contractConfig.constructorArgs,
          artifact
        ),
      })
    }
  }

  // Generate a deployment from the list of actions.
  return makeTargetTree(targets)
}