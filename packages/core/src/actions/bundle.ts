import { fromHexString, toHexString } from '@eth-optimism/core-utils'
import { ethers } from 'ethers'
import MerkleTree from 'merkletreejs'
import { astDereferencer } from 'solidity-ast/utils'

import {
  ConfigArtifacts,
  ConfigCache,
  ParsedChugSplashConfig,
  contractKindHashes,
} from '../config/types'
import {
  computeStorageSegments,
  extendStorageLayout,
} from '../languages/solidity/storage'
import {
  getCreationCodeWithConstructorArgs,
  getImplAddress,
  getDefaultProxyInitCode,
} from '../utils'
import {
  BundledChugSplashAction,
  ChugSplashAction,
  ChugSplashActionBundle,
  ChugSplashActionType,
  ChugSplashBundles,
  ChugSplashTarget,
  ChugSplashTargetBundle,
  DeployContractAction,
  RawChugSplashAction,
  SetStorageAction,
} from './types'
import { getStorageLayout } from './artifacts'
import { getChugSplashManagerAddress } from '../addresses'
import { getCreate3Address } from '../config/utils'

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

export const getDeployContractActions = (
  actionBundle: ChugSplashActionBundle
): Array<DeployContractAction> => {
  return actionBundle.actions
    .map((action) => fromRawChugSplashAction(action.action))
    .filter(isDeployContractAction)
}

export const getDeployContractActionBundle = (
  actionBundle: ChugSplashActionBundle
): Array<BundledChugSplashAction> => {
  return actionBundle.actions.filter((action) =>
    isDeployContractAction(fromRawChugSplashAction(action.action))
  )
}

export const getSetStorageActionBundle = (
  actionBundle: ChugSplashActionBundle
): Array<BundledChugSplashAction> => {
  return actionBundle.actions.filter((action) =>
    isSetStorageAction(fromRawChugSplashAction(action.action))
  )
}

export const getNumDeployContractActions = (
  actionBundle: ChugSplashActionBundle
): number => {
  return getDeployContractActionBundle(actionBundle).length
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
      data: ethers.utils.defaultAbiCoder.encode(
        ['bytes32', 'bytes'],
        [action.salt, action.code]
      ),
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
    const [salt, code] = ethers.utils.defaultAbiCoder.decode(
      ['bytes32', 'bytes'],
      rawAction.data
    )
    return {
      referenceName: rawAction.referenceName,
      addr: rawAction.addr,
      contractKindHash: rawAction.contractKindHash,
      salt,
      code,
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

export const makeTargetBundle = (
  targets: ChugSplashTarget[]
): ChugSplashTargetBundle => {
  // Compute the hash for each action.
  const elements = targets.map((target) => {
    return getTargetHash(target)
  })

  const tree = makeMerkleTree(elements)

  const root = toHexString(tree.getRoot())

  return {
    root: root !== '0x' ? root : ethers.constants.HashZero,
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
 * Generates an action bundle from a set of actions. Effectively encodes the inputs that will be
 * provided to the ChugSplashManager contract.
 *
 * @param actions Series of DeployContract and SetStorage actions to bundle.
 * @return Bundled actions.
 */
export const makeActionBundle = (
  actions: ChugSplashAction[]
): ChugSplashActionBundle => {
  // Turn the "nice" action structs into raw actions.
  const rawActions = actions.map((action) => {
    return toRawChugSplashAction(action)
  })

  // Now compute the hash for each action.
  const elements = rawActions.map((action) => {
    return getActionHash(action)
  })

  const tree = makeMerkleTree(elements)

  const root = toHexString(tree.getRoot())

  return {
    root: root !== '0x' ? root : ethers.constants.HashZero,
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

export const makeBundlesFromConfig = (
  parsedConfig: ParsedChugSplashConfig,
  artifacts: ConfigArtifacts,
  configCache: ConfigCache
): ChugSplashBundles => {
  const actionBundle = makeActionBundleFromConfig(
    parsedConfig,
    artifacts,
    configCache
  )
  const targetBundle = makeTargetBundleFromConfig(parsedConfig, artifacts)
  return { actionBundle, targetBundle }
}

/**
 * Generates a ChugSplash action bundle from a config file.
 *
 * @param config Config file to convert into a bundle.
 * @param env Environment variables to inject into the config file.
 * @returns Action bundle generated from the parsed config file.
 */
export const makeActionBundleFromConfig = (
  parsedConfig: ParsedChugSplashConfig,
  artifacts: ConfigArtifacts,
  configCache: ConfigCache
): ChugSplashActionBundle => {
  const actions: ChugSplashAction[] = []
  for (const [referenceName, contractConfig] of Object.entries(
    parsedConfig.contracts
  )) {
    const { buildInfo, artifact } = artifacts[referenceName]
    const { sourceName, contractName, abi, bytecode } = artifact
    const { isTargetDeployed } = configCache.contractConfigCache[referenceName]
    const { kind, address, salt, constructorArgs } = contractConfig
    const managerAddress = getChugSplashManagerAddress(
      parsedConfig.options.organizationID
    )

    if (!isTargetDeployed) {
      if (kind === 'immutable') {
        // Add a DEPLOY_CONTRACT action for the unproxied contract.
        actions.push({
          referenceName,
          addr: address,
          contractKindHash: contractKindHashes[kind],
          salt,
          code: getCreationCodeWithConstructorArgs(
            bytecode,
            constructorArgs,
            abi
          ),
        })
      } else if (kind === 'proxy') {
        // Add a DEPLOY_CONTRACT action for the default proxy.
        actions.push({
          referenceName,
          addr: address,
          contractKindHash: contractKindHashes[kind],
          salt,
          code: getDefaultProxyInitCode(managerAddress),
        })
      } else {
        throw new Error(
          `${referenceName} is not deployed. Should never happen.`
        )
      }
    }

    if (kind !== 'immutable') {
      // Add a DEPLOY_CONTRACT action for the proxy's implementation. Note that it may be possible
      // for the implementation to be deployed already. We don't check for that here because this
      // would slow down the Foundry plugin's FFI call to retrieve the MinimalConfig, since we would
      // need to run the parsing logic in order to get the implementation's constructor args and
      // bytecode.

      const implInitCode = getCreationCodeWithConstructorArgs(
        bytecode,
        constructorArgs,
        abi
      )
      // We use a 'salt' value that's a hash of the implementation contract's init code. This
      // essentially mimics the behavior of Create2 in the sense that the implementation's address
      // has a one-to-one mapping with its init code. This allows us to skip deploying implementation
      // contracts that have already been deployed.
      const implSalt = ethers.utils.keccak256(implInitCode)
      const implAddress = getCreate3Address(managerAddress, implSalt)

      actions.push({
        referenceName,
        addr: implAddress,
        contractKindHash: contractKindHashes['immutable'],
        salt: implSalt,
        code: implInitCode,
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
        addr: address,
        contractKindHash: contractKindHashes[kind],
        key: segment.key,
        offset: segment.offset,
        value: segment.val,
      })
    }
  }

  // Generate a bundle from the list of actions.
  return makeActionBundle(actions)
}

/**
 * Generates a ChugSplash target bundle from a config file. Note that non-proxied contract types are
 * not included in the target bundle.
 *
 * @param config Config file to convert into a bundle.
 * @param env Environment variables to inject into the config file.
 * @returns Target bundle generated from the parsed config file.
 */
export const makeTargetBundleFromConfig = (
  parsedConfig: ParsedChugSplashConfig,
  configArtifacts: ConfigArtifacts
): ChugSplashTargetBundle => {
  const { projectName, organizationID } = parsedConfig.options

  const managerAddress = getChugSplashManagerAddress(organizationID)

  const targets: ChugSplashTarget[] = []
  for (const [referenceName, contractConfig] of Object.entries(
    parsedConfig.contracts
  )) {
    const { abi, bytecode } = configArtifacts[referenceName].artifact

    // Only add targets for proxies.
    if (contractConfig.kind !== 'immutable') {
      targets.push({
        projectName,
        referenceName,
        contractKindHash: contractKindHashes[contractConfig.kind],
        addr: contractConfig.address,
        implementation: getImplAddress(
          managerAddress,
          bytecode,
          contractConfig.constructorArgs,
          abi
        ),
      })
    }
  }

  // Generate a bundle from the list of actions.
  return makeTargetBundle(targets)
}
