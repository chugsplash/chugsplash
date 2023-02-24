import {
  ChugSplashRegistryArtifact,
  ChugSplashBootLoaderArtifact,
  ChugSplashManagerProxyArtifact,
  ChugSplashManagerArtifact,
  ProxyArtifact,
  ProxyInitializerArtifact,
  DefaultAdapterArtifact,
  OZUUPSAdapterArtifact,
  DefaultUpdaterArtifact,
  OZUUPSUpdaterArtifact,
  OZTransparentAdapterArtifact,
  RegistryAdapterArtifact,
  OWNER_BOND_AMOUNT,
  EXECUTION_LOCK_TIME,
  EXECUTOR_PAYMENT_PERCENTAGE,
  CHUGSPLASH_REGISTRY_PROXY_ADDRESS,
  CHUGSPLASH_BOOTLOADER_ADDRESS,
  DEFAULT_UPDATER_ADDRESS,
  OZ_UUPS_UPDATER_ADDRESS,
  registryProxyConstructorArgValues,
  proxyInitializerConstructorArgValues,
  ChugSplashManagerABI,
  DETERMINISTIC_DEPLOYMENT_PROXY_ADDRESS,
  CHUGSPLASH_SALT,
  CHUGSPLASH_RECORDER_ADDRESS,
} from '@chugsplash/contracts'
import { utils } from 'ethers'

export const EXECUTION_BUFFER_MULTIPLIER = 2
export type Integration = 'hardhat' | 'foundry'

export const keywords = {
  preserve: '{preserve}',
}

// TODO: We should use fully qualified names instead of source names
const chugsplashRegistrySourceName = ChugSplashRegistryArtifact.sourceName
const chugsplashBootLoaderSourceName = ChugSplashBootLoaderArtifact.sourceName
const chugsplashManagerProxySourceName =
  ChugSplashManagerProxyArtifact.sourceName
const chugsplashManagerSourceName = ChugSplashManagerArtifact.sourceName
const chugsplashRegistyProxySourceName = ProxyArtifact.sourceName
const proxyInitializerSourceName = ProxyInitializerArtifact.sourceName
const defaultAdapterSourceName = DefaultAdapterArtifact.sourceName
const OZUUPSAdapterSourceName = OZUUPSAdapterArtifact.sourceName
const defaultUpdaterSourceName = DefaultUpdaterArtifact.sourceName
const OZUUPSUpdaterSourceName = OZUUPSUpdaterArtifact.sourceName
const OZTransparentAdapterSourceName = OZTransparentAdapterArtifact.sourceName
const registryAdapterSourceName = RegistryAdapterArtifact.sourceName

// TODO: All of the ChugSplash contract constructor arguments should be in this format to make it
// easy to do meta-upgrades on them later.
export const chugsplashManagerConstructorArgs = {
  _registry: CHUGSPLASH_REGISTRY_PROXY_ADDRESS,
  _recorder: CHUGSPLASH_RECORDER_ADDRESS,
  _executionLockTime: EXECUTION_LOCK_TIME,
  _ownerBondAmount: OWNER_BOND_AMOUNT.toString(),
  _executorPaymentPercentage: EXECUTOR_PAYMENT_PERCENTAGE,
}

export const CHUGSPLASH_CONSTRUCTOR_ARGS = {}
CHUGSPLASH_CONSTRUCTOR_ARGS[chugsplashRegistrySourceName] = [
  OWNER_BOND_AMOUNT,
  EXECUTION_LOCK_TIME,
  EXECUTOR_PAYMENT_PERCENTAGE,
]
CHUGSPLASH_CONSTRUCTOR_ARGS[chugsplashBootLoaderSourceName] = []
CHUGSPLASH_CONSTRUCTOR_ARGS[chugsplashManagerProxySourceName] = [
  CHUGSPLASH_REGISTRY_PROXY_ADDRESS,
  CHUGSPLASH_BOOTLOADER_ADDRESS,
]
CHUGSPLASH_CONSTRUCTOR_ARGS[chugsplashManagerSourceName] = Object.values(
  chugsplashManagerConstructorArgs
)
CHUGSPLASH_CONSTRUCTOR_ARGS[defaultAdapterSourceName] = [
  DEFAULT_UPDATER_ADDRESS,
]
CHUGSPLASH_CONSTRUCTOR_ARGS[OZUUPSAdapterSourceName] = [OZ_UUPS_UPDATER_ADDRESS]
CHUGSPLASH_CONSTRUCTOR_ARGS[OZTransparentAdapterSourceName] = [
  DEFAULT_UPDATER_ADDRESS,
]
CHUGSPLASH_CONSTRUCTOR_ARGS[defaultUpdaterSourceName] = []
CHUGSPLASH_CONSTRUCTOR_ARGS[OZUUPSUpdaterSourceName] = []
CHUGSPLASH_CONSTRUCTOR_ARGS[chugsplashRegistyProxySourceName] =
  registryProxyConstructorArgValues
CHUGSPLASH_CONSTRUCTOR_ARGS[proxyInitializerSourceName] =
  proxyInitializerConstructorArgValues
CHUGSPLASH_CONSTRUCTOR_ARGS[registryAdapterSourceName] = [
  DEFAULT_UPDATER_ADDRESS,
]

const [chugsplashManagerConstructorFragment] = ChugSplashManagerABI.filter(
  (fragment) => fragment.type === 'constructor'
)
const chugsplashManagerConstructorArgTypes =
  chugsplashManagerConstructorFragment.inputs.map((input) => input.type)
export const INITIAL_CHUGSPLASH_MANAGER_ADDRESS = utils.getCreate2Address(
  DETERMINISTIC_DEPLOYMENT_PROXY_ADDRESS,
  CHUGSPLASH_SALT,
  utils.solidityKeccak256(
    ['bytes', 'bytes'],
    [
      ChugSplashManagerArtifact.bytecode,
      utils.defaultAbiCoder.encode(
        chugsplashManagerConstructorArgTypes,
        Object.values(chugsplashManagerConstructorArgs)
      ),
    ]
  )
)
