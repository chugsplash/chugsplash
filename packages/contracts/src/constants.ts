import { ethers } from 'ethers'

import {
  ProxyArtifact,
  DefaultAdapterArtifact,
  ProxyUpdaterArtifact,
  ChugSplashBootLoaderArtifact,
  ChugSplashRegistryArtifact,
  ChugSplashManagerArtifact,
  ChugSplashManagerProxyArtifact,
  ChugSplashManagerABI,
  ProxyABI,
  ProxyInitializerArtifact,
  ProxyInitializerABI,
  ReverterArtifact,
} from './ifaces'

export const OWNER_MULTISIG_ADDRESS =
  '0xF2a21e4E9F22AAfD7e8Bf47578a550b4102732a9'
export const EXECUTOR = '0x42761facf5e6091fca0e38f450adfb1e22bd8c3c'

export const TRANSPARENT_PROXY_TYPE_HASH = ethers.utils.keccak256(
  ethers.utils.toUtf8Bytes('transparent')
)

export const CHUGSPLASH_SALT = ethers.constants.HashZero

const chugsplashRegistrySourceName = ChugSplashRegistryArtifact.sourceName
const chugsplashBootLoaderSourceName = ChugSplashBootLoaderArtifact.sourceName
const chugsplashManagerProxySourceName =
  ChugSplashManagerProxyArtifact.sourceName
const chugsplashManagerSourceName = ChugSplashManagerArtifact.sourceName
const proxyUpdaterSourceName = ProxyUpdaterArtifact.sourceName
const defaultAdapterSourceName = DefaultAdapterArtifact.sourceName
const chugsplashRegistyProxySourceName = ProxyArtifact.sourceName
const proxyInitializerSourceName = ProxyInitializerArtifact.sourceName
const reverterSourceName = ReverterArtifact.sourceName

const [proxyInitializerConstructorFragment] = ProxyInitializerABI.filter(
  (fragment) => fragment.type === 'constructor'
)
const proxyInitializerConstructorArgTypes =
  proxyInitializerConstructorFragment.inputs.map((input) => input.type)
const proxyInitializerConstructorArgValues = [
  OWNER_MULTISIG_ADDRESS,
  CHUGSPLASH_SALT,
]

const [chugsplashManagerConstructorFragment] = ChugSplashManagerABI.filter(
  (fragment) => fragment.type === 'constructor'
)
const chugsplashManagerConstructorArgTypes =
  chugsplashManagerConstructorFragment.inputs.map((input) => input.type)

export const DETERMINISTIC_DEPLOYMENT_PROXY_ADDRESS =
  '0x4e59b44847b379578588920ca78fbf26c0b4956c'
export const OWNER_BOND_AMOUNT = ethers.utils.parseEther('0.001')
export const EXECUTION_LOCK_TIME = 15 * 60
export const EXECUTOR_PAYMENT_PERCENTAGE = 20

export const CHUGSPLASH_BOOTLOADER_ADDRESS = ethers.utils.getCreate2Address(
  DETERMINISTIC_DEPLOYMENT_PROXY_ADDRESS,
  CHUGSPLASH_SALT,
  ethers.utils.solidityKeccak256(
    ['bytes'],
    [ChugSplashBootLoaderArtifact.bytecode]
  )
)

export const PROXY_UPDATER_ADDRESS = ethers.utils.getCreate2Address(
  CHUGSPLASH_BOOTLOADER_ADDRESS,
  CHUGSPLASH_SALT,
  ethers.utils.solidityKeccak256(['bytes'], [ProxyUpdaterArtifact.bytecode])
)

export const REVERTER_ADDRESS = ethers.utils.getCreate2Address(
  CHUGSPLASH_BOOTLOADER_ADDRESS,
  CHUGSPLASH_SALT,
  ethers.utils.solidityKeccak256(['bytes'], [ReverterArtifact.bytecode])
)

export const PROXY_INITIALIZER_ADDRESS = ethers.utils.getCreate2Address(
  DETERMINISTIC_DEPLOYMENT_PROXY_ADDRESS,
  CHUGSPLASH_SALT,
  ethers.utils.solidityKeccak256(
    ['bytes', 'bytes'],
    [
      ProxyInitializerArtifact.bytecode,
      ethers.utils.defaultAbiCoder.encode(
        proxyInitializerConstructorArgTypes,
        proxyInitializerConstructorArgValues
      ),
    ]
  )
)

const [registryProxyConstructorFragment] = ProxyABI.filter(
  (fragment) => fragment.type === 'constructor'
)
const registryProxyConstructorArgTypes =
  registryProxyConstructorFragment.inputs.map((input) => input.type)
const registryProxyConstructorArgValues = [PROXY_INITIALIZER_ADDRESS]

export const CHUGSPLASH_REGISTRY_PROXY_ADDRESS = ethers.utils.getCreate2Address(
  PROXY_INITIALIZER_ADDRESS,
  CHUGSPLASH_SALT,
  ethers.utils.solidityKeccak256(
    ['bytes', 'bytes'],
    [
      ProxyArtifact.bytecode,
      ethers.utils.defaultAbiCoder.encode(
        registryProxyConstructorArgTypes,
        registryProxyConstructorArgValues
      ),
    ]
  )
)

export const ROOT_CHUGSPLASH_MANAGER_PROXY_ADDRESS =
  ethers.utils.getCreate2Address(
    CHUGSPLASH_BOOTLOADER_ADDRESS,
    CHUGSPLASH_SALT,
    ethers.utils.solidityKeccak256(
      ['bytes', 'bytes'],
      [
        ChugSplashManagerProxyArtifact.bytecode,
        ethers.utils.defaultAbiCoder.encode(
          ['address', 'address'],
          [CHUGSPLASH_REGISTRY_PROXY_ADDRESS, CHUGSPLASH_BOOTLOADER_ADDRESS]
        ),
      ]
    )
  )

const chugsplashManagerConstructorArgValues = [
  CHUGSPLASH_REGISTRY_PROXY_ADDRESS,
  'Root Manager',
  OWNER_MULTISIG_ADDRESS,
  PROXY_UPDATER_ADDRESS,
  EXECUTION_LOCK_TIME,
  OWNER_BOND_AMOUNT,
  EXECUTOR_PAYMENT_PERCENTAGE,
]

export const CHUGSPLASH_MANAGER_ADDRESS = ethers.utils.getCreate2Address(
  DETERMINISTIC_DEPLOYMENT_PROXY_ADDRESS,
  CHUGSPLASH_SALT,
  ethers.utils.solidityKeccak256(
    ['bytes', 'bytes'],
    [
      ChugSplashManagerArtifact.bytecode,
      ethers.utils.defaultAbiCoder.encode(
        chugsplashManagerConstructorArgTypes,
        chugsplashManagerConstructorArgValues
      ),
    ]
  )
)

export const CHUGSPLASH_REGISTRY_ADDRESS = ethers.utils.getCreate2Address(
  CHUGSPLASH_BOOTLOADER_ADDRESS,
  CHUGSPLASH_SALT,
  ethers.utils.solidityKeccak256(
    ['bytes', 'bytes'],
    [
      ChugSplashRegistryArtifact.bytecode,
      ethers.utils.defaultAbiCoder.encode(
        ['address', 'address', 'uint256', 'uint256', 'uint256', 'address'],
        [
          PROXY_UPDATER_ADDRESS,
          REVERTER_ADDRESS,
          OWNER_BOND_AMOUNT,
          EXECUTION_LOCK_TIME,
          EXECUTOR_PAYMENT_PERCENTAGE,
          CHUGSPLASH_MANAGER_ADDRESS,
        ]
      ),
    ]
  )
)

export const DEFAULT_ADAPTER_ADDRESS = ethers.utils.getCreate2Address(
  DETERMINISTIC_DEPLOYMENT_PROXY_ADDRESS,
  CHUGSPLASH_SALT,
  ethers.utils.solidityKeccak256(['bytes'], [DefaultAdapterArtifact.bytecode])
)

export const CHUGSPLASH_CONSTRUCTOR_ARGS = {}
CHUGSPLASH_CONSTRUCTOR_ARGS[chugsplashRegistrySourceName] = [
  PROXY_UPDATER_ADDRESS,
  OWNER_BOND_AMOUNT,
  EXECUTION_LOCK_TIME,
  EXECUTOR_PAYMENT_PERCENTAGE,
  CHUGSPLASH_MANAGER_ADDRESS,
]
CHUGSPLASH_CONSTRUCTOR_ARGS[chugsplashBootLoaderSourceName] = []
CHUGSPLASH_CONSTRUCTOR_ARGS[chugsplashManagerProxySourceName] = [
  CHUGSPLASH_REGISTRY_PROXY_ADDRESS,
  CHUGSPLASH_BOOTLOADER_ADDRESS,
]
CHUGSPLASH_CONSTRUCTOR_ARGS[chugsplashManagerSourceName] =
  chugsplashManagerConstructorArgValues
CHUGSPLASH_CONSTRUCTOR_ARGS[proxyUpdaterSourceName] = []
CHUGSPLASH_CONSTRUCTOR_ARGS[defaultAdapterSourceName] = []
CHUGSPLASH_CONSTRUCTOR_ARGS[chugsplashRegistyProxySourceName] =
  registryProxyConstructorArgValues
CHUGSPLASH_CONSTRUCTOR_ARGS[proxyInitializerSourceName] =
  proxyInitializerConstructorArgValues
CHUGSPLASH_CONSTRUCTOR_ARGS[reverterSourceName] = []
