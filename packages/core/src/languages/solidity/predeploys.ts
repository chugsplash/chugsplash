import * as path from 'path'
import assert from 'assert'

import { ethers } from 'ethers'
import {
  DETERMINISTIC_DEPLOYMENT_PROXY_ADDRESS,
  getOwnerAddress,
  OWNER_MULTISIG_ADDRESS,
  ManagedServiceArtifact,
  OZ_TRANSPARENT_PROXY_TYPE_HASH,
  OZ_UUPS_OWNABLE_PROXY_TYPE_HASH,
  OZ_UUPS_ACCESS_CONTROL_PROXY_TYPE_HASH,
  DEFAULT_PROXY_TYPE_HASH,
  EXTERNAL_TRANSPARENT_PROXY_TYPE_HASH,
  AuthFactoryABI,
} from '@chugsplash/contracts'
import { Logger } from '@eth-optimism/common-ts'

import {
  isContractDeployed,
  getGasPriceOverrides,
  getImpersonatedSigner,
  isLocalNetwork,
  getChugSplashRegistryReadOnly,
} from '../../utils'
import {
  OZ_UUPS_OWNABLE_ADAPTER_ADDRESS,
  getChugSplashManagerV1Address,
  getChugSplashRegistryAddress,
  getManagedServiceAddress,
  OZ_TRANSPARENT_ADAPTER_ADDRESS,
  DEFAULT_ADAPTER_ADDRESS,
  OZ_UUPS_ACCESS_CONTROL_ADAPTER_ADDRESS,
  AUTH_FACTORY_ADDRESS,
  AUTH_IMPL_V1_ADDRESS,
} from '../../addresses'
import {
  isSupportedNetworkOnEtherscan,
  verifyChugSplash,
} from '../../etherscan'
import { ChugSplashSystemConfig } from './types'
import {
  PROTOCOL_PAYMENT_RECIPIENT_ROLE,
  REMOTE_EXECUTOR_ROLE,
} from '../../constants'
import { resolveNetworkName } from '../../messages'
import { assertValidBlockGasLimit } from '../../config/parse'
import { getChugSplashConstants } from '../../contract-info'

const fetchChugSplashSystemConfig = (configPath: string) => {
  delete require.cache[require.resolve(path.resolve(configPath))]

  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const exported: ChugSplashSystemConfig = require(path.resolve(
    configPath
  )).default
  if (
    typeof exported === 'object' &&
    exported.executors.length > 0 &&
    exported.relayers.length > 0
  ) {
    return exported
  } else {
    throw new Error(
      'Config file must export a valid config object with a list of executors and relayers.'
    )
  }
}

export const initializeAndVerifyChugSplash = async (
  systemConfigPath: string,
  provider: ethers.providers.JsonRpcProvider
) => {
  const config = fetchChugSplashSystemConfig(systemConfigPath)

  const logger = new Logger({
    name: 'deploy',
  })

  // Deploy Contracts
  await initializeChugSplash(
    provider,
    await provider.getSigner(),
    config.executors,
    config.relayers,
    (
      await provider.getNetwork()
    ).chainId,
    logger
  )

  // Verify ChugSplash contracts on etherscan
  try {
    // Verify the ChugSplash contracts if the current network is supported.
    if (
      isSupportedNetworkOnEtherscan(
        await resolveNetworkName(provider, 'hardhat')
      )
    ) {
      const apiKey = process.env.ETHERSCAN_API_KEY
      if (apiKey) {
        logger.info(
          '[ChugSplash]: attempting to verify the chugsplash contracts...'
        )
        await verifyChugSplash(provider, provider.network.name, apiKey)
        logger.info(
          '[ChugSplash]: finished attempting to verify the chugsplash contracts'
        )
      } else {
        logger.info(
          `[ChugSplash]: skipped verifying chugsplash contracts. reason: no api key found`
        )
      }
    } else {
      logger.info(
        `[ChugSplash]: skipped verifying chugsplash contracts. reason: etherscan config not detected for: ${provider.network.name}`
      )
    }
  } catch (e) {
    logger.error(
      `[ChugSplash]: error: failed to verify chugsplash contracts on ${provider.network.name}`,
      e
    )
  }
}

/**
 * @notice Ensures that the ChugSplash contracts are deployed and initialized. This will only send
 * transactions from the signer if the provider is a local, non-forked network. The signer will
 * never be used to send transactions on a live network.
 */
export const ensureChugSplashInitialized = async (
  provider: ethers.providers.JsonRpcProvider,
  signer: ethers.Signer,
  executors: string[] = [],
  relayers: string[] = [],
  logger?: Logger
) => {
  if (await isContractDeployed(getChugSplashRegistryAddress(), provider)) {
    return
  } else if (await isLocalNetwork(provider)) {
    await initializeChugSplash(
      provider,
      signer,
      executors,
      relayers,
      (
        await provider.getNetwork()
      ).chainId,
      logger
    )
  } else {
    const { name } = await provider.getNetwork()
    throw new Error(
      `ChugSplash is not supported on ${name} yet. Reach out on Discord if you'd like us to support it!`
    )
  }
}

export const initializeChugSplash = async (
  provider: ethers.providers.JsonRpcProvider,
  deployer: ethers.Signer,
  executors: string[],
  relayers: string[],
  chainId: number,
  logger?: Logger
): Promise<void> => {
  const { gasLimit: blockGasLimit } = await provider.getBlock('latest')
  assertValidBlockGasLimit(blockGasLimit)

  for (const {
    artifact,
    constructorArgs,
    expectedAddress,
  } of getChugSplashConstants(chainId)) {
    const { abi, bytecode, contractName } = artifact

    logger?.info(`[ChugSplash]: deploying ${contractName}...`)

    const contract = await doDeterministicDeploy(provider, {
      signer: deployer,
      contract: {
        abi,
        bytecode,
      },
      args: constructorArgs,
      salt: ethers.constants.HashZero,
    })

    assert(
      contract.address === expectedAddress,
      `address mismatch for ${contractName}`
    )

    logger?.info(`[ChugSplash]: deployed ${contractName}`)
  }

  logger?.info(`[ChugSplash]: finished deploying ChugSplash contracts`)

  // We need to do some additional setup: adding the manager version, adding executor roles, etc
  // This requires a signer with the owner role which we have to handle differently depending on the situation.
  // 1. If the owner is the multisig and we're deploying on a test node then we can use an impersonated signer.
  // 2. If the owner is the multisig and we're deploying on a live network then we have to use the gnosis safe ethers adapter (which we have not implemented yet).
  // 3. We also allow the user to specify a different owner via process.env.CHUGSPLASH_INTERNAL__OWNER_PRIVATE_KEY. This is useful for testing on live networks without using the multisig.
  //    In this case, we need to create a signer using the CHUGSPLASH_INTERNAL__OWNER_PRIVATE_KEY and use that.
  let signer: ethers.Signer

  // If deploying on a live network and the target owner is the multisig, then throw an error because
  // we have not setup the safe ethers adapter yet.
  const localNetwork = await isLocalNetwork(provider)
  if (!localNetwork && getOwnerAddress() === OWNER_MULTISIG_ADDRESS) {
    if (!process.env.CHUGSPLASH_INTERNAL__OWNER_PRIVATE_KEY) {
      throw new Error('Must define CHUGSPLASH_INTERNAL__OWNER_PRIVATE_KEY')
    }

    signer = new ethers.Wallet(
      process.env.CHUGSPLASH_INTERNAL__OWNER_PRIVATE_KEY!,
      provider
    )
  } else {
    // if target owner is multisig, then use an impersonated multisig signer
    if (getOwnerAddress() === OWNER_MULTISIG_ADDRESS) {
      signer = await getImpersonatedSigner(OWNER_MULTISIG_ADDRESS, provider)
    } else {
      // if target owner is not multisig, then use the owner signer
      // CHUGSPLASH_INTERNAL__OWNER_PRIVATE_KEY will always be defined if the OWNER_ADDRESS is not the OWNER_MULTISIG_ADDRESS
      signer = new ethers.Wallet(
        process.env.CHUGSPLASH_INTERNAL__OWNER_PRIVATE_KEY!,
        provider
      )
    }

    if (localNetwork) {
      // Fund the signer
      await (
        await deployer.sendTransaction({
          to: await signer.getAddress(),
          value: ethers.utils.parseEther('0.1'),
        })
      ).wait()
    }
  }

  const ManagedService = new ethers.Contract(
    getManagedServiceAddress(),
    ManagedServiceArtifact.abi,
    signer
  )

  logger?.info('[ChugSplash]: assigning executor roles...')
  for (const executor of executors) {
    if (
      (await ManagedService.hasRole(REMOTE_EXECUTOR_ROLE, executor)) === false
    ) {
      await (
        await ManagedService.connect(signer).grantRole(
          REMOTE_EXECUTOR_ROLE,
          executor,
          await getGasPriceOverrides(provider)
        )
      ).wait()
    }
  }
  logger?.info('[ChugSplash]: finished assigning executor roles')

  logger?.info('[ChugSplash]: assigning caller roles...')
  for (const relayer of relayers) {
    if (
      (await ManagedService.hasRole(
        PROTOCOL_PAYMENT_RECIPIENT_ROLE,
        relayer
      )) === false
    ) {
      await (
        await ManagedService.connect(signer).grantRole(
          PROTOCOL_PAYMENT_RECIPIENT_ROLE,
          relayer,
          await getGasPriceOverrides(provider)
        )
      ).wait()
    }
  }
  logger?.info('[ChugSplash]: finished assigning caller roles')

  logger?.info('[ChugSplash]: adding the initial ChugSplashManager version...')

  const ChugSplashRegistry = getChugSplashRegistryReadOnly(provider)
  const chugSplashManagerV1Address = getChugSplashManagerV1Address()
  if (
    (await ChugSplashRegistry.managerImplementations(
      chugSplashManagerV1Address
    )) === false
  ) {
    try {
      await (
        await ChugSplashRegistry.connect(signer).addVersion(
          chugSplashManagerV1Address,
          await getGasPriceOverrides(provider)
        )
      ).wait()
    } catch (e) {
      if (!e.message.includes('version already set')) {
        throw e
      }
    }
  }

  logger?.info('[ChugSplash]: added the initial ChugSplashManager version')

  logger?.info('[ChugSplash]: setting the default ChugSplashManager version')

  if (
    (await ChugSplashRegistry.currentManagerImplementation()) !==
    chugSplashManagerV1Address
  ) {
    await (
      await ChugSplashRegistry.connect(signer).setCurrentManagerImplementation(
        chugSplashManagerV1Address,
        await getGasPriceOverrides(provider)
      )
    ).wait()
  }

  logger?.info('[ChugSplash]: set the default ChugSplashManager version')

  logger?.info('[ChugSplash]: setting the default ChugSplashAuth version')

  const AuthFactory = new ethers.Contract(
    AUTH_FACTORY_ADDRESS,
    AuthFactoryABI,
    signer
  )

  if (!(await AuthFactory.authImplementations(AUTH_IMPL_V1_ADDRESS))) {
    await (
      await AuthFactory.addVersion(
        AUTH_IMPL_V1_ADDRESS,
        await getGasPriceOverrides(provider)
      )
    ).wait()
  }

  if (
    (await AuthFactory.currentAuthImplementation()) !== AUTH_IMPL_V1_ADDRESS
  ) {
    await (
      await AuthFactory.setCurrentAuthImplementation(
        AUTH_IMPL_V1_ADDRESS,
        await getGasPriceOverrides(provider)
      )
    ).wait()
  }

  logger?.info('[ChugSplash]: set the default ChugSplashAuth version')

  logger?.info(
    '[ChugSplash]: adding the default proxy type to the ChugSplashRegistry...'
  )

  // Set the oz transparent proxy type on the registry.
  const transparentAdapterAddress = OZ_TRANSPARENT_ADAPTER_ADDRESS
  if (
    (await ChugSplashRegistry.adapters(OZ_TRANSPARENT_PROXY_TYPE_HASH)) !==
    transparentAdapterAddress
  ) {
    await (
      await ChugSplashRegistry.connect(signer).addContractKind(
        OZ_TRANSPARENT_PROXY_TYPE_HASH,
        transparentAdapterAddress,
        await getGasPriceOverrides(provider)
      )
    ).wait()
    logger?.info(
      '[ChugSplash]: added the transparent proxy type to the ChugSplashRegistry'
    )
  } else {
    logger?.info(
      '[ChugSplash]: the transparent proxy type was already added to the ChugSplashRegistry'
    )
  }

  logger?.info(
    '[ChugSplash]: adding the uups proxy type to the ChugSplashRegistry...'
  )

  // Set the oz uups proxy type on the registry.
  const uupsOwnableAdapterAddress = OZ_UUPS_OWNABLE_ADAPTER_ADDRESS
  if (
    (await ChugSplashRegistry.adapters(OZ_UUPS_OWNABLE_PROXY_TYPE_HASH)) !==
    uupsOwnableAdapterAddress
  ) {
    await (
      await ChugSplashRegistry.connect(signer).addContractKind(
        OZ_UUPS_OWNABLE_PROXY_TYPE_HASH,
        uupsOwnableAdapterAddress,
        await getGasPriceOverrides(provider)
      )
    ).wait()
    logger?.info(
      '[ChugSplash]: added the uups ownable proxy type to the ChugSplashRegistry'
    )
  } else {
    logger?.info(
      '[ChugSplash]: the uups ownable proxy type was already added to the ChugSplashRegistry'
    )
  }

  // Set the oz uups proxy type on the registry.
  const ozUUPSAccessControlAdapterAddress =
    OZ_UUPS_ACCESS_CONTROL_ADAPTER_ADDRESS
  if (
    (await ChugSplashRegistry.adapters(
      OZ_UUPS_ACCESS_CONTROL_PROXY_TYPE_HASH
    )) !== ozUUPSAccessControlAdapterAddress
  ) {
    await (
      await ChugSplashRegistry.connect(signer).addContractKind(
        OZ_UUPS_ACCESS_CONTROL_PROXY_TYPE_HASH,
        ozUUPSAccessControlAdapterAddress,
        await getGasPriceOverrides(provider)
      )
    ).wait()
    logger?.info(
      '[ChugSplash]: added the uups access control proxy type to the ChugSplashRegistry'
    )
  } else {
    logger?.info(
      '[ChugSplash]: the uups access control proxy type was already added to the ChugSplashRegistry'
    )
  }

  const defaultAdapterAddress = DEFAULT_ADAPTER_ADDRESS
  if (
    (await ChugSplashRegistry.adapters(
      EXTERNAL_TRANSPARENT_PROXY_TYPE_HASH
    )) !== defaultAdapterAddress
  ) {
    await (
      await ChugSplashRegistry.connect(signer).addContractKind(
        EXTERNAL_TRANSPARENT_PROXY_TYPE_HASH,
        defaultAdapterAddress,
        await getGasPriceOverrides(provider)
      )
    ).wait()
    logger?.info(
      '[ChugSplash]: added the external default proxy type to the ChugSplashRegistry'
    )
  } else {
    logger?.info(
      '[ChugSplash]: the external default proxy type was already added to the ChugSplashRegistry'
    )
  }

  if (
    (await ChugSplashRegistry.adapters(DEFAULT_PROXY_TYPE_HASH)) !==
    defaultAdapterAddress
  ) {
    await (
      await ChugSplashRegistry.connect(signer).addContractKind(
        ethers.constants.HashZero,
        defaultAdapterAddress,
        await getGasPriceOverrides(provider)
      )
    ).wait()
    logger?.info(
      '[ChugSplash]: added the internal default proxy type to the ChugSplashRegistry'
    )
  } else {
    logger?.info(
      '[ChugSplash]: the internal default proxy type was already added to the ChugSplashRegistry'
    )
  }
}

export const getDeterministicFactoryAddress = async (
  provider: ethers.providers.JsonRpcProvider
) => {
  // Deploy the deterministic deployer.
  if (
    (await isContractDeployed(
      DETERMINISTIC_DEPLOYMENT_PROXY_ADDRESS,
      provider
    )) === false
  ) {
    const sender = '0x3fab184622dc19b6109349b94811493bf2a45362'

    // Try to fund the sender account. Will work if we're running against a local hardhat node. If
    // we're not running against hardhat then this will fail silently. We'll just need to try the
    // deployment and see if the sender has enough funds to pay for the deployment.
    try {
      await provider.send('hardhat_setBalance', [
        sender,
        '0xFFFFFFFFFFFFFFFFFFFFFF',
      ])
    } catch {
      // Ignore.
    }

    // Send the raw deployment transaction for the deterministic deployer.
    try {
      await provider.waitForTransaction(
        await provider.send('eth_sendRawTransaction', [
          '0xf8a58085174876e800830186a08080b853604580600e600039806000f350fe7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe03601600081602082378035828234f58015156039578182fd5b8082525050506014600cf31ba02222222222222222222222222222222222222222222222222222222222222222a02222222222222222222222222222222222222222222222222222222222222222',
        ])
      )
    } catch (err) {
      if (err.message.includes('insufficient balance')) {
        throw new Error(
          `insufficient balance to deploy deterministic deployer, please fund the sender: ${sender}`
        )
      } else {
        throw err
      }
    }
  }

  return DETERMINISTIC_DEPLOYMENT_PROXY_ADDRESS
}

export const doDeterministicDeploy = async (
  provider: ethers.providers.JsonRpcProvider,
  options: {
    contract: {
      abi: any
      bytecode: string
    }
    salt: string
    signer: ethers.Signer
    args?: any[]
  }
): Promise<ethers.Contract> => {
  const factory = new ethers.ContractFactory(
    options.contract.abi,
    options.contract.bytecode
  )
  const deployer = await getDeterministicFactoryAddress(provider)

  const deploymentTx = factory.getDeployTransaction(...(options.args || []))
  if (deploymentTx.data === undefined) {
    throw new Error(`Deployment transaction data is undefined`)
  }

  const address = ethers.utils.getCreate2Address(
    deployer,
    options.salt,
    ethers.utils.keccak256(deploymentTx.data)
  )

  // Short circuit if already deployed.
  if (await isContractDeployed(address, provider)) {
    return new ethers.Contract(address, options.contract.abi, options.signer)
  }

  // Create a transaction request with gas price overrides.
  const txnRequest = await getGasPriceOverrides(provider, {
    to: deployer,
    data: options.salt + ethers.utils.hexlify(deploymentTx.data).slice(2),
  })

  // Deploy the contract.
  await (await options.signer.sendTransaction(txnRequest)).wait()

  if ((await isContractDeployed(address, provider)) === false) {
    throw new Error(`failed to deploy contract at ${address}`)
  }

  return new ethers.Contract(address, options.contract.abi, options.signer)
}
