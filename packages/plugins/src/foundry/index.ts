import * as fs from 'fs'

import {
  chugsplashApproveAbstractTask,
  chugsplashDeployAbstractTask,
  chugsplashFundAbstractTask,
  chugsplashProposeAbstractTask,
  chugsplashRegisterAbstractTask,
  monitorChugSplashSetup,
  ChugSplashExecutorType,
  chugsplashMonitorAbstractTask,
  chugsplashAddProposersAbstractTask,
  chugsplashWithdrawAbstractTask,
  chugsplashListProjectsAbstractTask,
  chugsplashListProposersAbstractTask,
  chugsplashCancelAbstractTask,
  chugsplashClaimProxyAbstractTask,
  chugsplashTransferOwnershipAbstractTask,
  getEIP1967ProxyAdminAddress,
  initializeChugSplash,
  readValidatedChugSplashConfig,
  getDefaultProxyAddress,
  readUnvalidatedChugSplashConfig,
} from '@chugsplash/core'
import { BigNumber, ethers } from 'ethers'
import ora from 'ora'
import { CHUGSPLASH_REGISTRY_PROXY_ADDRESS } from '@chugsplash/contracts'

import { cleanPath, fetchPaths, getArtifactPaths } from './utils'
import { initializeExecutor } from '../executor'
import { createChugSplashRuntime } from '../utils'

const args = process.argv.slice(2)
const command = args[0]

;(async () => {
  switch (command) {
    case 'register': {
      const configPath = args[1]
      const rpcUrl = args[2]
      const network = args[3] !== 'localhost' ? args[3] : undefined
      const privateKey = args[4]
      const silent = args[5] === 'true'
      const outPath = cleanPath(args[6])
      const buildInfoPath = cleanPath(args[7])
      let owner = args[8]
      const allowManagedProposals = args[9] === 'true'

      const cre = await createChugSplashRuntime(
        configPath,
        args[3] !== 'localhost',
        true,
        undefined,
        silent,
        process.stdout
      )

      const provider = new ethers.providers.JsonRpcProvider(rpcUrl, network)
      const wallet = new ethers.Wallet(privateKey, provider)

      const { artifactFolder, buildInfoFolder } = fetchPaths(
        outPath,
        buildInfoPath
      )
      const userConfig = await readUnvalidatedChugSplashConfig(configPath)
      const artifactPaths = await getArtifactPaths(
        userConfig.contracts,
        artifactFolder,
        buildInfoFolder
      )

      const config = await readValidatedChugSplashConfig(
        provider,
        configPath,
        artifactPaths,
        'foundry',
        cre
      )

      await provider.getNetwork()
      const address = await wallet.getAddress()
      owner = owner !== 'self' ? owner : address

      if (!silent) {
        console.log('-- ChugSplash Register --')
      }
      await chugsplashRegisterAbstractTask(
        provider,
        wallet,
        config,
        allowManagedProposals,
        owner,
        'foundry',
        cre
      )
      break
    }
    case 'propose': {
      const configPath = args[1]
      const rpcUrl = args[2]
      const network = args[3] !== 'localhost' ? args[3] : undefined
      const privateKey = args[4]
      const silent = args[5] === 'true'
      const outPath = cleanPath(args[6])
      const buildInfoPath = cleanPath(args[7])
      const ipfsUrl = args[8] !== 'none' ? args[8] : ''
      const remoteExecution = args[9] === 'true'

      const cre = await createChugSplashRuntime(
        configPath,
        args[3] !== 'localhost',
        true,
        undefined,
        silent,
        process.stdout
      )

      const { artifactFolder, buildInfoFolder, canonicalConfigPath } =
        fetchPaths(outPath, buildInfoPath)
      const userConfig = await readUnvalidatedChugSplashConfig(configPath)
      const artifactPaths = await getArtifactPaths(
        userConfig.contracts,
        artifactFolder,
        buildInfoFolder
      )

      const provider = new ethers.providers.JsonRpcProvider(rpcUrl, network)
      const wallet = new ethers.Wallet(privateKey, provider)
      const config = await readValidatedChugSplashConfig(
        provider,
        configPath,
        artifactPaths,
        'foundry',
        cre
      )

      await provider.getNetwork()
      await wallet.getAddress()

      if (!silent) {
        console.log('-- ChugSplash Propose --')
      }
      await chugsplashProposeAbstractTask(
        provider,
        wallet,
        config,
        configPath,
        ipfsUrl,
        remoteExecution,
        'foundry',
        artifactPaths,
        canonicalConfigPath,
        cre
      )
      break
    }
    case 'fund': {
      const configPath = args[1]
      const rpcUrl = args[2]
      const network = args[3] !== 'localhost' ? args[3] : undefined
      const privateKey = args[4]
      const silent = args[5] === 'true'
      const outPath = cleanPath(args[6])
      const buildInfoPath = cleanPath(args[7])
      const amount = BigNumber.from(args[8])
      const autoEstimate = args[9] === 'true'

      const cre = await createChugSplashRuntime(
        configPath,
        args[3] !== 'localhost',
        true,
        undefined,
        silent,
        process.stdout
      )

      const { artifactFolder, buildInfoFolder } = fetchPaths(
        outPath,
        buildInfoPath
      )
      const userConfig = await readUnvalidatedChugSplashConfig(configPath)
      const artifactPaths = await getArtifactPaths(
        userConfig.contracts,
        artifactFolder,
        buildInfoFolder
      )

      const provider = new ethers.providers.JsonRpcProvider(rpcUrl, network)
      const wallet = new ethers.Wallet(privateKey, provider)
      await provider.getNetwork()

      const parsedConfig = await readValidatedChugSplashConfig(
        provider,
        configPath,
        artifactPaths,
        'foundry',
        cre
      )

      if (!silent) {
        console.log('-- ChugSplash Fund --')
      }
      await chugsplashFundAbstractTask(
        provider,
        wallet,
        configPath,
        amount,
        autoEstimate,
        artifactPaths,
        'foundry',
        parsedConfig,
        cre
      )
      break
    }
    case 'approve': {
      const configPath = args[1]
      const rpcUrl = args[2]
      const network = args[3] !== 'localhost' ? args[3] : undefined
      const privateKey = args[4]
      const silent = args[5] === 'true'
      const outPath = cleanPath(args[6])
      const buildInfoPath = cleanPath(args[7])
      const withdrawFunds = args[8] === 'true'
      const skipMonitorStatus = args[9] === 'true'

      const cre = await createChugSplashRuntime(
        configPath,
        args[3] !== 'localhost',
        true,
        undefined,
        silent,
        process.stdout
      )

      const {
        artifactFolder,
        buildInfoFolder,
        deploymentFolder,
        canonicalConfigPath,
      } = fetchPaths(outPath, buildInfoPath)
      const userConfig = await readUnvalidatedChugSplashConfig(configPath)
      const artifactPaths = await getArtifactPaths(
        userConfig.contracts,
        artifactFolder,
        buildInfoFolder
      )

      const provider = new ethers.providers.JsonRpcProvider(rpcUrl, network)
      const wallet = new ethers.Wallet(privateKey, provider)
      await provider.getNetwork()
      await wallet.getAddress()

      const remoteExecution = args[3] !== 'localhost'

      const parsedConfig = await readValidatedChugSplashConfig(
        provider,
        configPath,
        artifactPaths,
        'foundry',
        cre
      )

      if (!silent) {
        console.log('-- ChugSplash Approve --')
      }
      await chugsplashApproveAbstractTask(
        provider,
        wallet,
        configPath,
        !withdrawFunds,
        skipMonitorStatus,
        artifactPaths,
        'foundry',
        canonicalConfigPath,
        deploymentFolder,
        remoteExecution,
        parsedConfig,
        cre
      )
      break
    }
    case 'deploy': {
      const configPath = args[1]
      const rpcUrl = args[2]
      const network = args[3] !== 'localhost' ? args[3] : undefined
      const privateKey = args[4]
      const silent = args[5] === 'true'
      const outPath = cleanPath(args[6])
      const buildInfoPath = cleanPath(args[7])
      const withdrawFunds = args[8] === 'true'
      let newOwner = args[9]
      const ipfsUrl = args[10] !== 'none' ? args[10] : ''
      const allowManagedProposals = args[11] === 'true'

      const confirm = true

      const logPath = `logs/${network ?? 'anvil'}`
      if (!fs.existsSync(logPath)) {
        fs.mkdirSync(logPath, { recursive: true })
      }

      const now = new Date()
      const logWriter = fs.createWriteStream(
        `${logPath}/deploy-${now.getTime()}`
      )

      const cre = await createChugSplashRuntime(
        configPath,
        args[3] !== 'localhost',
        confirm,
        undefined,
        silent,
        logWriter
      )

      const {
        artifactFolder,
        buildInfoFolder,
        deploymentFolder,
        canonicalConfigPath,
      } = fetchPaths(outPath, buildInfoPath)
      const userConfig = await readUnvalidatedChugSplashConfig(configPath)
      const artifactPaths = await getArtifactPaths(
        userConfig.contracts,
        artifactFolder,
        buildInfoFolder
      )

      const provider = new ethers.providers.JsonRpcProvider(rpcUrl, network)
      const wallet = new ethers.Wallet(privateKey, provider)
      await provider.getNetwork()
      const address = await wallet.getAddress()
      newOwner = newOwner !== 'self' ? newOwner : address

      const remoteExecution = args[3] !== 'localhost'
      const spinner = ora({ isSilent: cre.silent, stream: logWriter })

      const parsedConfig = await readValidatedChugSplashConfig(
        provider,
        configPath,
        artifactPaths,
        'foundry',
        cre
      )

      if (!silent) {
        logWriter.write('-- ChugSplash Deploy --\n')
      }
      let executor: ChugSplashExecutorType | undefined
      if (remoteExecution) {
        spinner.start('Waiting for the executor to set up ChugSplash...')
        await monitorChugSplashSetup(provider, wallet)
      } else {
        spinner.start('Booting up ChugSplash...')
        executor = await initializeExecutor(provider)
      }

      spinner.succeed('ChugSplash is ready to go.')

      const contractArtifacts = await chugsplashDeployAbstractTask(
        provider,
        wallet,
        configPath,
        remoteExecution,
        ipfsUrl,
        withdrawFunds,
        newOwner ?? (await wallet.getAddress()),
        allowManagedProposals,
        artifactPaths,
        canonicalConfigPath,
        deploymentFolder,
        'foundry',
        cre,
        parsedConfig,
        executor
      )

      const artifactStructABI =
        'tuple(string referenceName, string contractName, address contractAddress)[]'
      const encodedArtifacts = ethers.utils.AbiCoder.prototype.encode(
        [artifactStructABI],
        [contractArtifacts]
      )

      process.stdout.write(encodedArtifacts)
      break
    }
    case 'monitor': {
      const configPath = args[1]
      const rpcUrl = args[2]
      const network = args[3] !== 'localhost' ? args[3] : undefined
      const privateKey = args[4]
      const silent = args[5] === 'true'
      const outPath = cleanPath(args[6])
      const buildInfoPath = cleanPath(args[7])
      const withdrawFunds = args[8] === 'true'
      let newOwner = args[9]

      const cre = await createChugSplashRuntime(
        configPath,
        args[3] !== 'localhost',
        true,
        undefined,
        silent,
        process.stdout
      )

      const {
        artifactFolder,
        buildInfoFolder,
        deploymentFolder,
        canonicalConfigPath,
      } = fetchPaths(outPath, buildInfoPath)
      const userConfig = await readUnvalidatedChugSplashConfig(configPath)
      const artifactPaths = await getArtifactPaths(
        userConfig.contracts,
        artifactFolder,
        buildInfoFolder
      )

      const provider = new ethers.providers.JsonRpcProvider(rpcUrl, network)
      const wallet = new ethers.Wallet(privateKey, provider)
      await provider.getNetwork()
      const address = await wallet.getAddress()
      newOwner = newOwner !== 'self' ? newOwner : address

      const remoteExecution = args[3] !== 'localhost'

      const parsedConfig = await readValidatedChugSplashConfig(
        provider,
        configPath,
        artifactPaths,
        'foundry',
        cre
      )

      if (!silent) {
        console.log('-- ChugSplash Monitor --')
      }
      await chugsplashMonitorAbstractTask(
        provider,
        wallet,
        configPath,
        !withdrawFunds,
        newOwner,
        artifactPaths,
        canonicalConfigPath,
        deploymentFolder,
        'foundry',
        remoteExecution,
        parsedConfig,
        cre
      )
      break
    }
    case 'cancel': {
      const configPath = args[1]
      const rpcUrl = args[2]
      const network = args[3] !== 'localhost' ? args[3] : undefined
      const privateKey = args[4]

      const provider = new ethers.providers.JsonRpcProvider(rpcUrl, network)
      const wallet = new ethers.Wallet(privateKey, provider)
      await provider.getNetwork()
      await wallet.getAddress()

      const cre = await createChugSplashRuntime(
        configPath,
        args[3] !== 'localhost',
        true,
        undefined,
        false,
        process.stdout
      )

      console.log('-- ChugSplash Cancel --')
      await chugsplashCancelAbstractTask(
        provider,
        wallet,
        configPath,
        'foundry',
        cre
      )
      break
    }
    case 'withdraw': {
      const configPath = args[1]
      const rpcUrl = args[2]
      const network = args[3] !== 'localhost' ? args[3] : undefined
      const privateKey = args[4]
      const silent = args[5] === 'true'

      const provider = new ethers.providers.JsonRpcProvider(rpcUrl, network)
      const wallet = new ethers.Wallet(privateKey, provider)
      await provider.getNetwork()
      await wallet.getAddress()

      const cre = await createChugSplashRuntime(
        configPath,
        args[3] !== 'localhost',
        true,
        undefined,
        silent,
        process.stdout
      )

      if (!silent) {
        console.log('-- ChugSplash Withdraw --')
      }
      await chugsplashWithdrawAbstractTask(
        provider,
        wallet,
        configPath,
        'foundry',
        cre
      )
      break
    }
    case 'listProjects': {
      const rpcUrl = args[1]
      const network = args[2] !== 'localhost' ? args[2] : undefined
      const privateKey = args[3]

      const provider = new ethers.providers.JsonRpcProvider(rpcUrl, network)
      const wallet = new ethers.Wallet(privateKey, provider)
      await provider.getNetwork()
      await wallet.getAddress()

      const cre = await createChugSplashRuntime(
        '',
        args[3] !== 'localhost',
        true,
        undefined,
        false,
        process.stdout
      )

      console.log('-- ChugSplash List Projects --')
      await chugsplashListProjectsAbstractTask(provider, wallet, 'foundry', cre)
      break
    }
    case 'listProposers': {
      const configPath = args[1]
      const rpcUrl = args[2]
      const network = args[3] !== 'localhost' ? args[3] : undefined
      const privateKey = args[4]

      const provider = new ethers.providers.JsonRpcProvider(rpcUrl, network)
      const wallet = new ethers.Wallet(privateKey, provider)
      await provider.getNetwork()
      await wallet.getAddress()

      console.log('-- ChugSplash List Proposers --')
      await chugsplashListProposersAbstractTask(
        provider,
        wallet,
        configPath,
        'foundry'
      )
      break
    }
    case 'addProposer': {
      const configPath = args[1]
      const rpcUrl = args[2]
      const network = args[3] !== 'localhost' ? args[3] : undefined
      const privateKey = args[4]
      const newProposer = args[5]

      const provider = new ethers.providers.JsonRpcProvider(rpcUrl, network)
      const wallet = new ethers.Wallet(privateKey, provider)
      await provider.getNetwork()
      await wallet.getAddress()

      const cre = await createChugSplashRuntime(
        configPath,
        args[3] !== 'localhost',
        true,
        undefined,
        false,
        process.stdout
      )

      console.log('-- ChugSplash Add Proposer --')
      await chugsplashAddProposersAbstractTask(
        provider,
        wallet,
        configPath,
        [newProposer],
        'foundry',
        cre
      )
      break
    }
    case 'claimProxy': {
      const configPath = args[1]
      const rpcUrl = args[2]
      const network = args[3] !== 'localhost' ? args[3] : undefined
      const privateKey = args[4]
      const silent = args[5] === 'true'
      const outPath = cleanPath(args[6])
      const buildInfoPath = cleanPath(args[7])
      const referenceName = args[8]

      const cre = await createChugSplashRuntime(
        configPath,
        args[3] !== 'localhost',
        true,
        undefined,
        silent,
        process.stdout
      )

      const { artifactFolder, buildInfoFolder } = fetchPaths(
        outPath,
        buildInfoPath
      )
      const userConfig = await readUnvalidatedChugSplashConfig(configPath)
      const artifactPaths = await getArtifactPaths(
        userConfig.contracts,
        artifactFolder,
        buildInfoFolder
      )

      const provider = new ethers.providers.JsonRpcProvider(rpcUrl, network)
      const wallet = new ethers.Wallet(privateKey, provider)
      await provider.getNetwork()
      await wallet.getAddress()

      const parsedConfig = await readValidatedChugSplashConfig(
        provider,
        configPath,
        artifactPaths,
        'foundry',
        cre
      )

      if (!silent) {
        console.log('-- ChugSplash Claim Proxy --')
      }
      await chugsplashClaimProxyAbstractTask(
        provider,
        wallet,
        configPath,
        referenceName,
        'foundry',
        parsedConfig,
        cre
      )
      break
    }
    case 'transferProxy': {
      const configPath = args[1]
      const rpcUrl = args[2]
      const network = args[3] !== 'localhost' ? args[3] : undefined
      const privateKey = args[4]
      const silent = args[5] === 'true'
      const proxyAddress = args[6]

      const provider = new ethers.providers.JsonRpcProvider(rpcUrl, network)
      const wallet = new ethers.Wallet(privateKey, provider)
      await provider.getNetwork()
      await wallet.getAddress()

      const cre = await createChugSplashRuntime(
        configPath,
        args[3] !== 'localhost',
        true,
        undefined,
        silent,
        process.stdout
      )

      if (!silent) {
        console.log('-- ChugSplash Transfer Proxy --')
      }
      await chugsplashTransferOwnershipAbstractTask(
        provider,
        wallet,
        configPath,
        proxyAddress,
        'foundry',
        cre
      )
      break
    }
    case 'getAddress': {
      const configPath = args[1]
      const referenceName = args[2]

      const userConfig = await readUnvalidatedChugSplashConfig(configPath)

      const proxy =
        userConfig.contracts[referenceName].externalProxy ||
        getDefaultProxyAddress(userConfig.options.projectName, referenceName)

      process.stdout.write(proxy)
      break
    }
    case 'getRegistryAddress': {
      process.stdout.write(CHUGSPLASH_REGISTRY_PROXY_ADDRESS)
      break
    }
    case 'getEIP1967ProxyAdminAddress': {
      const rpcUrl = args[1]
      const proxyAddress = args[2]

      const provider = new ethers.providers.JsonRpcProvider(rpcUrl)
      const adminAddress = await getEIP1967ProxyAdminAddress(
        provider,
        proxyAddress
      )

      process.stdout.write(adminAddress)
      break
    }
    case 'initializeChugSplash': {
      const rpcUrl = args[1]
      const network = args[2] !== 'localhost' ? args[2] : undefined
      const privateKey = args[3]

      const provider = new ethers.providers.JsonRpcProvider(rpcUrl, network)
      const wallet = new ethers.Wallet(privateKey, provider)
      const walletAddress = await wallet.getAddress()
      await initializeChugSplash(provider, wallet, [walletAddress])
      break
    }
  }
})().catch((err: Error) => {
  console.error(err)
  process.stdout.write('')
})
