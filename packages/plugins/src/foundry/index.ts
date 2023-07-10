import * as fs from 'fs'

import {
  readParsedOwnerConfig,
  readUserChugSplashConfig,
  ProposalRoute,
  getChugSplashRegistryReadOnly,
  getPreviousConfigUri,
  postDeploymentActions,
  getChugSplashManagerReadOnly,
  DeploymentState,
  initializeChugSplash,
  bytecodeContainsEIP1967Interface,
  bytecodeContainsUUPSInterface,
  FailureAction,
  CanonicalProjectConfig,
  ProjectConfigArtifacts,
  getChugSplashManagerAddress,
  proposeAbstractTask,
} from '@chugsplash/core'
import { Contract, ethers } from 'ethers'
import { defaultAbiCoder, hexConcat } from 'ethers/lib/utils'

import { getFoundryConfigOptions } from './options'
import { makeGetConfigArtifacts, makeGetProviderFromChainId } from './utils'
import { createChugSplashRuntime } from '../cre'
import {
  getEncodedFailure,
  getPrettyWarnings,
  validationStderrWrite,
} from './logs'

const args = process.argv.slice(2)
const command = args[0]

;(async () => {
  switch (command) {
    case 'propose': {
      process.stderr.write = validationStderrWrite

      try {
        const configPath = args[1]
        const projectName = args[2]
        const dryRun = args[3] === 'true'

        const {
          artifactFolder,
          buildInfoFolder,
          canonicalConfigFolder,
          cachePath,
          rpcEndpoints,
        } = await getFoundryConfigOptions()

        const cre = createChugSplashRuntime(
          true,
          true,
          canonicalConfigFolder,
          undefined,
          false,
          process.stderr
        )

        await proposeAbstractTask(
          configPath,
          projectName,
          dryRun,
          cre,
          makeGetConfigArtifacts(artifactFolder, buildInfoFolder, cachePath),
          await makeGetProviderFromChainId(rpcEndpoints),
          undefined,
          FailureAction.THROW
        )

        const encodedWarnings = defaultAbiCoder.encode(
          ['string'],
          [getPrettyWarnings()]
        )

        const encodedSuccess = hexConcat([
          encodedWarnings,
          defaultAbiCoder.encode(['bool'], [true]), // true = success
        ])

        process.stdout.write(encodedSuccess)
      } catch (err) {
        const encodedFailure = getEncodedFailure(err)
        process.stdout.write(encodedFailure)
      }
      break
    }
    case 'getPreviousConfigUri': {
      const rpcUrl = args[1]
      const proxyAddress = args[2]
      const provider = new ethers.providers.JsonRpcProvider(rpcUrl)
      const registry = await getChugSplashRegistryReadOnly(provider)

      const configUri = await getPreviousConfigUri(
        provider,
        registry,
        proxyAddress
      )

      const exists = configUri !== undefined

      const encodedCanonicalConfigUri = ethers.utils.defaultAbiCoder.encode(
        ['bool', 'string'],
        [exists, configUri ?? '']
      )

      process.stdout.write(encodedCanonicalConfigUri)
      break
    }
    case 'checkProxyBytecodeCompatible': {
      const bytecode = args[1]

      if (
        bytecodeContainsEIP1967Interface(bytecode) &&
        bytecodeContainsUUPSInterface(bytecode)
      ) {
        process.stdout.write('true')
      } else {
        process.stdout.write('false')
      }
    }
    case 'deployOnAnvil': {
      const rpcUrl = args[1]
      const provider = new ethers.providers.JsonRpcProvider(rpcUrl)
      const wallet = new ethers.Wallet(
        '0xdbda1821b80551c9d65939329250298aa3472ba22feea921c0cf5d620ea67b97',
        provider
      )

      try {
        await initializeChugSplash(
          provider,
          wallet,
          [],
          [],
          (
            await provider.getNetwork()
          ).chainId
        )
      } catch (e) {
        if (!e.reason.includes('could not detect network')) {
          throw e
        }
      }

      break
    }
    case 'generateArtifacts': {
      const { canonicalConfigFolder, deploymentFolder, cachePath } =
        await getFoundryConfigOptions()

      const networkName = args[1]
      const rpcUrl = args[2]
      const ownerAddress = args[3]

      const provider: ethers.providers.JsonRpcProvider =
        new ethers.providers.JsonRpcProvider(rpcUrl)

      const deployer = getChugSplashManagerAddress(ownerAddress)
      const manager = getChugSplashManagerReadOnly(deployer, provider)

      // Get the most recent deployment completed event for this deployment ID.
      const deploymentCompletedEvent = (
        await manager.queryFilter(
          // This might be problematic if you're deploying multiple projects with the same manager.
          // We really should include the project name on these events so we can filter by it.
          manager.filters.ChugSplashDeploymentCompleted()
        )
      ).at(-1)
      const deploymentId = deploymentCompletedEvent?.args?.deploymentId

      const deployment: DeploymentState = await manager.deployments(
        deploymentId
      )

      const ipfsHash = deployment.configUri.replace('ipfs://', '')
      const canonicalConfig: CanonicalProjectConfig = JSON.parse(
        fs.readFileSync(`.canonical-configs/${ipfsHash}.json`).toString()
      )

      const configArtifacts: ProjectConfigArtifacts = JSON.parse(
        fs
          .readFileSync(`${cachePath}/configArtifacts/${ipfsHash}.json`)
          .toString()
      )

      await postDeploymentActions(
        canonicalConfig,
        configArtifacts,
        deploymentId,
        canonicalConfigFolder,
        deployment.configUri,
        false,
        networkName,
        deploymentFolder,
        'foundry',
        true,
        manager.owner(),
        provider,
        manager
      )
    }
  }
})()
