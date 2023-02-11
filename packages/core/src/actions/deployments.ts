import { ethers } from 'ethers'
import ora from 'ora'

import { trackProposed } from '../analytics'
import { ParsedChugSplashConfig, verifyBundle } from '../config'
import { Integration } from '../constants'
import { ArtifactPaths } from '../languages'
import { resolveNetworkName } from '../messages'
import { chugsplashCommitAbstractSubtask } from '../tasks'
import {
  getProjectOwnerAddress,
  getChugSplashManager,
  getGasPriceOverrides,
  computeBundleId,
} from '../utils'
import { ChugSplashActionBundle } from './types'

export const proposeChugSplashBundle = async (
  provider: ethers.providers.JsonRpcProvider,
  signer: ethers.Signer,
  parsedConfig: ParsedChugSplashConfig,
  bundle: ChugSplashActionBundle,
  configUri: string,
  remoteExecution: boolean,
  ipfsUrl: string,
  configPath: string,
  spinner: ora.Ora = ora({ isSilent: true }),
  confirm: boolean,
  artifactPaths: ArtifactPaths,
  canonicalConfigPath: string,
  silent: boolean,
  integration: Integration
) => {
  const projectName = parsedConfig.options.projectName

  spinner.start(`Checking if the caller is a proposer...`)

  spinner.succeed(`Caller is a proposer.`)

  spinner.start(`Proposing ${projectName}...`)

  const ChugSplashManager = getChugSplashManager(signer, projectName)

  if (remoteExecution) {
    await chugsplashCommitAbstractSubtask(
      provider,
      signer,
      parsedConfig,
      ipfsUrl,
      true,
      artifactPaths,
      canonicalConfigPath,
      integration,
      spinner
    )
    // Verify that the bundle has been committed to IPFS with the correct bundle hash.
    await verifyBundle({
      configUri,
      bundleId: computeBundleId(bundle.root, bundle.actions.length, configUri),
      ipfsUrl,
      artifactPaths,
      integration,
    })
  }
  // Propose the bundle.
  await (
    await ChugSplashManager.proposeChugSplashBundle(
      bundle.root,
      bundle.actions.length,
      configUri,
      await getGasPriceOverrides(provider)
    )
  ).wait()

  const networkName = await resolveNetworkName(provider, integration)
  await trackProposed(
    await getProjectOwnerAddress(signer, projectName),
    projectName,
    networkName,
    integration
  )

  spinner.succeed(`Proposed ${projectName}.`)
}
