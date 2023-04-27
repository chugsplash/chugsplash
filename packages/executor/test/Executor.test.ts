import * as path from 'path'
import '@chugsplash/plugins'

import hre, { chugsplash } from 'hardhat'
import { BigNumber, Contract } from 'ethers'
import {
  chugsplashApproveAbstractTask,
  chugsplashClaimAbstractTask,
  chugsplashFundAbstractTask,
  chugsplashProposeAbstractTask,
  readUnvalidatedChugSplashConfig,
  readValidatedChugSplashConfig,
} from '@chugsplash/core'
import { expect } from 'chai'

import { getArtifactPaths } from '../../plugins/dist'
import { createChugSplashRuntime } from '../../plugins/src/utils'

const configPath = './chugsplash/ExecutorTest.config.ts'

describe('Remote Execution', () => {
  let ExecutorTest: Contract
  beforeEach(async () => {
    const provider = hre.ethers.provider
    const signer = provider.getSigner()
    const signerAddress = await signer.getAddress()
    const canonicalConfigPath = hre.config.paths.canonicalConfigs
    const deploymentFolder = hre.config.paths.deployments

    const userConfig = await readUnvalidatedChugSplashConfig(configPath)

    const artifactPaths = await getArtifactPaths(
      hre,
      userConfig.contracts,
      hre.config.paths.artifacts,
      path.join(hre.config.paths.artifacts, 'build-info')
    )

    const cre = await createChugSplashRuntime(
      configPath,
      true,
      true,
      hre.config.paths.canonicalConfigs,
      hre,
      // if the config parsing fails and exits with code 1, you should flip this to false to see verbose output
      false
    )

    const parsedConfig = await readValidatedChugSplashConfig(
      provider,
      configPath,
      artifactPaths,
      'hardhat',
      cre,
      true
    )

    // claim
    await chugsplashClaimAbstractTask(
      provider,
      signer,
      parsedConfig,
      true,
      signerAddress,
      'hardhat',
      cre
    )

    // fund
    await chugsplashFundAbstractTask(
      provider,
      signer,
      configPath,
      BigNumber.from(0),
      true,
      artifactPaths,
      'hardhat',
      parsedConfig,
      cre
    )

    await chugsplashProposeAbstractTask(
      provider,
      signer,
      parsedConfig,
      configPath,
      '',
      'hardhat',
      artifactPaths,
      canonicalConfigPath,
      cre
    )

    // approve
    await chugsplashApproveAbstractTask(
      provider,
      signer,
      configPath,
      true,
      false,
      artifactPaths,
      'hardhat',
      canonicalConfigPath,
      deploymentFolder,
      parsedConfig,
      cre
    )

    ExecutorTest = await chugsplash.getContract(
      parsedConfig.options.projectName,
      'ExecutorTest'
    )
  })

  it('does deploy remotely', async () => {
    expect(await ExecutorTest.number()).to.equal(1)
    expect(await ExecutorTest.stored()).to.equal(true)
    expect(await ExecutorTest.storageName()).to.equal('First')
    expect(await ExecutorTest.otherStorage()).to.equal(
      '0x1111111111111111111111111111111111111111'
    )
  })
})
