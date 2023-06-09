import hre from 'hardhat'
import '@nomiclabs/hardhat-ethers'
import { Contract } from 'ethers'
import {
  chugsplashClaimAbstractTask,
  chugsplashProposeAbstractTask,
  FORWARDER_ADDRESS,
  getChugSplashManagerReadOnly,
  ProposalRoute,
  readValidatedChugSplashConfig,
} from '@chugsplash/core'
import { ForwarderArtifact } from '@chugsplash/contracts'
import { expect } from 'chai'

import { createChugSplashRuntime } from '../../plugins/src/cre'
import { makeGetConfigArtifacts } from '../src/hardhat/artifacts'

const configPath = './chugsplash/Metatx.config.ts'

describe('Meta txs', () => {
  process.env['LOCAL_TEST_METATX_PROPOSE'] = 'true'

  let expectedDeploymentId: string
  let manager: Contract
  before(async () => {
    const provider = hre.ethers.provider
    const signer = provider.getSigner()
    const signerAddress = await signer.getAddress()

    const cre = await createChugSplashRuntime(
      true,
      true,
      hre.config.paths.canonicalConfigs,
      hre,
      // if the config parsing fails and exits with code 1, you should flip this to false to see verbose output
      true
    )

    const { parsedConfig, configArtifacts, configCache } =
      await readValidatedChugSplashConfig(
        configPath,
        provider,
        cre,
        makeGetConfigArtifacts(hre)
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

    const metatxs = await chugsplashProposeAbstractTask(
      provider,
      signer,
      parsedConfig,
      configPath,
      '',
      'hardhat',
      configArtifacts,
      ProposalRoute.RELAY,
      cre,
      configCache
    )

    const { request, signature, deploymentId } = metatxs!
    expectedDeploymentId = deploymentId

    const Forwarder = new Contract(
      FORWARDER_ADDRESS,
      ForwarderArtifact.abi,
      signer
    )

    // Validate request on the forwarder contract
    const valid = await Forwarder.verify(request, signature)
    if (!valid) {
      throw new Error(`Invalid metatxs request`)
    }

    // Send meta-tx through relayer to the forwarder contract
    const gasLimit = (request.gas + 50000).toString()
    await Forwarder.execute(request, signature, { gasLimit })
    manager = getChugSplashManagerReadOnly(
      hre.ethers.provider,
      parsedConfig.options.organizationID
    )
  })

  it('does propose with meta txs', async () => {
    const deployment = await manager.deployments(expectedDeploymentId)
    expect(deployment).to.exist
    expect(deployment.status).to.equal(1)
  })

  after(() => {
    process.env['LOCAL_TEST_METATX_PROPOSE'] = 'false'
  })
})
