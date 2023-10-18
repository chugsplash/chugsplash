import chai from 'chai'

// TODO(hai):
// - what does the governor private key do?
// - what does the `yarn script:goerli` command do?

import {
  buildInfo as sphinxContractsBuildInfo,
} from '@sphinx-labs/contracts'
import { getFoundryConfigOptions } from '../../../src/foundry/options'
import { getStorageSlotKey } from '@sphinx-labs/core'
import { makeGetConfigArtifacts } from '../../../src/foundry/utils'

const expect = chai.expect

describe('LocalSphinxManager', () => {
  it(`LocalSphinxManager 'callNonces' mapping matches SphinxManager in storage layout`, async () => {
    const managerSlotKey = getStorageSlotKey(
      'contracts/SphinxManager.sol:SphinxManager',
      sphinxContractsBuildInfo.output,
      'callNonces'
    )

    const { artifactFolder, buildInfoFolder, cachePath } = await getFoundryConfigOptions()

    const getConfigArtifacts = makeGetConfigArtifacts(
      artifactFolder,
      buildInfoFolder,
      cachePath
    )

    const localManagerFullyQualifiedName = 'contracts/foundry/LocalSphinxManager.sol:LocalSphinxManager'
    const configArtifacts = await getConfigArtifacts([localManagerFullyQualifiedName])

    const localManagerSlotKey = getStorageSlotKey(
      localManagerFullyQualifiedName,
      configArtifacts[localManagerFullyQualifiedName].buildInfo.output,
      'callNonces'
    )

    expect(localManagerSlotKey).to.equal(managerSlotKey)
  })
})
