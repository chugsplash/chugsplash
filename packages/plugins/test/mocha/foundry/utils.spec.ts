import chai from 'chai'
import chaiAsPromised from 'chai-as-promised'

chai.use(chaiAsPromised)
const expect = chai.expect

import {
  messageArtifactNotFound,
  getFoundryContractArtifact,
} from '../../../src/foundry/utils'
import { getFoundryConfigOptions } from '../../../src/foundry/options'

describe('Utils', async () => {
  describe('getFoundryContractArtifact', async () => {
    it('Errors if artifact is not found', async () => {
      const { artifactFolder } = await getFoundryConfigOptions()

      const fullyQualifiedName =
        'contracts/DoesNotExist.sol:NonExistentContract'
      await expect(
        getFoundryContractArtifact(fullyQualifiedName, artifactFolder)
      ).to.be.rejectedWith(messageArtifactNotFound(fullyQualifiedName))
    })

    it('Gets the artifact for a fully qualified name', async () => {
      const { artifactFolder } = await getFoundryConfigOptions()

      const fullyQualifiedName = 'script/BridgeFunds.s.sol:SphinxScript'
      const artifact = await getFoundryContractArtifact(
        fullyQualifiedName,
        artifactFolder
      )
      expect(artifact.contractName).equals('SphinxScript')
    })
  })
})
