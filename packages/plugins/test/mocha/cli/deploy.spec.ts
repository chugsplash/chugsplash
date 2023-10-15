import { join, resolve } from 'path'
import { existsSync, unlinkSync } from 'fs'
import { exec } from 'child_process'

import chai from 'chai'
import chaiAsPromised from 'chai-as-promised'
import {
  SphinxJsonRpcProvider,
  execAsync,
  getPreview,
  sleep,
} from '@sphinx-labs/core'
import { ethers } from 'ethers'

import { deploy } from '../../../src/cli/deploy'
import { getFoundryConfigOptions } from '../../../src/foundry/options'

chai.use(chaiAsPromised)
const expect = chai.expect

const provider = new SphinxJsonRpcProvider(`http://127.0.0.1:42005`)

const forgeScriptPath = 'contracts/test/script/Simple.s.sol'
const contractAddress = '0xa736B2394965D6b796c6D3F2766D96D19d8b2CFB'

const mockPrompt = async (q: string) => {}

describe('Deploy CLI command', () => {
  let deploymentArtifactFilePath: string
  before(async () => {
    const { deploymentFolder } = await getFoundryConfigOptions()
    deploymentArtifactFilePath = join(
      deploymentFolder,
      'goerli-local',
      'MyContract1.json'
    )
  })

  beforeEach(async () => {
    // Start an Anvil node with a fresh state. We must use `exec` instead of `execAsync`
    // because the latter will hang indefinitely.
    exec(`anvil --chain-id 5 --port 42005 --silent &`)
    await sleep(500)

    if (existsSync(deploymentArtifactFilePath)) {
      unlinkSync(deploymentArtifactFilePath)
    }
  })

  afterEach(async () => {
    // Kill the Anvil node
    await execAsync(`kill $(lsof -t -i:42005)`)
  })

  describe('With preview', () => {
    it('Executes deployment', async () => {
      // We run `forge clean` to ensure that a proposal can occur even if we're running
      // a fresh compilation process.
      await execAsync(`forge clean`)

      expect((await provider.getCode(contractAddress)) === '0x')

      // Check that the deployment artifact hasn't been created yet
      expect(existsSync(deploymentArtifactFilePath)).to.be.false

      const { deployedParsedConfig, previewParsedConfig } = await deploy(
        forgeScriptPath,
        'goerli',
        false, // Run preview
        true, // Silent
        undefined, // Only one contract in the script file, so there's no target contract to specify.
        undefined, // Don't verify on Etherscan.
        mockPrompt
      )
      expect(deployedParsedConfig).to.not.be.undefined
      // This narrows the type of `previewParsedConfig` to `ParsedConfig`.
      if (!previewParsedConfig) {
        throw new Error(`Expected previewParsedConfig to be defined`)
      }

      const contract = getContract()
      expect((await provider.getCode(contractAddress)) !== '0x')
      expect(await contract.uintArg()).to.equal(3n)

      expect(previewParsedConfig.actionInputs).to.deep.equal(
        previewParsedConfig.actionInputs
      )

      const preview = getPreview([previewParsedConfig])
      expect(preview).to.deep.equal([
        {
          networkTags: ['goerli (local)'],
          executing: [
            {
              referenceName: 'SphinxManager',
              functionName: 'constructor',
              variables: {},
            },
            {
              referenceName: 'MyContract1',
              functionName: 'constructor',
              variables: {
                _intArg: -1n,
                _uintArg: 2n,
                _addressArg: '0x' + '00'.repeat(19) + '01',
                _otherAddressArg: '0x' + '00'.repeat(19) + '02',
              },
            },
            {
              referenceName: 'MyContract1',
              functionName: 'incrementUint',
              variables: {},
            },
          ],
          skipping: [],
        },
      ])

      // Check that the deployment artifact was created
      expect(existsSync(deploymentArtifactFilePath)).to.be.true
    })

    it(`Displays preview then exits when there's nothing to deploy`, async () => {
      expect((await provider.getCode(contractAddress)) === '0x')

      await deploy(
        forgeScriptPath,
        'goerli',
        true, // Skip preview
        true, // Silent
        undefined, // Only one contract in the script file, so there's no target contract to specify.
        undefined, // Don't verify on Etherscan.
        mockPrompt
      )

      // Remove the deployment artifact. Later, we'll test that it isn't created again.
      unlinkSync(deploymentArtifactFilePath)

      expect((await provider.getCode(contractAddress)) !== '0x')
      const contract = getContract()
      expect(await contract.uintArg()).to.equal(3n)

      const { deployedParsedConfig, previewParsedConfig } = await deploy(
        forgeScriptPath,
        'goerli',
        false, // Run preview
        true, // Silent
        undefined, // Only one contract in the script file, so there's no target contract to specify.
        undefined, // Don't verify on Etherscan.
        mockPrompt
      )
      // This narrows the type of `previewParsedConfig` to `ParsedConfig`.
      if (!previewParsedConfig) {
        throw new Error(`Expected previewParsedConfig to be defined`)
      }

      expect(deployedParsedConfig).to.be.undefined

      expect(await contract.uintArg()).to.equal(3n)

      const preview = getPreview([previewParsedConfig])
      expect(preview).to.deep.equal([
        {
          networkTags: ['goerli (local)'],
          executing: [],
          skipping: [
            {
              referenceName: 'MyContract1',
              functionName: 'constructor',
              variables: {
                _intArg: -1n,
                _uintArg: 2n,
                _addressArg: '0x' + '00'.repeat(19) + '01',
                _otherAddressArg: '0x' + '00'.repeat(19) + '02',
              },
            },
            {
              referenceName: 'MyContract1',
              functionName: 'incrementUint',
              variables: {},
            },
          ],
        },
      ])

      // Check that the deployment artifact wasn't created
      expect(existsSync(deploymentArtifactFilePath)).to.be.false
    })
  })

  describe('Without preview', () => {
    it('Executes deployment', async () => {
      expect((await provider.getCode(contractAddress)) === '0x')

      // Check that the deployment artifact hasn't been created yet
      expect(existsSync(deploymentArtifactFilePath)).to.be.false

      const { deployedParsedConfig, previewParsedConfig } = await deploy(
        forgeScriptPath,
        'goerli',
        true, // Skip preview
        true, // Silent
        undefined, // Only one contract in the script file, so there's no target contract to specify.
        undefined, // Don't verify on Etherscan.
        mockPrompt
      )

      expect(deployedParsedConfig).to.not.be.undefined
      expect(previewParsedConfig).to.be.undefined

      expect((await provider.getCode(contractAddress)) !== '0x')
      const contract = getContract()
      expect(await contract.uintArg()).to.equal(3n)

      // Check that the deployment artifact was created
      expect(existsSync(deploymentArtifactFilePath)).to.be.true
    })

    it(`Skips deployment when there's nothing to deploy`, async () => {
      expect((await provider.getCode(contractAddress)) === '0x')

      await deploy(
        forgeScriptPath,
        'goerli',
        true, // Skip preview
        true, // Silent
        undefined, // Only one contract in the script file, so there's no target contract to specify.
        undefined, // Don't verify on Etherscan.
        mockPrompt
      )

      expect((await provider.getCode(contractAddress)) !== '0x')
      const contract = getContract()
      expect(await contract.uintArg()).to.equal(3n)

      // Remove the deployment artifact. Later, we'll test that it isn't created again.
      unlinkSync(deploymentArtifactFilePath)

      const { deployedParsedConfig, previewParsedConfig } = await deploy(
        forgeScriptPath,
        'goerli',
        true, // Skip preview
        true, // Silent
        undefined, // Only one contract in the script file, so there's no target contract to specify.
        undefined, // Don't verify on Etherscan.
        mockPrompt
      )
      expect(deployedParsedConfig).to.not.be.undefined
      expect(previewParsedConfig).to.be.undefined

      expect(await contract.uintArg()).to.equal(3n)

      // Check that the deployment artifact wasn't created
      expect(existsSync(deploymentArtifactFilePath)).to.be.false
    })
  })
})

const getContract = (): ethers.Contract => {
  const abi =
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  require(resolve(
    './out/artifacts/MyContracts.sol/MyContract1.json'
  )).abi

  const contract = new ethers.Contract(
    contractAddress,
    abi,
    provider
  )
  return contract
}