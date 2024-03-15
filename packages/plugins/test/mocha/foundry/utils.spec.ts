import { resolve } from 'path'
import { existsSync } from 'fs'

import sinon from 'sinon'
import chai from 'chai'
import chaiAsPromised from 'chai-as-promised'
import { ConstructorFragment, ethers } from 'ethers'
import {
  ContractArtifact,
  LinkReferences,
  parseFoundryContractArtifact,
  remove0x,
} from '@sphinx-labs/contracts'
import {
  GetConfigArtifacts,
  sphinxCoreUtils,
  SphinxJsonRpcProvider,
  getBytesLength,
} from '@sphinx-labs/core'

chai.use(chaiAsPromised)
const expect = chai.expect

import {
  sphinxFoundryUtils,
  convertLibraryFormat,
  isDeployedCodeInArtifact,
  isInitCodeMatch,
  readBuildInfoCache,
  replaceEnvVariables,
  findContractArtifactForDeployedCode,
} from '../../../src/foundry/utils'
import { getFoundryToml } from '../../../src/foundry/options'
import * as MyContract1Artifact from '../../../out/artifacts/MyContracts.sol/MyContract1.json'
import * as MyContract2Artifact from '../../../out/artifacts/MyContracts.sol/MyContract2.json'
import * as MyContractWithLibrariesArtifact from '../../../out/artifacts/MyContracts.sol/MyContractWithLibraries.json'
import * as MyContractWithLibrariesAndImmutablesArtifact from '../../../out/artifacts/MyContracts.sol/MyContractWithLibrariesAndImmutables.json'
import * as MyImmutableContractArtifact from '../../../out/artifacts/MyContracts.sol/MyImmutableContract.json'
import * as MyLargeContractArtifact from '../../../out/artifacts/MyContracts.sol/MyLargeContract.json'
import {
  encodeFunctionCalldata,
  getAnvilRpcUrl,
  killAnvilNodes,
  makeAddress,
  runForgeScript,
  startAnvilNodes,
} from '../common'
import { FoundryToml } from '../../../src/foundry/types'
import {
  assertNoLinkedLibraries,
  findContractArtifact,
  isDeployedCodeMatch,
  makeGetConfigArtifacts,
  parseScriptFunctionCalldata,
  validateProposalNetworks,
} from '../../../dist'
import {
  InvalidFirstSigArgumentErrorMessage,
  SigCalledWithNoArgsErrorMessage,
  SphinxConfigMainnetsContainsTestnetsErrorMessage,
  SphinxConfigTestnetsContainsMainnetsErrorMessage,
  getDetectedLinkedLibraryErrorMessage,
  getFailedRequestErrorMessage,
  getLocalNetworkErrorMessage,
  getMissingEndpointErrorMessage,
  getMixedNetworkTypeErrorMessage,
  getUnsupportedNetworkErrorMessage,
} from '../../../src/foundry/error-messages'
import { getFakeBuildInfoCache } from '../fake'
import { getDummyBuildInfoCache } from '../dummy'

describe('Utils', async () => {
  let foundryToml: FoundryToml

  before(async () => {
    foundryToml = await getFoundryToml()
  })

  describe('findContractArtifact', async () => {
    const projectRoot = process.cwd()
    const dummyBytecode = 'dummyBytecode'

    let artifactFolder: string
    let isBytecodeInArtifactStub: sinon.SinonStub
    let isDeployedCodeInArtifactSpy: sinon.SinonSpy
    let existsSyncSpy: sinon.SinonSpy

    before(async () => {
      artifactFolder = foundryToml.artifactFolder
    })

    beforeEach(() => {
      isBytecodeInArtifactStub = sinon.stub()
      isDeployedCodeInArtifactSpy = sinon.spy(isDeployedCodeInArtifact)
      existsSyncSpy = sinon.spy(sphinxCoreUtils, 'existsSync')
    })

    afterEach(() => {
      sinon.restore()
    })

    it('Returns undefined if artifact is not found', async () => {
      const fullyQualifiedName =
        'contracts/DoesNotExist.sol:NonExistentContract'
      const artifact = await findContractArtifact(
        fullyQualifiedName,
        projectRoot,
        artifactFolder,
        dummyBytecode,
        isBytecodeInArtifactStub
      )
      expect(artifact).to.be.undefined
      expect(existsSyncSpy.called).to.be.true
      expect(
        existsSyncSpy.returnValues.every((value) => value === false)
      ).equals(true)
      expect(isBytecodeInArtifactStub.notCalled).to.be.true
    })

    it('Returns undefined if artifact is found but bytecode does not match', async () => {
      const { sourceName, contractName } =
        parseFoundryContractArtifact(MyContract1Artifact)
      const fullyQualifiedName = `${sourceName}:${contractName}`

      const artifact = await findContractArtifact(
        fullyQualifiedName,
        projectRoot,
        artifactFolder,
        dummyBytecode,
        isDeployedCodeInArtifactSpy
      )
      expect(artifact).to.be.undefined
      expect(existsSyncSpy.called).to.be.true
      expect(existsSyncSpy.returnValues.some((value) => value === true)).equals(
        true
      )
      expect(isDeployedCodeInArtifactSpy.called).to.be.true
      for (const returnValue of isDeployedCodeInArtifactSpy.returnValues) {
        expect(returnValue).to.be.false
      }
    })

    it('Gets the artifact for a fully qualified name', async () => {
      const expectedArtifact = parseFoundryContractArtifact(MyContract1Artifact)
      const { sourceName, contractName, deployedBytecode } = expectedArtifact
      const fullyQualifiedName = `${sourceName}:${contractName}`
      const artifact = await findContractArtifact(
        fullyQualifiedName,
        projectRoot,
        artifactFolder,
        deployedBytecode,
        isDeployedCodeInArtifact
      )
      expect(expectedArtifact).deep.equals(artifact)
    })

    // Tests scenarios where there are multiple contracts with the same name but located in
    // different directories or with different source file names.
    it('Gets artifacts for contracts with the same name', async () => {
      isBytecodeInArtifactStub.returns(true)

      // The source name and contract name of this contract match.
      const contractOne =
        'contracts/test/DuplicateContractName.sol:DuplicateContractName'
      // The source name and contract name of this contract don't match.
      const contractTwo = 'contracts/test/MyContracts.sol:DuplicateContractName'
      // This contract's source file is nested one level. We use the absolute path because it's
      // possible that the artifact path is an absolute path in production. This isn't strictly
      // necessary to test, but it adds variety to this test case.
      const absolutePath = resolve(
        'contracts/test/deep/DuplicateContractName.sol'
      )
      const contractThree = `${absolutePath}:DuplicateContractName`
      // This contract's source file is nested two levels.
      const contractFour =
        'contracts/test/deep/deeper/DuplicateContractName.sol:DuplicateContractName'
      // This contract is nested only one level, but it shares a parent source directory with the
      // previous contract. (They both exist in a `deeper` directory).
      const contractFive =
        'contracts/test/deeper/DuplicateContractName.sol:DuplicateContractName'

      const artifactOne = await findContractArtifact(
        contractOne,
        projectRoot,
        artifactFolder,
        dummyBytecode,
        isBytecodeInArtifactStub
      )
      const artifactTwo = await findContractArtifact(
        contractTwo,
        projectRoot,
        artifactFolder,
        dummyBytecode,
        isBytecodeInArtifactStub
      )
      const artifactThree = await findContractArtifact(
        contractThree,
        projectRoot,
        artifactFolder,
        dummyBytecode,
        isBytecodeInArtifactStub
      )
      const artifactFour = await findContractArtifact(
        contractFour,
        projectRoot,
        artifactFolder,
        dummyBytecode,
        isBytecodeInArtifactStub
      )
      const artifactFive = await findContractArtifact(
        contractFive,
        projectRoot,
        artifactFolder,
        dummyBytecode,
        isBytecodeInArtifactStub
      )

      // Check that the location of the artifact files is correct.
      // First contract:
      expect(
        existsSync(
          `${artifactFolder}/DuplicateContractName.sol/DuplicateContractName.json`
        )
      ).equals(true)
      // Second contract:
      expect(
        existsSync(
          `${artifactFolder}/MyContracts.sol/DuplicateContractName.json`
        )
      ).equals(true)
      // Third contract:
      expect(
        existsSync(
          `${artifactFolder}/deep/DuplicateContractName.sol/DuplicateContractName.json`
        )
      ).equals(true)
      // Fourth contract:
      expect(
        existsSync(
          `${artifactFolder}/deeper/DuplicateContractName.sol/DuplicateContractName.json`
        )
      ).equals(true)
      // Fifth contract:
      expect(
        existsSync(
          `${artifactFolder}/test/deeper/DuplicateContractName.sol/DuplicateContractName.json`
        )
      ).equals(true)

      // Check that we retrieved the correct artifacts.
      expect(
        artifactOne?.abi.some((e) => e.name === 'duplicateContractOne')
      ).equals(true)
      expect(
        artifactTwo?.abi.some((e) => e.name === 'duplicateContractTwo')
      ).equals(true)
      expect(
        artifactThree?.abi.some((e) => e.name === 'duplicateContractThree')
      ).equals(true)
      expect(
        artifactFour?.abi.some((e) => e.name === 'duplicateContractFour')
      ).equals(true)
      expect(
        artifactFive?.abi.some((e) => e.name === 'duplicateContractFive')
      ).equals(true)
    })
  })

  describe('convertLibraryFormat', () => {
    it('should handle an empty array', () => {
      const librariesArray: string[] = []

      const expectedOutput: string[] = []

      const result = convertLibraryFormat(librariesArray)
      expect(result).to.deep.equal(expectedOutput)
    })

    it('should correctly convert library formats', () => {
      const librariesArray = [
        'script/Counter.s.sol:MyLibrary:0x5FbDB2315678afecb367f032d93F642f64180aa3',
        'file.sol:Math=0x1234567890123456789012345678901234567890',
      ]

      const expectedOutput = [
        'script/Counter.s.sol:MyLibrary=0x5FbDB2315678afecb367f032d93F642f64180aa3',
        'file.sol:Math=0x1234567890123456789012345678901234567890',
      ]

      const result = convertLibraryFormat(librariesArray)
      expect(result).to.deep.equal(expectedOutput)
    })

    it('should normalize Ethereum addresses', () => {
      // This address is lowercase (not in checksum format).
      const librariesArray = [
        'script/Counter.s.sol:MyLibrary:0x8ba1f109551bd432803012645ac136ddd64dba72',
      ]

      // This uses a checksum address.
      const expectedOutput = [
        'script/Counter.s.sol:MyLibrary=0x8ba1f109551bD432803012645Ac136ddd64DBA72',
      ]

      const result = convertLibraryFormat(librariesArray)
      expect(result).to.deep.equal(expectedOutput)
    })

    it('should throw an error for invalid formats', () => {
      const librariesArray = ['invalidformat']

      expect(() => convertLibraryFormat(librariesArray)).to.throw(
        'Invalid library string format.'
      )
    })
  })

  describe('replaceEnvVariables', () => {
    before(() => {
      process.env['TEST_VAR'] = 'TestValue'
      process.env['ANOTHER_VAR'] = 'AnotherValue'
      process.env['RPC_API_KEY'] = 'MockApiKey'
      process.env['ETHERSCAN_API_KEY_OPTIMISM'] = 'MockEtherscanKey'
    })

    after(() => {
      delete process.env['TEST_VAR']
      delete process.env['ANOTHER_VAR']
      delete process.env['RPC_API_KEY']
      delete process.env['ETHERSCAN_API_KEY_OPTIMISM']
    })

    it('should replace environment variables in a string', () => {
      const input = `URL is \${TEST_VAR}`
      const expected = 'URL is TestValue'
      expect(replaceEnvVariables(input)).to.equal(expected)
    })

    it('should work with nested objects', () => {
      const input = {
        level1: {
          level2: `Nested \${TEST_VAR}`,
        },
      }
      const expected = {
        level1: {
          level2: 'Nested TestValue',
        },
      }
      expect(replaceEnvVariables(input)).to.deep.equal(expected)
    })

    it('should work with arrays', () => {
      const input = [`\${TEST_VAR}`, 'static', `\${ANOTHER_VAR}`]
      const expected = ['TestValue', 'static', 'AnotherValue']
      expect(replaceEnvVariables(input)).to.deep.equal(expected)
    })

    it('should ignore strings without environment variables', () => {
      const input = 'This is a test string'
      expect(replaceEnvVariables(input)).to.equal(input)
    })

    it('should replace environment variables in a nested object and trim the string', () => {
      const input = {
        outerField: {
          innerField: `      untrimmed    \${TEST_VAR}           `,
        },
      }
      const expected = {
        outerField: {
          innerField: 'untrimmed    TestValue', // Expected to be trimmed
        },
      }
      expect(replaceEnvVariables(input)).to.deep.equal(expected)
    })

    it('should work for sample foundry.toml', () => {
      const input = {
        src: 'src',
        test: 'test',
        script: 'script',
        out: 'out',
        libs: ['node_modules'],
        remappings: [
          '@sphinx-labs/plugins/=node_modules/@sphinx-labs/plugins/contracts/foundry/',
          '@sphinx-labs/contracts/=node_modules/@sphinx-labs/contracts/',
          'forge-std/=node_modules/forge-std/src/',
          'sphinx-forge-std/=node_modules/@sphinx-labs/plugins/node_modules/sphinx-forge-std/src/',
          'sphinx-solmate/=node_modules/@sphinx-labs/plugins/node_modules/sphinx-solmate/src/',
          'ds-test/=node_modules/ds-test/src/',
          '@openzeppelin/contracts-upgradeable/=../../node_modules/@openzeppelin/contracts-upgradeable/',
          '@openzeppelin/contracts/=../../node_modules/@openzeppelin/contracts/',
          'solidity-stringutils=../../node_modules/solidity-stringutils/src/',
          'solmate/src/=../../node_modules/solmate/src/',
        ],
        auto_detect_remappings: true,
        libraries: [],
        cache: true,
        cache_path: 'cache',
        broadcast: 'broadcast',
        allow_paths: ['../..'],
        include_paths: [],
        force: false,
        evm_version: 'paris',
        gas_reports: ['*'],
        gas_reports_ignore: [],
        solc: null,
        auto_detect_solc: true,
        offline: false,
        optimizer: false,
        optimizer_runs: 200,
        optimizer_details: null,
        model_checker: null,
        verbosity: 0,
        eth_rpc_url: null,
        eth_rpc_jwt: null,
        etherscan_api_key: null,
        etherscan: {
          optimism_sepolia: {
            url: 'https://api-optimistic.etherscan.io/api?',
            key: `\${ETHERSCAN_API_KEY_OPTIMISM}`,
          },
        },
        ignored_error_codes: ['license', 'code-size', 'init-code-size'],
        deny_warnings: false,
        match_test: null,
        no_match_test: null,
        match_contract: null,
        no_match_contract: null,
        match_path: null,
        no_match_path: null,
        fuzz: {
          runs: 256,
          max_test_rejects: 65536,
          seed: null,
          dictionary_weight: 40,
          include_storage: true,
          include_push_bytes: true,
          max_fuzz_dictionary_addresses: 15728640,
          max_fuzz_dictionary_values: 6553600,
        },
        invariant: {
          runs: 256,
          depth: 15,
          fail_on_revert: false,
          call_override: false,
          dictionary_weight: 80,
          include_storage: true,
          include_push_bytes: true,
          max_fuzz_dictionary_addresses: 15728640,
          max_fuzz_dictionary_values: 6553600,
          shrink_sequence: true,
          shrink_run_limit: 262144,
        },
        ffi: false,
        sender: '0x1804c8ab1f12e6bbf3894d4083f33e07309d1f38',
        tx_origin: '0x1804c8ab1f12e6bbf3894d4083f33e07309d1f38',
        initial_balance: '0xffffffffffffffffffffffff',
        block_number: 1,
        fork_block_number: null,
        chain_id: null,
        gas_limit: 9223372036854775807,
        code_size_limit: null,
        gas_price: null,
        block_base_fee_per_gas: 0,
        block_coinbase: '0x0000000000000000000000000000000000000000',
        block_timestamp: 1,
        block_difficulty: 0,
        block_prevrandao:
          '0x0000000000000000000000000000000000000000000000000000000000000000',
        block_gas_limit: null,
        memory_limit: 134217728,
        extra_output: ['storageLayout'],
        extra_output_files: [],
        names: false,
        sizes: false,
        via_ir: false,
        rpc_storage_caching: {
          chains: 'all',
          endpoints: 'all',
        },
        no_storage_caching: false,
        no_rpc_rate_limit: false,
        rpc_endpoints: {
          anvil: 'http://127.0.0.1:8545',
          arbitrum_sepolia: `https://arb-sepolia.g.alchemy.com/v2/\${RPC_API_KEY}`,
          optimism_sepolia: `https://opt-sepolia.g.alchemy.com/v2/\${RPC_API_KEY}`,
          sepolia: `https://eth-sepolia.g.alchemy.com/v2/\${RPC_API_KEY\}`,
        },
        use_literal_content: false,
        bytecode_hash: 'ipfs',
        cbor_metadata: true,
        revert_strings: null,
        sparse_mode: false,
        build_info: true,
        build_info_path: null,
        fmt: {
          line_length: 120,
          tab_width: 4,
          bracket_spacing: false,
          int_types: 'long',
          multiline_func_header: 'attributes_first',
          quote_style: 'double',
          number_underscore: 'preserve',
          hex_underscore: 'remove',
          single_line_statement_blocks: 'preserve',
          override_spacing: false,
          wrap_comments: false,
          ignore: [],
          contract_new_lines: false,
          sort_imports: false,
        },
        doc: {
          out: 'docs',
          title: '',
          book: 'book.toml',
          homepage: 'README.md',
          ignore: [],
        },
        fs_permissions: [
          {
            access: true,
            path: './',
          },
        ],
        cancun: false,
      }

      const expected = {
        src: 'src',
        test: 'test',
        script: 'script',
        out: 'out',
        libs: ['node_modules'],
        remappings: [
          '@sphinx-labs/plugins/=node_modules/@sphinx-labs/plugins/contracts/foundry/',
          '@sphinx-labs/contracts/=node_modules/@sphinx-labs/contracts/',
          'forge-std/=node_modules/forge-std/src/',
          'sphinx-forge-std/=node_modules/@sphinx-labs/plugins/node_modules/sphinx-forge-std/src/',
          'sphinx-solmate/=node_modules/@sphinx-labs/plugins/node_modules/sphinx-solmate/src/',
          'ds-test/=node_modules/ds-test/src/',
          '@openzeppelin/contracts-upgradeable/=../../node_modules/@openzeppelin/contracts-upgradeable/',
          '@openzeppelin/contracts/=../../node_modules/@openzeppelin/contracts/',
          'solidity-stringutils=../../node_modules/solidity-stringutils/src/',
          'solmate/src/=../../node_modules/solmate/src/',
        ],
        auto_detect_remappings: true,
        libraries: [],
        cache: true,
        cache_path: 'cache',
        broadcast: 'broadcast',
        allow_paths: ['../..'],
        include_paths: [],
        force: false,
        evm_version: 'paris',
        gas_reports: ['*'],
        gas_reports_ignore: [],
        solc: null,
        auto_detect_solc: true,
        offline: false,
        optimizer: false,
        optimizer_runs: 200,
        optimizer_details: null,
        model_checker: null,
        verbosity: 0,
        eth_rpc_url: null,
        eth_rpc_jwt: null,
        etherscan_api_key: null,
        etherscan: {
          optimism_sepolia: {
            url: 'https://api-optimistic.etherscan.io/api?',
            key: 'MockEtherscanKey', // This is the replaced value
          },
        },
        ignored_error_codes: ['license', 'code-size', 'init-code-size'],
        deny_warnings: false,
        match_test: null,
        no_match_test: null,
        match_contract: null,
        no_match_contract: null,
        match_path: null,
        no_match_path: null,
        fuzz: {
          runs: 256,
          max_test_rejects: 65536,
          seed: null,
          dictionary_weight: 40,
          include_storage: true,
          include_push_bytes: true,
          max_fuzz_dictionary_addresses: 15728640,
          max_fuzz_dictionary_values: 6553600,
        },
        invariant: {
          runs: 256,
          depth: 15,
          fail_on_revert: false,
          call_override: false,
          dictionary_weight: 80,
          include_storage: true,
          include_push_bytes: true,
          max_fuzz_dictionary_addresses: 15728640,
          max_fuzz_dictionary_values: 6553600,
          shrink_sequence: true,
          shrink_run_limit: 262144,
        },
        ffi: false,
        sender: '0x1804c8ab1f12e6bbf3894d4083f33e07309d1f38',
        tx_origin: '0x1804c8ab1f12e6bbf3894d4083f33e07309d1f38',
        initial_balance: '0xffffffffffffffffffffffff',
        block_number: 1,
        fork_block_number: null,
        chain_id: null,
        gas_limit: 9223372036854775807,
        code_size_limit: null,
        gas_price: null,
        block_base_fee_per_gas: 0,
        block_coinbase: '0x0000000000000000000000000000000000000000',
        block_timestamp: 1,
        block_difficulty: 0,
        block_prevrandao:
          '0x0000000000000000000000000000000000000000000000000000000000000000',
        block_gas_limit: null,
        memory_limit: 134217728,
        extra_output: ['storageLayout'],
        extra_output_files: [],
        names: false,
        sizes: false,
        via_ir: false,
        rpc_storage_caching: {
          chains: 'all',
          endpoints: 'all',
        },
        no_storage_caching: false,
        no_rpc_rate_limit: false,
        rpc_endpoints: {
          anvil: 'http://127.0.0.1:8545',
          arbitrum_sepolia: 'https://arb-sepolia.g.alchemy.com/v2/MockApiKey',
          optimism_sepolia: 'https://opt-sepolia.g.alchemy.com/v2/MockApiKey',
          sepolia: 'https://eth-sepolia.g.alchemy.com/v2/MockApiKey',
        },
        use_literal_content: false,
        bytecode_hash: 'ipfs',
        cbor_metadata: true,
        revert_strings: null,
        sparse_mode: false,
        build_info: true,
        build_info_path: null,
        fmt: {
          line_length: 120,
          tab_width: 4,
          bracket_spacing: false,
          int_types: 'long',
          multiline_func_header: 'attributes_first',
          quote_style: 'double',
          number_underscore: 'preserve',
          hex_underscore: 'remove',
          single_line_statement_blocks: 'preserve',
          override_spacing: false,
          wrap_comments: false,
          ignore: [],
          contract_new_lines: false,
          sort_imports: false,
        },
        doc: {
          out: 'docs',
          title: '',
          book: 'book.toml',
          homepage: 'README.md',
          ignore: [],
        },
        fs_permissions: [
          {
            access: true,
            path: './',
          },
        ],
        cancun: false,
      }
      expect(replaceEnvVariables(input)).to.deep.equal(expected)
    })
  })

  describe('getConfigArtifacts', () => {
    let getConfigArtifacts: GetConfigArtifacts

    before(() => {
      getConfigArtifacts = makeGetConfigArtifacts(
        foundryToml.artifactFolder,
        foundryToml.buildInfoFolder,
        process.cwd(),
        foundryToml.cachePath
      )
    })

    // Test that this function returns an empty object if it can't find an artifact for the given
    // init code. This ensures the user can deploy contracts that are defined as inline bytecode,
    // like a `CREATE3` proxy.
    it('returns empty object for init code that does not belong to a source file', async () => {
      const artifacts = await getConfigArtifacts([
        '0x67363d3d37363d34f03d5260086018f3', // `CREATE3` proxy initcode
      ])
      expect(artifacts).deep.equals({
        buildInfos: {},
        configArtifacts: {},
      })
    })
  })

  describe('isInitCodeMatch', () => {
    const coder = ethers.AbiCoder.defaultAbiCoder()

    /**
     * A helper function that creates the artifact parameter passed into `isInitCodeMatch`.
     */
    const makeArtifactParam = (
      artifact: ContractArtifact
    ): {
      bytecode: string
      linkReferences: LinkReferences
      constructorFragment?: ethers.ConstructorFragment
    } => {
      const iface = new ethers.Interface(artifact.abi)
      const constructorFragment = iface.fragments.find(
        ConstructorFragment.isFragment
      )

      return {
        bytecode: artifact.bytecode,
        linkReferences: artifact.linkReferences,
        constructorFragment,
      }
    }

    it('returns false for different contracts', () => {
      const artifactOne = parseFoundryContractArtifact(MyContract1Artifact)
      const artifactTwo = parseFoundryContractArtifact(MyContract2Artifact)

      expect(
        isInitCodeMatch(artifactOne.bytecode, makeArtifactParam(artifactTwo))
      ).to.equal(false)
    })

    it('returns false if artifact bytecode length is greater than actual bytecode length', () => {
      const artifact = parseFoundryContractArtifact(MyContract2Artifact)
      const actualInitCode = '0x22'
      expect(getBytesLength(artifact.bytecode)).gt(
        getBytesLength(actualInitCode)
      )

      expect(
        isInitCodeMatch(actualInitCode, makeArtifactParam(artifact))
      ).to.equal(false)
    })

    it('returns false if constructor cannot be ABI decoded', () => {
      const artifact = parseFoundryContractArtifact(MyContract1Artifact)

      // Encode an incorrect number of constructor args. (There should be 4, but we only encode 3).
      const encodedConstructorArgs = coder.encode(
        ['int256', 'uint256', 'address'],
        [3, 4, makeAddress(5)]
      )

      // Sanity check that we're encoding the wrong number of constructor args.
      const constructorFragment = new ethers.Interface(
        artifact.abi
      ).fragments.find(ConstructorFragment.isFragment)
      // Narrow the TypeScript type of the constructor fragment.
      if (!constructorFragment) {
        throw new Error(`Could not find constructor fragment.`)
      }
      expect(constructorFragment.inputs.length).does.not.equal(3)

      const initCodeWithArgs = ethers.concat([
        artifact.bytecode,
        encodedConstructorArgs,
      ])

      expect(
        isInitCodeMatch(initCodeWithArgs, makeArtifactParam(artifact))
      ).to.equal(false)
    })

    it('returns true for contract with no constructor args', () => {
      const artifact = parseFoundryContractArtifact(MyContract2Artifact)

      expect(
        isInitCodeMatch(artifact.bytecode, makeArtifactParam(artifact))
      ).to.equal(true)
    })

    it('returns true for contract with constructor args', () => {
      const artifact = parseFoundryContractArtifact(MyContract1Artifact)

      const encodedConstructorArgs = coder.encode(
        ['int256', 'uint256', 'address', 'address'],
        [3, 4, makeAddress(5), makeAddress(6)]
      )
      const initCodeWithArgs = ethers.concat([
        artifact.bytecode,
        encodedConstructorArgs,
      ])

      expect(
        isInitCodeMatch(initCodeWithArgs, makeArtifactParam(artifact))
      ).to.equal(true)
    })

    it('returns true for large contract', () => {
      const artifact = parseFoundryContractArtifact(MyLargeContractArtifact)

      expect(
        isInitCodeMatch(artifact.bytecode, makeArtifactParam(artifact))
      ).to.equal(true)
    })

    it('returns true for contract with libraries', async () => {
      const artifact = parseFoundryContractArtifact(
        MyContractWithLibrariesArtifact
      )

      const chainId = BigInt(31337)
      // Start an Anvil node, then deploy the contract and its libraries, then kill the Anvil node.
      // We must deploy the contract so that its bytecode contains the actual library addresses
      // instead of placeholders.
      await startAnvilNodes([chainId])
      const broadcast = await runForgeScript(
        'contracts/test/script/Libraries.s.sol',
        foundryToml.broadcastFolder,
        getAnvilRpcUrl(chainId),
        'MyContractWithLibraries_Script'
      )
      await killAnvilNodes([chainId])

      const initCodeWithArgs =
        broadcast.transactions[broadcast.transactions.length - 1].transaction
          .data
      // Narrow the TypeScript type.
      if (!initCodeWithArgs) {
        throw new Error(`Could not find init code.`)
      }

      expect(
        isInitCodeMatch(initCodeWithArgs, makeArtifactParam(artifact))
      ).to.equal(true)
    })

    it('returns true for contract with immutable variables', async () => {
      const artifact = parseFoundryContractArtifact(MyImmutableContractArtifact)

      // Create the contract's init code. We don't need to deploy the contract because immutable
      // variable references only exist in the runtime bytecode and not the init code. This is
      // different from library placeholders, which exist in both the runtime bytecode and the init
      // code.
      const encodedConstructorArgs = coder.encode(['uint256', 'uint8'], [1, 2])
      const initCodeWithArgs = ethers.concat([
        artifact.bytecode,
        encodedConstructorArgs,
      ])

      expect(
        isInitCodeMatch(initCodeWithArgs, makeArtifactParam(artifact))
      ).to.equal(true)
    })
  })

  describe('isDeployedCodeMatch', () => {
    it('returns false if bytecode length differs', () => {
      expect(
        isDeployedCodeMatch('0x11', {
          deployedBytecode: '0x1111',
          deployedLinkReferences: {},
          immutableReferences: {},
        })
      ).equals(false)
    })

    it('returns true for artifact with no linked libraries or immutable variables', () => {
      const artifact = parseFoundryContractArtifact(MyContract1Artifact)
      const {
        deployedBytecode,
        deployedLinkReferences,
        linkReferences,
        immutableReferences,
      } = artifact

      expect(Object.keys(deployedLinkReferences).length).equals(0)
      expect(Object.keys(linkReferences).length).equals(0)
      expect(Object.keys(immutableReferences).length).equals(0)

      // The artifact bytecode matches the actual bytecode in this scenario because the contract has
      // no linked libraries or immutable variables.
      expect(
        isDeployedCodeMatch(deployedBytecode, {
          deployedBytecode,
          deployedLinkReferences,
          immutableReferences,
        })
      ).equals(true)
    })

    it('returns true for artifact with linked libraries and immutable variables', async () => {
      const artifact = parseFoundryContractArtifact(
        MyContractWithLibrariesAndImmutablesArtifact
      )
      const {
        deployedBytecode,
        deployedLinkReferences,
        linkReferences,
        immutableReferences,
      } = artifact

      expect(Object.keys(deployedLinkReferences).length).greaterThan(0)
      expect(Object.keys(linkReferences).length).greaterThan(0)
      expect(Object.keys(immutableReferences).length).greaterThan(0)

      // Start an Anvil node, then deploy the contract and its libraries, then kill the Anvil node.
      // We must deploy the contract so that its bytecode contains the actual library addresses
      // instead of placeholders.
      const chainId = BigInt(31337)
      await startAnvilNodes([chainId])
      const broadcast = await runForgeScript(
        'contracts/test/script/Libraries.s.sol',
        foundryToml.broadcastFolder,
        getAnvilRpcUrl(chainId),
        'MyContractWithLibrariesAndImmutables_Script'
      )
      const contractAddress =
        broadcast.transactions[broadcast.transactions.length - 1]
          .contractAddress
      // Narrow the TypeScript type.
      if (!contractAddress) {
        throw new Error(`Could not find contract address. Should never happen.`)
      }
      const provider = new SphinxJsonRpcProvider(`http://127.0.0.1:8545`)

      const actualDeployedCode = await provider.getCode(contractAddress)
      await killAnvilNodes([chainId])

      expect(
        isDeployedCodeMatch(actualDeployedCode, {
          deployedBytecode,
          deployedLinkReferences,
          immutableReferences,
        })
      ).equals(true)
    })
  })

  describe('assertNoLinkedLibraries', () => {
    const projectRoot = process.cwd()

    let readBuildInfoCacheStub: sinon.SinonStub

    beforeEach(() => {
      readBuildInfoCacheStub = sinon.stub(
        sphinxFoundryUtils,
        'readBuildInfoCache'
      )
    })

    afterEach(() => {
      sinon.restore()
    })

    it('throws an error if artifact contains linked library', async () => {
      const artifact = parseFoundryContractArtifact(
        MyContractWithLibrariesArtifact
      )
      readBuildInfoCacheStub.resolves(getFakeBuildInfoCache(artifact))

      await expect(
        assertNoLinkedLibraries(
          artifact.deployedBytecode,
          foundryToml.cachePath,
          foundryToml.artifactFolder,
          projectRoot
        )
      ).to.eventually.be.rejectedWith(
        getDetectedLinkedLibraryErrorMessage(
          artifact.sourceName,
          artifact.contractName
        )
      )
    })

    it('succeeds if no linked libraries are found', async () => {
      const artifact = parseFoundryContractArtifact(MyContract1Artifact)
      readBuildInfoCacheStub.resolves(getFakeBuildInfoCache(artifact))

      await expect(
        assertNoLinkedLibraries(
          artifact.deployedBytecode,
          foundryToml.cachePath,
          foundryToml.artifactFolder,
          projectRoot
        )
      ).to.eventually.be.fulfilled
    })
  })

  describe('findContractArtifactForDeployedCode', () => {
    const projectRoot = process.cwd()

    let readBuildInfoCacheStub: sinon.SinonStub

    beforeEach(() => {
      readBuildInfoCacheStub = sinon.stub(
        sphinxFoundryUtils,
        'readBuildInfoCache'
      )
    })

    afterEach(() => {
      sinon.restore()
    })

    it('returns undefined if no contract artifact exists', async () => {
      readBuildInfoCacheStub.returns(getDummyBuildInfoCache())
      const result = findContractArtifactForDeployedCode(
        'dummyDeployedCode',
        foundryToml.cachePath,
        projectRoot,
        foundryToml.artifactFolder
      )
      await expect(result).to.eventually.be.undefined
    })

    it('returns contract artifact', async () => {
      const expectedArtifact = parseFoundryContractArtifact(MyContract1Artifact)
      readBuildInfoCacheStub.returns(getFakeBuildInfoCache(expectedArtifact))
      const artifact = await findContractArtifactForDeployedCode(
        expectedArtifact.deployedBytecode,
        foundryToml.cachePath,
        projectRoot,
        foundryToml.artifactFolder
      )
      expect(artifact).to.deep.equal(expectedArtifact)
    })
  })

  describe('validateProposalNetworks', () => {
    const validMainnetOne = 'mainnet-1'
    const validMainnetTwo = 'other-mainnet-2'
    const validTestnetOne = 'testnet'
    const validNetworks = [validMainnetOne, validMainnetTwo, validTestnetOne]
    const unsupportedNetworkOne = 'unsupported1'
    const unsupportedNetworkTwo = 'unsupported2'

    let isLiveNetwork: sinon.SinonSpy
    let rpcEndpoints: FoundryToml['rpcEndpoints']
    let getNetworkStub: sinon.SinonStub

    beforeEach(() => {
      rpcEndpoints = {
        [validMainnetOne]: 'http://mainnet.rpc',
        [validTestnetOne]: 'http://testnet.rpc',
        [validMainnetTwo]: 'http://other-mainnet.rpc',
        [unsupportedNetworkOne]: 'http://unsupported-1.rpc',
        [unsupportedNetworkTwo]: 'http://unsupported-2.rpc',
      }

      getNetworkStub = sinon.stub()

      isLiveNetwork = sinon.fake.resolves(true)
      sinon
        .stub(SphinxJsonRpcProvider.prototype, 'getNetwork')
        .callsFake(getNetworkStub)
    })

    afterEach(() => {
      sinon.restore()
    })

    it('throws an error if no CLI networks are provided', async () => {
      await expect(
        validateProposalNetworks([], [], [], rpcEndpoints, isLiveNetwork)
      ).to.be.rejectedWith(
        `Expected at least one network, but none were supplied.`
      )
    })

    it('throws an error for missing RPC endpoints', async () => {
      const unknownNetworks = ['unknown1', 'unknown2']
      await expect(
        validateProposalNetworks(
          unknownNetworks,
          [],
          [],
          rpcEndpoints,
          isLiveNetwork
        )
      ).to.be.rejectedWith(getMissingEndpointErrorMessage(unknownNetworks))
    })

    it('throws an error for failed requests to RPC endpoints', async () => {
      getNetworkStub.rejects(new Error('Request failed'))
      await expect(
        validateProposalNetworks(
          validNetworks,
          [],
          [],
          rpcEndpoints,
          isLiveNetwork
        )
      ).to.be.rejectedWith(getFailedRequestErrorMessage(validNetworks))
    })

    it('throws an error for unsupported networks', async () => {
      const unsupportedChainIdOne = '-1'
      const unsupportedChainIdTwo = '-2'
      const unsupportedNetworks = [
        { networkName: unsupportedNetworkOne, chainId: unsupportedChainIdOne },
        { networkName: unsupportedNetworkTwo, chainId: unsupportedChainIdTwo },
      ]

      getNetworkStub
        .onFirstCall()
        .resolves({ chainId: BigInt(unsupportedChainIdOne) })
      getNetworkStub
        .onSecondCall()
        .resolves({ chainId: BigInt(unsupportedChainIdTwo) })

      await expect(
        validateProposalNetworks(
          [unsupportedNetworkOne, unsupportedNetworkTwo],
          [],
          [],
          rpcEndpoints,
          isLiveNetwork
        )
      ).to.be.rejectedWith(
        getUnsupportedNetworkErrorMessage(unsupportedNetworks)
      )
    })

    it('throws error for local networks', async () => {
      getNetworkStub.resolves({ chainId: BigInt(1) })
      isLiveNetwork = sinon.fake.resolves(false)
      await expect(
        validateProposalNetworks(
          validNetworks,
          [],
          [],
          rpcEndpoints,
          isLiveNetwork
        )
      ).to.be.rejectedWith(getLocalNetworkErrorMessage(validNetworks))
    })

    it('throws an error for mixed network types (test and production)', async () => {
      const mixedNetworks = [
        { networkType: 'Mainnet', network: validMainnetOne },
        { networkType: 'Mainnet', network: validMainnetTwo },
        { networkType: 'Testnet', network: validTestnetOne },
      ]

      getNetworkStub.onFirstCall().resolves({ chainId: BigInt(1) }) // Production network (Ethereum)
      getNetworkStub.onSecondCall().resolves({ chainId: BigInt(10) }) // Production network (Optimism)
      getNetworkStub.onThirdCall().resolves({ chainId: BigInt(11155111) }) // Test network (Sepolia)

      await expect(
        validateProposalNetworks(
          validNetworks,
          [],
          [],
          rpcEndpoints,
          isLiveNetwork
        )
      ).to.be.rejectedWith(getMixedNetworkTypeErrorMessage(mixedNetworks))
    })

    it('throws an error if sphinxConfig.mainnets contains all testnets', async () => {
      getNetworkStub.resolves({ chainId: BigInt(11155111) }) // Test network (Sepolia)

      await expect(
        validateProposalNetworks(
          ['mainnets'],
          [],
          [validTestnetOne],
          rpcEndpoints,
          isLiveNetwork
        )
      ).to.be.rejectedWith(SphinxConfigMainnetsContainsTestnetsErrorMessage)
    })

    it('throws an error if sphinxConfig.testnets contains all mainnets', async () => {
      getNetworkStub.resolves({ chainId: BigInt(1) }) // Production network

      await expect(
        validateProposalNetworks(
          ['testnets'],
          [validMainnetOne, validMainnetTwo],
          [],
          rpcEndpoints,
          isLiveNetwork
        )
      ).to.be.rejectedWith(SphinxConfigTestnetsContainsMainnetsErrorMessage)
    })

    it('validates CLI networks correctly', async () => {
      getNetworkStub.onFirstCall().resolves({ chainId: BigInt(1) }) // Production network (Ethereum)
      getNetworkStub.onSecondCall().resolves({ chainId: BigInt(10) }) // Production network (Optimism)

      const result = await validateProposalNetworks(
        [validMainnetOne, validMainnetTwo],
        [],
        [],
        rpcEndpoints,
        isLiveNetwork
      )
      expect(result.rpcUrls).to.deep.equals([
        rpcEndpoints[validMainnetOne],
        rpcEndpoints[validMainnetTwo],
      ])
      expect(result.isTestnet).to.be.false
    })

    it('validates config mainnets correctly', async () => {
      getNetworkStub.onFirstCall().resolves({ chainId: BigInt(1) }) // Production network (Ethereum)
      getNetworkStub.onSecondCall().resolves({ chainId: BigInt(10) }) // Production network (Optimism)

      const result = await validateProposalNetworks(
        ['mainnets'],
        [],
        [validMainnetOne, validMainnetTwo],
        rpcEndpoints,
        isLiveNetwork
      )
      expect(result.rpcUrls).to.deep.equals([
        rpcEndpoints[validMainnetOne],
        rpcEndpoints[validMainnetTwo],
      ])
      expect(result.isTestnet).to.be.false
    })

    it('validates config testnets correctly', async () => {
      getNetworkStub.resolves({ chainId: BigInt(11155111) }) // Test network (Sepolia)

      const result = await validateProposalNetworks(
        ['testnets'],
        [validTestnetOne],
        [],
        rpcEndpoints,
        isLiveNetwork
      )
      expect(result.rpcUrls).to.deep.equals([rpcEndpoints[validTestnetOne]])
      expect(result.isTestnet).to.be.true
    })
  })

  describe('parseScriptFunctionCalldata', () => {
    let spawnAsyncStub: sinon.SinonStub

    beforeEach(() => {
      spawnAsyncStub = sinon.stub()
    })

    afterEach(() => {
      sinon.restore()
    })

    it('throws an error if called with no arguments', async () => {
      await expect(parseScriptFunctionCalldata([])).to.be.rejectedWith(
        SigCalledWithNoArgsErrorMessage
      )
    })

    it('throws an error if spawnAsync fails on selector retrieval', async () => {
      const mockSig = ['testFunc(uint256)']
      const errorMessage = 'spawnAsync failed on selector retrieval'

      spawnAsyncStub
        .onFirstCall()
        .resolves({ code: 1, stdout: '', stderr: errorMessage })

      await expect(
        parseScriptFunctionCalldata(mockSig, spawnAsyncStub)
      ).to.be.rejectedWith(errorMessage)
    })

    it('throws an error if spawnAsync fails on abi-encode', async () => {
      const mockSig = ['testFunc(uint256)', '1234']
      const errorMessage = 'spawnAsync failed on abi-encode'

      spawnAsyncStub
        .onFirstCall()
        .resolves({ code: 0, stdout: 'selector', stderr: '' })
      spawnAsyncStub
        .onSecondCall()
        .resolves({ code: 1, stdout: '', stderr: errorMessage })

      await expect(
        parseScriptFunctionCalldata(mockSig, spawnAsyncStub)
      ).to.be.rejectedWith(errorMessage)
    })

    it('throws an error if the first argument is a function with no parentheses', async () => {
      const invalidSig = ['invalidSig']
      await expect(parseScriptFunctionCalldata(invalidSig)).to.be.rejectedWith(
        InvalidFirstSigArgumentErrorMessage
      )
    })

    it('throws an error if the first argument is a hex string with odd number of bytes', async () => {
      const invalidSig = ['0x111']
      await expect(parseScriptFunctionCalldata(invalidSig)).to.be.rejectedWith(
        InvalidFirstSigArgumentErrorMessage
      )
    })

    it('should handle valid function signature with parentheses', async () => {
      const sig = ['testFunc(uint256)', '1234']
      const expectedCalldata = encodeFunctionCalldata(sig)

      const actualCalldata = await parseScriptFunctionCalldata(sig)
      expect(actualCalldata).to.equal(expectedCalldata)
    })

    it("should return the input if it's an 0x-prefixed hex string", async () => {
      const calldata = encodeFunctionCalldata(['testFunc(uint256)', '1234'])

      const actualCalldata = await parseScriptFunctionCalldata([calldata])
      expect(actualCalldata).to.equal(calldata)
    })

    it('should return the 0x-prefixed input if the input is a hex string that is not 0x-prefixed', async () => {
      const with0x = encodeFunctionCalldata(['testFunc(uint256)', '1234'])
      const calldata = remove0x(with0x)

      const actualCalldata = await parseScriptFunctionCalldata([calldata])
      expect(actualCalldata).to.equal(with0x)
    })

    it('should trim strings surrounding hex string', async () => {
      const calldata = encodeFunctionCalldata(['testFunc(uint256)', '1234'])
      const withStrings = `""""""${calldata}""""""`

      const actualCalldata = await parseScriptFunctionCalldata([withStrings])
      expect(actualCalldata).to.equal(calldata)
    })
  })

  // This test suite should check for backwards compatibility between different versions of Sphinx's
  // build info cache. This is important because our plugin may break unexpectedly if we don't
  // gracefully handle previous cache versions.
  describe('readBuildInfoCache', () => {
    let readFileSyncStub: sinon.SinonStub
    let existsSyncStub: sinon.SinonStub

    beforeEach(() => {
      readFileSyncStub = sinon.stub(sphinxCoreUtils, 'readFileSync')
      existsSyncStub = sinon.stub(sphinxCoreUtils, 'existsSync')
    })

    afterEach(() => {
      sinon.restore()
    })

    it('returns empty cache if cache file does not exist', () => {
      existsSyncStub.returns(false)
      readFileSyncStub.throws(new Error('File does not exist'))
      const cache = readBuildInfoCache(foundryToml.cachePath)
      expect(cache).to.deep.equal({
        _format: 'sphinx-build-info-cache-1',
        entries: {},
      })
    })

    // Test that the 'sphinx-build-info-cache-1' version is compatible with the original
    // version, which did not have a `_format` string.
    it('returns empty cache for original cache structure', () => {
      const originalCache = {
        'dummyBuildInfoId.json': {
          name: 'dummyBuildInfoName.json',
          time: 123,
          contracts: [],
        },
      }
      existsSyncStub.returns(true)
      readFileSyncStub.returns(JSON.stringify(originalCache))
      const cache = readBuildInfoCache(foundryToml.cachePath)
      expect(cache).to.deep.equal({
        _format: 'sphinx-build-info-cache-1',
        entries: {},
      })
    })
  })
})
