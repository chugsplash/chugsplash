import * as path from 'path'

// Hardhat plugins
import '@nomiclabs/hardhat-ethers'
import '@openzeppelin/hardhat-upgrades'
import '../dist'

import { expect } from 'chai'
import hre from 'hardhat'
import {
  assertValidLibraries,
  readUnvalidatedChugSplashConfig,
  readValidatedChugSplashConfig,
  resolveContractAddresses,
} from '@chugsplash/core'

import { getArtifactPaths } from '../src/hardhat/artifacts'
import { createChugSplashRuntime } from '../src/utils'

const variableValidateConfigPath = './chugsplash/VariableValidation.config.ts'
const constructorArgConfigPath =
  './chugsplash/ConstructorArgValidation.config.ts'
const libraryValidationConfigPath = './chugsplash/LibraryValidation.config.ts'
const outOfOrderLibraryConfigPath = './chugsplash/OutOfOrderLibrary.config.ts'

describe('Validate', () => {
  let validationOutput = ''

  before(async () => {
    const provider = hre.ethers.provider
    const varValidationUserConfig = await readUnvalidatedChugSplashConfig(
      variableValidateConfigPath
    )

    const constructorArgsValidationUserConfig =
      await readUnvalidatedChugSplashConfig(constructorArgConfigPath)

    const libraryValidationUserConfig = await readUnvalidatedChugSplashConfig(
      libraryValidationConfigPath
    )

    const outOfOrderLibraryUserConfig = await readUnvalidatedChugSplashConfig(
      outOfOrderLibraryConfigPath
    )

    const varValidationArtifactPaths = await getArtifactPaths(
      hre,
      varValidationUserConfig.contracts,
      hre.config.paths.artifacts,
      path.join(hre.config.paths.artifacts, 'build-info')
    )

    const constructorArgsValidationArtifactPaths = await getArtifactPaths(
      hre,
      constructorArgsValidationUserConfig.contracts,
      hre.config.paths.artifacts,
      path.join(hre.config.paths.artifacts, 'build-info')
    )

    const libraryValidationArtifactPaths = await getArtifactPaths(
      hre,
      libraryValidationUserConfig.contracts,
      hre.config.paths.artifacts,
      path.join(hre.config.paths.artifacts, 'build-info')
    )

    const outOfOrderLibraryArtifactPaths = await getArtifactPaths(
      hre,
      outOfOrderLibraryUserConfig.contracts,
      hre.config.paths.artifacts,
      path.join(hre.config.paths.artifacts, 'build-info')
    )

    process.stderr.write = (message: string) => {
      console.log(message)
      validationOutput += message
      return true
    }

    // Construct CRE
    const cre = await createChugSplashRuntime(
      '',
      false,
      true,
      hre,
      false,
      process.stderr
    )

    // Run validation for variable validation config
    console.log('aaaaaaaaaaa')
    await readValidatedChugSplashConfig(
      provider,
      variableValidateConfigPath,
      varValidationArtifactPaths,
      'hardhat',
      cre,
      false
    )

    // Run validation for constructor arg validation config
    console.log('bbbbbbbbbbb')
    await readValidatedChugSplashConfig(
      provider,
      constructorArgConfigPath,
      constructorArgsValidationArtifactPaths,
      'hardhat',
      cre,
      false
    )

    // Run validation for unnecessary libraries config
    console.log('cccccccccccc')
    await assertValidLibraries(
      libraryValidationUserConfig,
      libraryValidationArtifactPaths,
      'hardhat',
      cre,
      false
    )

    // Run validation for out of order libraries config
    console.log('dddddddddddd')
    await resolveContractAddresses(
      outOfOrderLibraryUserConfig,
      outOfOrderLibraryArtifactPaths,
      'hardhat',
      { stream: cre.stream, silent: cre.silent },
      false
    )
  })

  it('did catch invalid arrayInt8', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for variable arrayInt8 expected number, string, or BigNumber but got array'
    )
  })

  it('did catch invalid int8OutsideRange', async () => {
    expect(validationOutput).to.have.string(
      'invalid value for int8OutsideRange: 255, outside valid range: [-128:127]'
    )
  })

  it('did catch invalid uint8OutsideRange', async () => {
    expect(validationOutput).to.have.string(
      'invalid value for uint8OutsideRange: 256, outside valid range: [0:255]'
    )
  })

  it('did catch invalid intAddress', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for intAddress: 1, expected address string but got number'
    )
  })

  it('did catch invalid arrayAddress', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for arrayAddress: 0x00000000, expected address string but got array'
    )
  })

  it('did catch invalid shortAddress', async () => {
    expect(validationOutput).to.have.string(
      'invalid address for shortAddress: 0x00000000'
    )
  })

  it('did catch invalid intBytes32', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for intBytes32: 1, expected DataHexString but got number'
    )
  })

  it('did catch invalid arrayBytes32', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for arrayBytes32: 1, expected DataHexString but got array'
    )
  })

  it('did catch invalid shortBytes32', async () => {
    expect(validationOutput).to.have.string(
      'invalid length for bytes32 variable shortBytes32: 0x00000000'
    )
  })

  it('did catch invalid longBytes8', async () => {
    expect(validationOutput).to.have.string(
      'invalid length for bytes8 variable longBytes8: 0x1111111111111111111111111111111111111111111111111111111111111111'
    )
  })

  it('did catch invalid malformedBytes16', async () => {
    expect(validationOutput).to.have.string(
      'invalid input format for variable malformedBytes16, expected DataHexString but got 11111111111111111111111111111111'
    )
  })

  it('did catch invalid intBoolean', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for variable intBoolean, expected boolean but got number'
    )
  })

  it('did catch invalid stringBoolean', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for variable stringBoolean, expected boolean but got string'
    )
  })

  it('did catch invalid arrayBoolean', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for variable arrayBoolean, expected boolean but got array'
    )
  })

  it('did catch invalid oversizedArray', async () => {
    expect(validationOutput).to.have.string(
      'Expected array of size 2 for oversizedArray but got [1,2,3]'
    )
  })

  it('did catch invalid oversizedNestedArray', async () => {
    expect(validationOutput).to.have.string(
      'Expected array of size 2 for oversizedNestedArray but got [[1,2],[1,2],[1,2]]'
    )
  })

  it('did catch invalid invalidBoolArray', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for variable invalidBoolArray, expected boolean but got string'
    )
  })

  it('did catch invalid invalidBytes32Array', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for invalidBytes32Array: 1, expected DataHexString but got number'
    )
  })

  it('did catch invalid invalidAddressArray', async () => {
    expect(validationOutput).to.have.string(
      'invalid address for invalidAddressArray: 0x00000000'
    )
  })

  it('did catch invalid invalidStringStringMapping', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for invalidStringStringMapping, expected DataHexString but got number'
    )
  })

  it('did catch invalid invalidStringIntMapping', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for variable invalidStringIntMapping expected number, string, or BigNumber but got boolean'
    )
  })

  it('did catch invalid invalidNestedStringIntBoolMapping', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for variable invalidNestedStringIntBoolMapping expected number, string, or BigNumber but got boolean'
    )
  })

  it('did catch missing variables', async () => {
    expect(validationOutput).to.have.string(
      'were not defined in the ChugSplash config file'
    )
    expect(validationOutput).to.have.string('notSetUint')
    expect(validationOutput).to.have.string('notSetString')
  })

  it('did catch extra variables', async () => {
    expect(validationOutput).to.have.string(
      'defined in the ChugSplash config file that do not exist in the contract'
    )
    expect(validationOutput).to.have.string('extraVar')
    expect(validationOutput).to.have.string('anotherExtraVar')
  })

  it('did catch odd dynamic bytes', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for variable oddDynamicBytes, expected DataHexString but got'
    )
  })

  it('did catch odd fixed bytes', async () => {
    expect(validationOutput).to.have.string(
      'invalid input format for variable oddStaticBytes, expected DataHexString but got'
    )
  })

  it('did catch extra constructor argument', async () => {
    expect(validationOutput).to.have.string(
      'but are not present in the contract constructor'
    )
    expect(validationOutput).to.have.string('_immutableUint')
  })

  it('did catch missing constructor argument', async () => {
    expect(validationOutput).to.have.string('but were not found in your config')
    expect(validationOutput).to.have.string('_immutableBytes')
  })

  it('did catch variables in immutable contract', async () => {
    expect(validationOutput).to.have.string(
      `Detected variables for contract 'Stateless', but variables are not supported for non-proxied contracts.`
    )
  })

  it('did catch out of order library reference', async () => {
    expect(validationOutput).to.have.string(
      'Detected out of order library references'
    )
    expect(validationOutput).to.have.string(
      'ExternalLibrary: {{ ExternalLibrary }}'
    )
  })

  it('did catch unnecessary library', async () => {
    expect(validationOutput).to.have.string(
      'You tried to link the contract Stateless with UnnecessaryLibrary, which is not one of its libraries.'
    )
  })

  it('did catch invalid address', async () => {
    expect(validationOutput).to.have.string(
      'You tried to link the contract Stateless with the library ExternalLibrary, but provided this invalid address: invalidAddress'
    )
  })

  it('did catch missing library link', async () => {
    expect(validationOutput).to.have.string(
      'The contract Stateless is missing links for the following libraries:'
    )
    expect(validationOutput).to.have.string(
      'contracts/ExternalLibrary.sol:ExternalLibrary'
    )
  })
})
