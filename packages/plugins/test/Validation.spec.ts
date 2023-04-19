import * as path from 'path'

// Hardhat plugins
import '@nomiclabs/hardhat-ethers'
import '@openzeppelin/hardhat-upgrades'
import '../dist'

import { expect } from 'chai'
import hre from 'hardhat'
import {
  assertValidUserConfigFields,
  readUnvalidatedChugSplashConfig,
  readValidatedChugSplashConfig,
} from '@chugsplash/core'

import { getArtifactPaths } from '../src/hardhat/artifacts'
import { createChugSplashRuntime } from '../src/utils'

const variableValidateConfigPath = './chugsplash/VariableValidation.config.ts'
const constructorArgConfigPath =
  './chugsplash/ConstructorArgValidation.config.ts'
const noProxyContractReferenceConfigPath =
  './chugsplash/NoProxyContractReference.config.ts'

describe('Validate', () => {
  let validationOutput = ''

  before(async () => {
    const provider = hre.ethers.provider
    const varValidationUserConfig = await readUnvalidatedChugSplashConfig(
      variableValidateConfigPath
    )
    const constructorArgsValidationUserConfig =
      await readUnvalidatedChugSplashConfig(constructorArgConfigPath)
    const noProxyValidationUserConfig = await readUnvalidatedChugSplashConfig(
      noProxyContractReferenceConfigPath
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

    process.stderr.write = (message: string) => {
      validationOutput += message
      return true
    }

    const cre = await createChugSplashRuntime(
      variableValidateConfigPath,
      false,
      true,
      hre.config.paths.canonicalConfigs,
      hre,
      false,
      process.stderr
    )

    await readValidatedChugSplashConfig(
      provider,
      variableValidateConfigPath,
      varValidationArtifactPaths,
      'hardhat',
      cre,
      false
    )

    await readValidatedChugSplashConfig(
      provider,
      constructorArgConfigPath,
      constructorArgsValidationArtifactPaths,
      'hardhat',
      cre,
      false
    )

    await assertValidUserConfigFields(
      noProxyValidationUserConfig,
      provider,
      cre,
      false
    )
  })

  it('did catch invalid variable arrayInt8', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for variable arrayInt8 expected number, string, or BigNumber but got array'
    )
  })

  it('did catch invalid variable int8OutsideRange', async () => {
    expect(validationOutput).to.have.string(
      'invalid value for int8OutsideRange: 255, outside valid range: [-128:127]'
    )
  })

  it('did catch invalid variable uint8OutsideRange', async () => {
    expect(validationOutput).to.have.string(
      'invalid value for uint8OutsideRange: 256, outside valid range: [0:255]'
    )
  })

  it('did catch invalid variable intAddress', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for intAddress: 1, expected address string but got number'
    )
  })

  it('did catch invalid variable arrayAddress', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for arrayAddress: 0x00000000, expected address string but got array'
    )
  })

  it('did catch invalid variable shortAddress', async () => {
    expect(validationOutput).to.have.string(
      'invalid address for shortAddress: 0x00000000'
    )
  })

  it('did catch invalid variable intBytes32', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for intBytes32: 1, expected DataHexString but got number'
    )
  })

  it('did catch invalid variable arrayBytes32', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for arrayBytes32: 1, expected DataHexString but got array'
    )
  })

  it('did catch invalid variable shortBytes32', async () => {
    expect(validationOutput).to.have.string(
      'invalid length for bytes32 variable shortBytes32: 0x00000000'
    )
  })

  it('did catch invalid variable longBytes8', async () => {
    expect(validationOutput).to.have.string(
      'invalid length for bytes8 variable longBytes8: 0x1111111111111111111111111111111111111111111111111111111111111111'
    )
  })

  it('did catch invalid variable malformedBytes16', async () => {
    expect(validationOutput).to.have.string(
      'invalid input format for variable malformedBytes16, expected DataHexString but got 11111111111111111111111111111111'
    )
  })

  it('did catch invalid variable intBoolean', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for variable intBoolean, expected boolean but got number'
    )
  })

  it('did catch invalid variable stringBoolean', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for variable stringBoolean, expected boolean but got string'
    )
  })

  it('did catch invalid variable arrayBoolean', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for variable arrayBoolean, expected boolean but got array'
    )
  })

  it('did catch odd fixed bytes variable', async () => {
    expect(validationOutput).to.have.string(
      'invalid input format for variable oddStaticBytes, expected DataHexString but got'
    )
  })

  it('did catch invalid constructor arg _arrayInt8', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for variable _arrayInt8 expected number, string, or BigNumber but got array'
    )
  })

  it('did catch invalid constructor arg _int8OutsideRange', async () => {
    expect(validationOutput).to.have.string(
      'invalid value for _int8OutsideRange: 255, outside valid range: [-128:127]'
    )
  })

  it('did catch invalid constructor arg _uint8OutsideRange', async () => {
    expect(validationOutput).to.have.string(
      'invalid value for _uint8OutsideRange: 256, outside valid range: [0:255]'
    )
  })

  it('did catch invalid constructor arg _intAddress', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for _intAddress: 1, expected address string but got number'
    )
  })

  it('did catch invalid constructor arg _arrayAddress', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for _arrayAddress: 0x00000000, expected address string but got array'
    )
  })

  it('did catch invalid constructor arg _shortAddress', async () => {
    expect(validationOutput).to.have.string(
      'invalid address for _shortAddress: 0x00000000'
    )
  })

  it('did catch invalid constructor arg _intBytes32', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for _intBytes32: 1, expected DataHexString but got number'
    )
  })

  it('did catch invalid constructor arg _arrayBytes32', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for _arrayBytes32: 1, expected DataHexString but got array'
    )
  })

  it('did catch invalid constructor arg _shortBytes32', async () => {
    expect(validationOutput).to.have.string(
      'invalid length for bytes32 variable _shortBytes32: 0x00000000'
    )
  })

  it('did catch invalid constructor arg _longBytes8', async () => {
    expect(validationOutput).to.have.string(
      'invalid length for bytes8 variable _longBytes8: 0x1111111111111111111111111111111111111111111111111111111111111111'
    )
  })

  it('did catch invalid constructor arg _malformedBytes16', async () => {
    expect(validationOutput).to.have.string(
      'invalid input format for variable _malformedBytes16, expected DataHexString but got 11111111111111111111111111111111'
    )
  })

  it('did catch invalid constructor arg _intBoolean', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for variable _intBoolean, expected boolean but got number'
    )
  })

  it('did catch invalid constructor arg _stringBoolean', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for variable _stringBoolean, expected boolean but got string'
    )
  })

  it('did catch invalid constructor arg _arrayBoolean', async () => {
    expect(validationOutput).to.have.string(
      'invalid input type for variable _arrayBoolean, expected boolean but got array'
    )
  })

  it('did catch odd fixed bytes constructor arg', async () => {
    expect(validationOutput).to.have.string(
      'invalid input format for variable _oddStaticBytes, expected DataHexString but got'
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

  it('did catch invalid reference to no-proxy contract in constructor arguments of no-proxy contract', async () => {
    expect(validationOutput).to.have.string(
      `Invalid contract reference: {{ Stateless }}. Contract references to no-proxy contracts are not allowed in other no-proxy contracts.`
    )
  })
})
