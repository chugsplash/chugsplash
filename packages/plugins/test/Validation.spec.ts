import * as path from 'path'

// Hardhat plugins
import '@nomiclabs/hardhat-ethers'
import '@openzeppelin/hardhat-upgrades'
import '../dist'

import { expect } from 'chai'
import hre from 'hardhat'
import {
  readParsedChugSplashConfig,
  readUserChugSplashConfig,
} from '@chugsplash/core'

import { getArtifactPaths } from '../src/hardhat/artifacts'
const variableValidateConfigPath = './chugsplash/VariableValidation.config.ts'
const constructorArgConfigPath =
  './chugsplash/ConstructorArgValidation.config.ts'

describe('Validate', () => {
  let variableErr: Error
  let constructorArgErr: Error
  before(async () => {
    const provider = hre.ethers.provider
    const varValidationUserConfig = await readUserChugSplashConfig(
      variableValidateConfigPath
    )
    const constructorArgsValidationUserConfig = await readUserChugSplashConfig(
      constructorArgConfigPath
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

    try {
      await readParsedChugSplashConfig(
        provider,
        variableValidateConfigPath,
        varValidationArtifactPaths,
        'hardhat'
      )
    } catch (error) {
      expect(error).to.be.an('Error')
      variableErr = error
    }

    try {
      await readParsedChugSplashConfig(
        provider,
        constructorArgConfigPath,
        constructorArgsValidationArtifactPaths,
        'hardhat'
      )
    } catch (error) {
      expect(error).to.be.an('Error')
      constructorArgErr = error
    }
  })

  it('did catch invalid arrayInt8', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid input type for variable arrayInt8 expected number, string, or BigNumber but got array'
    )
  })

  it('did catch invalid int8OutsideRange', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid value for int8OutsideRange: 255, outside valid range: [-128:127]'
    )
  })

  it('did catch invalid uint8OutsideRange', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid value for uint8OutsideRange: 256, outside valid range: [0:255]'
    )
  })

  it('did catch invalid intAddress', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid input type for intAddress: 1, expected address string but got number'
    )
  })

  it('did catch invalid arrayAddress', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid input type for arrayAddress: 0x00000000, expected address string but got array'
    )
  })

  it('did catch invalid shortAddress', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid address for shortAddress: 0x00000000'
    )
  })

  it('did catch invalid intBytes32', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid input type for intBytes32: 1, expected DataHexString but got number'
    )
  })

  it('did catch invalid arrayBytes32', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid input type for arrayBytes32: 1, expected DataHexString but got array'
    )
  })

  it('did catch invalid shortBytes32', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid length for bytes32 variable shortBytes32: 0x00000000'
    )
  })

  it('did catch invalid longBytes8', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid length for bytes8 variable longBytes8: 0x1111111111111111111111111111111111111111111111111111111111111111'
    )
  })

  it('did catch invalid malformedBytes16', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid input format for variable malformedBytes16, expected DataHexString but got 11111111111111111111111111111111'
    )
  })

  it('did catch invalid intBoolean', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid input type for variable intBoolean, expected boolean but got number'
    )
  })

  it('did catch invalid stringBoolean', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid input type for variable stringBoolean, expected boolean but got string'
    )
  })

  it('did catch invalid arrayBoolean', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid input type for variable arrayBoolean, expected boolean but got array'
    )
  })

  it('did catch invalid oversizedArray', async () => {
    expect(variableErr.toString()).to.have.string(
      'Expected array of size 2 for oversizedArray but got [1,2,3]'
    )
  })

  it('did catch invalid oversizedNestedArray', async () => {
    expect(variableErr.toString()).to.have.string(
      'Expected array of size 2 for oversizedNestedArray but got [[1,2],[1,2],[1,2]]'
    )
  })

  it('did catch invalid invalidBoolArray', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid input type for variable invalidBoolArray, expected boolean but got string'
    )
  })

  it('did catch invalid invalidBytes32Array', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid input type for invalidBytes32Array: 1, expected DataHexString but got number'
    )
  })

  it('did catch invalid invalidAddressArray', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid address for invalidAddressArray: 0x00000000'
    )
  })

  it('did catch invalid invalidStringStringMapping', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid input type for invalidStringStringMapping, expected DataHexString but got number'
    )
  })

  it('did catch invalid invalidStringIntMapping', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid input type for variable invalidStringIntMapping expected number, string, or BigNumber but got boolean'
    )
  })

  it('did catch invalid invalidNestedStringIntBoolMapping', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid input type for variable invalidNestedStringIntBoolMapping expected number, string, or BigNumber but got boolean'
    )
  })

  it('did catch missing variables', async () => {
    expect(variableErr.toString()).to.have.string(
      'were not defined in the ChugSplash config file'
    )
    expect(variableErr.toString()).to.have.string('notSetUint')
    expect(variableErr.toString()).to.have.string('notSetString')
  })

  it('did catch extra variables', async () => {
    expect(variableErr.toString()).to.have.string(
      'following variables were defined in the ChugSplash config file but do not exist in the contract'
    )
    expect(variableErr.toString()).to.have.string('extraVar')
    expect(variableErr.toString()).to.have.string('anotherExtraVar')
  })

  it('did catch odd dynamic bytes', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid input type for variable oddDynamicBytes, expected DataHexString but got'
    )
  })

  it('did catch odd fixed bytes', async () => {
    expect(variableErr.toString()).to.have.string(
      'invalid input format for variable oddStaticBytes, expected DataHexString but got'
    )
  })

  it('did catch extra constructor argument', async () => {
    expect(constructorArgErr.toString()).to.have.string(
      'but are not present in the contract constructor'
    )
    expect(constructorArgErr.toString()).to.have.string('_immutableUint')
  })

  it('did catch missing constructor argument', async () => {
    expect(constructorArgErr.toString()).to.have.string(
      'but were not found in your config'
    )
    expect(constructorArgErr.toString()).to.have.string('_immutableBytes')
  })
})
