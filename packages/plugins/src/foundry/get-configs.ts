import { resolve } from 'path'

import { defaultAbiCoder, hexConcat } from 'ethers/lib/utils'
import {
  getMinimalConfig,
  readUserChugSplashConfig,
} from '@chugsplash/core/dist/config/config'

import { getEncodedFailure, validationStderrWrite } from './logs'

const args = process.argv.slice(2)
const configPath = args[0]
const projectName = args[1]
const ownerAddress = args[2]

// This function is in its own file to minimize the number of dependencies that are imported, as
// this speeds up the execution time of the script when called via FFI from Foundry.
;(async () => {
  process.stderr.write = validationStderrWrite
  try {
    if (!projectName) {
      throw Error('No project name provided')
    }

    const userConfig = await readUserChugSplashConfig(configPath)

    const minimalConfig = getMinimalConfig(
      userConfig,
      projectName,
      ownerAddress
    )

    const rootImportPath =
      process.env.DEV_FILE_PATH ?? './node_modules/@chugsplash/plugins/'
    const utilsArtifactFolder = `${rootImportPath}out/artifacts`

    const ChugSplashUtilsABI =
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      require(resolve(
        `${utilsArtifactFolder}/ChugSplashUtils.sol/ChugSplashUtils.json`
      )).abi
    const minimalConfigType = ChugSplashUtilsABI.find(
      (fragment) => fragment.name === 'minimalConfig'
    ).outputs[0]

    const encodedConfigs = defaultAbiCoder.encode(
      [minimalConfigType, 'string'],
      [minimalConfig, JSON.stringify(userConfig)]
    )

    const encodedSuccess = hexConcat([
      encodedConfigs,
      defaultAbiCoder.encode(['bool'], [true]), // true = success
    ])

    process.stdout.write(encodedSuccess)
  } catch (err) {
    const encodedFailure = getEncodedFailure(err)
    process.stdout.write(encodedFailure)
  }
})()
