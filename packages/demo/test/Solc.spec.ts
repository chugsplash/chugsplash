import { join } from 'path'
import { appendFileSync, rmSync } from 'fs'

import { expect } from 'chai'
import {
  sampleContractFileName,
  sampleScriptFileName,
  sampleTestFileName,
} from '@sphinx-labs/plugins'
import { SemVer, coerce, gt, lte } from 'semver'
import { spawnAsync } from '@sphinx-labs/core'

import { deleteForgeProject } from './common'

const srcDir = 'src'
const scriptDir = 'script'
const testDir = 'test'

// This test suite checks that Sphinx's plugin contracts can compile with the Yul intermediate
// representation compiler setting (i.e. `viaIR`). We test every supported solc version because
// minor changes in solc can lead to "stack too deep" errors. See this post and cameel's response
// for context: https://github.com/ethereum/solidity/issues/14082
describe('Solidity Compiler', () => {
  let contractPath: string
  let scriptPath: string
  let testPath: string
  let latestSolcVersion: SemVer
  before(async () => {
    contractPath = join(srcDir, sampleContractFileName)
    scriptPath = join(scriptDir, sampleScriptFileName)
    testPath = join(testDir, sampleTestFileName)

    deleteForgeProject(contractPath, scriptPath, testPath)

    const { code } = await spawnAsync(
      `npx`,
      [
        'sphinx',
        'init',
        '--org-id',
        'TEST_ORG_ID',
        '--sphinx-api-key',
        'TEST_SPHINX_KEY',
        '--alchemy-api-key',
        'TEST_ALCHEMY_KEY',
        '--owner',
        '0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266',
      ],
      {
        SPHINX_INTERNAL_TEST__SKIP_GIT: 'true',
      }
    )
    if (code !== 0) {
      throw new Error(`Failed to run 'sphinx init' command.`)
    }

    // Add a Foundry profile in the newly created `foundry.toml` that disables the Solidity compiler
    // optimizer. Foundry enables the optimizer by default and doesn't provide an option to disable
    // the optimizer using a CLI argument.
    const lines = '\n[profile.no_optimizer]\noptimizer = false'
    appendFileSync('foundry.toml', lines)

    const { stdout: latestSolcVersionRaw } = await spawnAsync(`npm`, [
      `show`,
      `solc`,
      `version`,
    ])

    const latestSolcVersionParsed = coerce(latestSolcVersionRaw)
    if (latestSolcVersionParsed === null) {
      throw new Error(`Could not find latest solc version.`)
    }
    if (
      latestSolcVersionParsed.major !== 0 ||
      latestSolcVersionParsed.minor !== 8
    ) {
      throw new Error(`Latest solc version is > 0.8.`)
    }
    latestSolcVersion = latestSolcVersionParsed
  })

  // TODO(later): undo
  // after(async () => {
  //   deleteForgeProject(contractPath, scriptPath, testPath)
  // })

  // Test that we can compile the Sphinx plugin contracts for solc versions ^0.8.1 using `viaIR` and
  // the optimizer. We don't test v0.8.0 because the sample Forge project can't compile with this
  // version and settings.
  it('Compiles with viaIR and optimizer enabled', async () => {
    const versions = generateSemverRange(
      new SemVer('0.8.11'),
      new SemVer('0.8.12')
    ) // TODO(later): undo

    // Generate output directory names. We use separate output directories for each compilation to
    // prevent race conditions.
    const outputDirs = versions.map((version) => `out-${version}`)
    // Generate separate cache directory names to prevent race conditions.
    const cacheDirs = versions.map((version) => `cache-${version}`)

    const buildPromises = versions.map((version, index) => {
      return spawnAsync(`forge`, [
        'build',
        '--use',
        version,
        '--via-ir',
        '--optimize',
        '--optimizer-runs',
        '200',
        '--force',
        '--out',
        outputDirs[index],
        '--cache-path',
        cacheDirs[index],
      ]).then(({ stdout, stderr, code }) => {
        return { version, stdout, stderr, code }
      })
    })

    const results = await Promise.all(buildPromises)

    const errorMessages: Array<string> = []
    results.forEach(({ version, stdout, stderr, code }) => {
      if (code !== 0) {
        if (stderr.includes('Unknown version provided')) {
          console.log(
            `Detected solc version not currently supported by foundry: ${version}`
          )
        } else {
          errorMessages.push(
            `Build failed for ${version} with optimizer enabled.\nSTDOUT: ${stdout}\nSTDERR: ${stderr}`
          )
        }
      }
    })

    // Delete the output directories
    outputDirs.forEach((dir) => rmSync(dir, { recursive: true, force: true }))

    if (errorMessages.length > 0) {
      console.error(errorMessages.join('\n\n'))
    }

    // If no errors, the test passes
    expect(errorMessages).to.be.empty
  })

  // Compile the Sphinx plugin contracts using `viaIR` with the optimizer disabled. We test for solc
  // versions greater than or equal to v0.8.21. Before v0.8.21, it was very easy to trigger a "stack
  // too deep" error when compiling via IR with the optimizer disabled. For example, the standard
  // project created by `forge init` can't compile with v0.8.20 using these settings. See these two
  // issues in the Solidity repo for more context:
  // The problem: https://github.com/ethereum/solidity/issues/12533
  // The fix in v0.8.21: https://github.com/ethereum/solidity/issues/13972
  it('Compiles with viaIR and the optimizer disabled', async () => {
    const versions = generateSemverRange(
      new SemVer('0.8.21'),
      latestSolcVersion
    )

    // Generate output directory names. We use separate output directories for each compilation to
    // prevent race conditions.
    const outputDirs = versions.map((version) => `out-${version}`)
    // Generate separate cache directory names to prevent race conditions.
    const cacheDirs = versions.map((version) => `cache-${version}`)

    const buildPromises = versions.map((version, index) => {
      return spawnAsync(
        `forge`,
        [
          'build',
          '--use',
          version,
          '--via-ir',
          '--force',
          '--out',
          outputDirs[index],
          '--cache-path',
          cacheDirs[index],
        ],
        {
          FOUNDRY_PROFILE: 'no_optimizer',
        }
      ).then(({ stdout, stderr, code }) => {
        return { version, stdout, stderr, code }
      })
    })

    const results = await Promise.all(buildPromises)

    const errorMessages: Array<string> = []
    results.forEach(({ version, stdout, stderr, code }) => {
      if (code !== 0) {
        if (stderr.includes('Unknown version provided')) {
          console.log(
            `Detected solc version not currently supported by foundry: ${version}`
          )
        } else {
          errorMessages.push(
            `Build failed for ${version} with optimizer enabled.\nSTDOUT: ${stdout}\nSTDERR: ${stderr}`
          )
        }
      }
    })

    // Delete the output directories
    outputDirs.forEach((dir) => rmSync(dir, { recursive: true, force: true }))

    if (errorMessages.length > 0) {
      console.error(errorMessages.join('\n\n'))
    }

    // If no errors, the test passes
    expect(errorMessages).to.be.empty

    if (errorMessages.length > 0) {
      for (const error of errorMessages) {
        console.error(error)
      }
    }
  })
})

const generateSemverRange = (
  startVersion: SemVer,
  endVersion: SemVer
): Array<string> => {
  let currentVersion = new SemVer(startVersion)
  const versions: Array<string> = []

  while (lte(currentVersion.version, endVersion)) {
    versions.push(currentVersion.version)

    // Increment the patch version
    currentVersion = new SemVer(currentVersion.inc('patch').version)

    // Stop if the current version exceeds the end version
    if (gt(currentVersion.version, endVersion.version)) {
      break
    }
  }

  // We intentionally filter out `0.8.24` since foundry does not appear to support it in the latest version.
  // See CHU-436 for more information.
  return versions.filter((version) => version !== '0.8.24')
}
