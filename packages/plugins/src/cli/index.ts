#!/usr/bin/env node

import { join } from 'path'
import { spawnSync } from 'child_process'

import * as dotenv from 'dotenv'
import yargs from 'yargs'
import { hideBin } from 'yargs/helpers'
import ora from 'ora'
import { execAsync } from '@sphinx-labs/core/dist/utils'
import { satisfies } from 'semver'
import { getSphinxManagerAddress } from '@sphinx-labs/core/dist/addresses'
import { Wallet, providers } from 'ethers/lib/ethers'
import {
  getDiff,
  getDiffString,
  getParsedConfig,
  userConfirmation,
  UserConfig,
  ensureSphinxInitialized,
  proposeAbstractTask,
  UserConfigWithOptions,
} from '@sphinx-labs/core'
import 'core-js/features/array/at'

import { writeSampleProjectFiles } from '../sample-project'
import {
  inferSolcVersion,
  makeGetConfigArtifacts,
  makeGetProviderFromChainId,
} from '../foundry/utils'
import { getFoundryConfigOptions } from '../foundry/options'
import { createSphinxRuntime } from '../cre'
import { writeDeploymentArtifactsUsingEvents } from '../foundry/artifacts'

// Load environment variables from .env
dotenv.config()

const configOption = 'config'
const rpcOption = 'rpc'
const projectOption = 'project'
const privateKeyOption = 'private-key'
const confirmOption = 'confirm'
const broadcastOption = 'broadcast'

const rootFfiPath =
  process.env.DEV_FILE_PATH ?? './node_modules/@sphinx-labs/plugins/'

yargs(hideBin(process.argv))
  .scriptName('sphinx')
  .command(
    'propose',
    `Propose the latest version of a config file. Signs a proposal meta transaction and relays it to Sphinx's back-end.`,
    (y) =>
      y
        .usage(
          `Usage: npx sphinx propose --${configOption} <path> [--testnets|--mainnets]`
        )
        .option(configOption, {
          alias: 'c',
          describe: 'Path to the Sphinx config file.',
          type: 'string',
        })
        .option('testnets', {
          describe: 'Propose on the testnets specified in the Sphinx config',
          boolean: true,
        })
        .option('mainnets', {
          describe: `Propose on the mainnets specified in the Sphinx config`,
          boolean: true,
        })
        .hide('version'),
    async (argv) => {
      const { config } = argv
      const testnets = argv.testnets ?? false
      const mainnets = argv.mainnets ?? false

      let isTestnet: boolean
      if (testnets && mainnets) {
        console.error('Cannot specify both --testnets and --mainnets')
        process.exit(1)
      } else if (testnets) {
        isTestnet = true
      } else if (mainnets) {
        isTestnet = false
      } else {
        console.error('Must specify either --testnets or --mainnets')
        process.exit(1)
      }

      if (!config) {
        console.error(
          `Must specify a path to a Sphinx config file via --${configOption}.`
        )
        process.exit(1)
      }

      // First, we compile the contracts to make sure we're using the latest versions. This command
      // displays the compilation process to the user in real time.
      const { status } = spawnSync(`forge`, ['build'], { stdio: 'inherit' })
      // Exit the process if compilation fails.
      if (status !== 0) {
        process.exit(1)
      }

      const spinner = ora()
      spinner.start(`Getting project info...`)

      const {
        artifactFolder,
        buildInfoFolder,
        cachePath,
        compilerConfigFolder,
        rpcEndpoints,
      } = await getFoundryConfigOptions()

      const cre = createSphinxRuntime(
        'hardhat',
        true,
        false,
        false, // Users must manually confirm proposals.
        compilerConfigFolder,
        undefined,
        false,
        process.stderr
      )

      // Get the user config by invoking a script with TS node. This is necessary to support
      // TypeScript configs because the current context is invoked with Node, not TS Node.
      const userConfigScriptPath = join(
        rootFfiPath,
        'dist',
        'foundry',
        'display-user-config.js'
      )
      let userConfig: UserConfigWithOptions
      try {
        // Using --swc speeds up the execution of the script.
        const { stdout } = await execAsync(
          `npx ts-node --swc ${userConfigScriptPath} ${config}`
        )
        userConfig = JSON.parse(stdout)
      } catch ({ stderr }) {
        spinner.stop()
        console.error(stderr)
        process.exit(1)
      }

      await proposeAbstractTask(
        userConfig,
        isTestnet,
        cre,
        makeGetConfigArtifacts(artifactFolder, buildInfoFolder, cachePath),
        await makeGetProviderFromChainId(rpcEndpoints),
        spinner
      )
    }
  )
  .command(
    'init',
    'Initialize a sample project',
    (y) =>
      y
        .usage('Usage: npx sphinx init --js|--ts [--quickstart]')
        .option('quickstart', {
          describe:
            'Initialize the project in a new repository. This writes a new foundry.toml and .env file.',
          boolean: true,
        })
        .option('js', {
          describe: 'Create a JavaScript Sphinx config file',
          boolean: true,
        })
        .option('ts', {
          describe: 'Create a TypeScript Sphinx config file',
          boolean: true,
        })
        .hide('version'),
    async (argv) => {
      const quickstart = argv.quickstart ?? false
      const { ts, js } = argv
      if (ts && js) {
        console.error('Cannot specify both --ts and --js. Please choose one.')
        process.exit(1)
      } else if (!ts && !js) {
        console.error(
          'Must specify either --ts (TypeScript) or --js (JavaScript).'
        )
        process.exit(1)
      }

      const { stdout } = await execAsync('node -v')
      if (!satisfies(stdout, '>=18.16.0')) {
        console.warn(
          '\x1b[33m%s\x1b[0m', // Yellow text
          `Your Node version is less than v18.16.0. We HIGHLY recommend using v18.16.0 or later because\n` +
            `it runs our Foundry plugin significantly faster. To update your Node version, go to:\n` +
            `https://github.com/nvm-sh/nvm#intro`
        )
      }

      const isTypeScriptProject = ts ? true : false

      const spinner = ora()

      const forgeConfigOutput = await execAsync('forge config --json')
      const forgeConfig = JSON.parse(forgeConfigOutput.stdout)
      const { src, test, solc } = forgeConfig

      const solcVersion = solc ?? (await inferSolcVersion())

      writeSampleProjectFiles(
        'sphinx',
        src,
        test,
        isTypeScriptProject,
        quickstart,
        solcVersion,
        'foundry'
      )
      spinner.succeed('Initialized Sphinx project.')
    }
  )
  .command(
    'artifacts',
    'Generate deployment artifacts for a Sphinx config file that was already deployed.',
    (y) =>
      y
        .usage(
          `Usage: npx sphinx artifacts --deployer <address> --${rpcOption} <rpc_url> --${projectOption} <name>`
        )
        .option('deployer', {
          describe: 'Address that deployed the project',
          type: 'string',
        })
        .option(rpcOption, {
          describe: 'RPC URL of the network where the deployment occurred',
          type: 'string',
        })
        .option(projectOption, {
          describe: 'Name of the project that was deployed',
          type: 'string',
        })
        .hide('version'),
    async (argv) => {
      const { deployer: owner, rpc, project } = argv
      if (!owner) {
        console.error('Must specify --deployer')
        process.exit(1)
      }
      if (!rpc) {
        console.error(`Must specify --${rpcOption}`)
        process.exit(1)
      }
      if (!project) {
        console.error(`Must specify --${projectOption}`)
        process.exit(1)
      }

      const spinner = ora()
      spinner.start(`Writing deployment artifacts...`)

      const { deploymentFolder, cachePath } = await getFoundryConfigOptions()

      const provider = new providers.JsonRpcProvider(rpc)

      const managerAddress = getSphinxManagerAddress(owner, project)

      await writeDeploymentArtifactsUsingEvents(
        provider,
        project,
        managerAddress,
        cachePath,
        deploymentFolder,
        spinner
      )
    }
  )
  .command(
    'deploy',
    `Deploy a Sphinx config file using Foundry. Writes deployment artifacts if broadcasting.`,
    (y) =>
      y
        .usage(
          `Usage: npx sphinx deploy --${configOption} <config_path> --${privateKeyOption} <key> [--${rpcOption} <url> --${confirmOption}]`
        )
        .option(configOption, {
          alias: 'c',
          describe: 'Path to the Sphinx config file.',
          type: 'string',
        })
        .option(privateKeyOption, {
          describe: 'Private key of the deployer.',
          type: 'string',
        })
        .option(rpcOption, {
          describe: 'RPC URL of the network to deploy on.',
          type: 'string',
        })
        .option(confirmOption, {
          describe: 'Automatically confirm the deployment.',
          boolean: true,
        })
        .option(broadcastOption, {
          describe: 'Broadcast the deployment to the network.',
          boolean: true,
        })
        .hide('version'),
    async (argv) => {
      const { config, confirm } = argv
      if (!config) {
        console.error(
          `Must specify a path to a Sphinx config file via --${configOption}.`
        )
        process.exit(1)
      }

      // The deploy command allows the user to pass in an RPC url, private key, and a boolean flag
      // for broadcasting. For these three commands, there are three valid combinations. Any other
      // combination of these flags is invalid, and an error will be thrown. The valid cases are:
      // 1. Broadcasting on a network: --rpc --private-key --broadcast
      // 2. Not broadcasting on a network: --rpc --private-key (no --broadcast)
      // 3. Not broadcasting on the in-process node, which doesn't use any of these flags.

      // Throw an error if the three flags are used in an invalid way.
      if (
        (argv[rpcOption] && !argv[privateKeyOption]) ||
        (!argv[rpcOption] && argv[privateKeyOption])
      ) {
        console.error(
          `Must specify both --${rpcOption} and --${privateKeyOption} or neither.`
        )
        process.exit(1)
      }
      if (
        argv[broadcastOption] &&
        !argv[rpcOption] &&
        !argv[privateKeyOption]
      ) {
        console.error(
          `Must specify --${rpcOption} and --${privateKeyOption} when broadcasting.`
        )
        process.exit(1)
      }

      const broadcast = argv[broadcastOption] ?? false
      const privateKey = argv[privateKeyOption] ?? ''
      const rpcUrl = argv[rpcOption] ?? 'http://127.0.0.1:8545'

      // First, we compile the contracts to make sure we're using the latest versions. This command
      // displays the compilation process to the user in real time.
      const { status } = spawnSync(`forge`, ['build'], { stdio: 'inherit' })
      // Exit the process if compilation fails.
      if (status !== 0) {
        process.exit(1)
      }

      const {
        artifactFolder,
        buildInfoFolder,
        compilerConfigFolder,
        cachePath,
        deploymentFolder,
      } = await getFoundryConfigOptions()

      const spinner = ora()

      const userConfigScriptPath = join(
        rootFfiPath,
        'dist',
        'foundry',
        'display-user-config.js'
      )

      const silent = false
      // Confirm with the user that they want to deploy the config. We skip this step if the user
      // has already confirmed or if they're deploying to the in-process Anvil node, which is
      // necessary because we can't access the in-process Anvil node from outside of Solidity.
      if (argv[rpcOption] && !confirm) {
        spinner.start('Getting project info...')

        const provider = new providers.JsonRpcProvider(rpcUrl)
        const owner = new Wallet(privateKey, provider)

        await ensureSphinxInitialized(provider, owner)

        // Get the user config by invoking a script with TS node. This is necessary to support
        // TypeScript configs because the current context is invoked with Node, not TS Node.
        let userConfig: UserConfig
        try {
          // Using --swc speeds up the execution of the script.
          const { stdout } = await execAsync(
            `npx ts-node --swc ${userConfigScriptPath} ${config}`
          )
          userConfig = JSON.parse(stdout)
        } catch ({ stderr }) {
          spinner.stop()
          console.error(stderr)
          process.exit(1)
        }

        const cre = createSphinxRuntime(
          'foundry',
          false,
          false,
          confirm,
          compilerConfigFolder,
          undefined,
          silent,
          process.stderr
        )

        const { configCache } = await getParsedConfig(
          userConfig,
          provider,
          cre,
          makeGetConfigArtifacts(artifactFolder, buildInfoFolder, cachePath),
          owner.address
        )

        const diff = getDiff([configCache])
        const diffString = getDiffString(diff)

        spinner.stop()
        await userConfirmation(diffString)
      }

      process.env['SPHINX_INTERNAL_CONFIG_PATH'] = config
      process.env['SPHINX_INTERNAL_RPC_URL'] = rpcUrl
      process.env['SPHINX_INTERNAL_BROADCAST'] = broadcast.toString()
      process.env['SPHINX_INTERNAL_PRIVATE_KEY'] = privateKey

      // The `SPHINX_INTERNAL_OVERRIDE_DEPLOY_SCRIPT` environment variable is used for testing
      // purposes only.
      const deployContractPath = process.env
        .SPHINX_INTERNAL_OVERRIDE_DEPLOY_SCRIPT
        ? join('script', 'Deploy.sol')
        : join(rootFfiPath, 'contracts', 'foundry', 'Deploy.sol')

      const forgeScriptArgs = ['script', deployContractPath]
      if (argv[rpcOption]) {
        forgeScriptArgs.push('--rpc-url', rpcUrl)
      }
      if (broadcast) {
        forgeScriptArgs.push('--broadcast')
      }

      // Run the deployment script.
      let isEmptyDeployment: boolean = false
      try {
        spinner.start(`Deploying project...`)
        const { stdout } = await execAsync(`forge ${forgeScriptArgs.join(' ')}`)

        if (
          stdout.includes(
            'Nothing to execute in this deployment. Exiting early.'
          )
        ) {
          isEmptyDeployment = true
        }

        spinner.stop()
        console.log(stdout)
      } catch ({ stderr }) {
        spinner.stop()
        // Strip \n from the end of the error message, if it exists
        const prettyError = stderr.endsWith('\n')
          ? stderr.substring(0, stderr.length - 1)
          : stderr

        console.error(prettyError)
        process.exit(1)
      }

      if (broadcast && !isEmptyDeployment) {
        spinner.start(`Writing deployment artifacts...`)
        const provider = new providers.JsonRpcProvider(rpcUrl)
        const owner = new Wallet(privateKey, provider)

        // Get the user config. Note that we use --swc because it speeds up the execution of the
        // script.
        const { stdout } = await execAsync(
          `npx ts-node --swc ${userConfigScriptPath} ${config}`
        )
        const userConfig: UserConfig = JSON.parse(stdout)

        await writeDeploymentArtifactsUsingEvents(
          provider,
          userConfig.projectName,
          owner.address,
          cachePath,
          deploymentFolder,
          spinner
        )
      }
    }
  )
  // Display the help menu when `npx sphinx` is called without any arguments.
  .showHelpOnFail(process.argv.length === 2 ? true : false)
  .demandCommand(
    1,
    'To get help for a specific task run: npx sphinx [task] --help'
  )
  .parse()
