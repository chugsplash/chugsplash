import { basename, join, resolve } from 'path'
import { existsSync, readFileSync, unlinkSync } from 'fs'

// TODO: if you stick with Label.fqn, it'd be nice if you could validate that it's a well-formed
// fqn. i.e. at least check for a semicolon.

// TODO: support adding labels within the user's script (instead of requiring that it's in the setup
// function). this way, they don't need to hard-code the addresses.

// TODO: add a helper function for labels: sphinxLabel(address(myContract), ‘path/to/contract.sol:Contract’)

// TODO: can you remove `sphinx generate` from the propose and deploy tasks? if so, c/f `sphinx
// generate` in the repo to see if there's anywhere else you can remove it.

// TODO: handle a label that has an empty string instead of an artifact path. (we shouldn't error).

import {
  isRawCreate2ActionInput,
  isRawCreateActionInput,
  isRawDeployContractActionInput,
  remove0x,
  spawnAsync,
} from '@sphinx-labs/core/dist/utils'
import { SphinxJsonRpcProvider } from '@sphinx-labs/core/dist/provider'
import {
  getPreview,
  getPreviewString,
  userConfirmation,
  getEtherscanEndpointForNetwork,
  SUPPORTED_NETWORKS,
  ParsedConfig,
  SphinxPreview,
} from '@sphinx-labs/core'
import { red } from 'chalk'
import ora from 'ora'
import { ethers } from 'ethers'

import { getBundleInfoArray, makeGetConfigArtifacts } from '../foundry/utils'
import { getFoundryConfigOptions } from '../foundry/options'
import {
  getCollectedSingleChainDeployment,
  makeParsedConfig,
} from '../foundry/decode'
import { generateClient } from './typegen/client'
import { FoundryBroadcastReceipt } from '../foundry/types'
import { writeDeploymentArtifacts } from '../foundry/artifacts'

export const deploy = async (
  scriptPath: string,
  network: string,
  skipPreview: boolean,
  silent: boolean,
  targetContract?: string,
  verify?: boolean,
  prompt: (q: string) => Promise<void> = userConfirmation
): Promise<{
  parsedConfig: ParsedConfig
  preview?: ReturnType<typeof getPreview>
}> => {
  // First, we run the `sphinx generate` command to make sure that the user's contracts and clients
  // are up-to-date. The Solidity compiler is run within this command via `forge build`.

  // Generate the clients to make sure that the user's contracts and clients are up-to-date. We skip
  // the last compilation step in the generate command to reduce the number of times in a row that
  // we compile the contracts. If we didn't do this, then it'd be possible for the user to see
  // "Compiling..." three times in a row when they run the deploy command with the preview skipped.
  // This isn't a big deal, but it may be puzzling to the user.
  await generateClient(silent, true)

  const {
    artifactFolder,
    buildInfoFolder,
    cachePath,
    rpcEndpoints,
    deploymentFolder,
    etherscan,
    broadcastFolder,
  } = await getFoundryConfigOptions()

  const chainId = SUPPORTED_NETWORKS[network]
  if (chainId === undefined) {
    throw new Error(
      `Network name ${network} is not supported. You must use a supported network: \n${Object.keys(
        SUPPORTED_NETWORKS
      ).join('\n')}`
    )
  }

  // If the verification flag is specified, then make sure there is an etherscan configuration for the target network
  if (verify) {
    if (!etherscan || !etherscan[network]) {
      const endpoint = getEtherscanEndpointForNetwork(chainId)
      console.error(
        red(
          `No etherscan configuration detected for ${network}. Please configure it in your foundry.toml file:\n` +
            `[etherscan]\n` +
            `${network} = { key = "<your api key>", url = "${endpoint.urls.apiURL}", chain = ${SUPPORTED_NETWORKS[network]} }`
        )
      )
      process.exit(1)
    }
  }

  // We must load any ABIs after running `forge build` to prevent a situation where the user clears
  // their artifacts then calls this task, in which case the artifact won't exist yet.
  const sphinxPluginTypesABI =
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    require(resolve(
      `${artifactFolder}/SphinxPluginTypes.sol/SphinxPluginTypes.json`
    )).abi

  const getConfigArtifacts = makeGetConfigArtifacts(
    artifactFolder,
    buildInfoFolder,
    cachePath
  )

  const forkUrl = rpcEndpoints[network]
  if (!forkUrl) {
    console.error(
      red(
        `No RPC endpoint specified in your foundry.toml for the network: ${network}.`
      )
    )
    process.exit(1)
  }

  const spinner = ora({ isSilent: silent })
  spinner.start(`Collecting transactions...`)

  // TODO: force recompile when deploying on a live network and when proposing. for the former,
  // you may need to do an early `isLiveNetwork` check b/c e.g. `--network optimism` may actually
  // be an anvil node.

  // TODO: We need to remove --skip-simulation everywhere that we collect txns. you'll need to
  // account for the note above '--skip-simulation' in the next call.

  // TODO(propose): put --silent in proposal args too

  const deploymentInfoPath = join(cachePath, 'deployment-info.txt')

  // Remove the existing DeploymentInfo file if it exists. This ensures that we don't accidentally
  // use an outdated file.
  if (existsSync(deploymentInfoPath)) {
    unlinkSync(deploymentInfoPath)
  }

  const forgeScriptCollectArgs = [
    'script',
    scriptPath,
    '--sig',
    'sphinxCollectDeployment(string,string)',
    network,
    deploymentInfoPath,
    '--rpc-url',
    forkUrl,
  ]
  if (targetContract) {
    forgeScriptCollectArgs.push('--target-contract', targetContract)
  }

  const spawnOutput = await spawnAsync('forge', forgeScriptCollectArgs)

  if (spawnOutput.code !== 0) {
    spinner.stop()
    // The `stdout` contains the trace of the error.
    console.log(spawnOutput.stdout)
    // The `stderr` contains the error message.
    console.log(spawnOutput.stderr)
    process.exit(1)
  }

  const { deploymentInfo, actionInputs } = getCollectedSingleChainDeployment(
    network,
    scriptPath,
    broadcastFolder,
    sphinxPluginTypesABI,
    'sphinxCollectDeployment',
    deploymentInfoPath
  )

  const contractNamesSet = new Set<string>()
  const fullyQualifiedNamesSet = new Set<string>()
  for (const rawInput of actionInputs) {
    if (isRawDeployContractActionInput(rawInput)) {
      fullyQualifiedNamesSet.add(rawInput.fullyQualifiedName)
    } else if (typeof rawInput.contractName === 'string') {
      rawInput.contractName.includes(':')
        ? fullyQualifiedNamesSet.add(rawInput.contractName)
        : contractNamesSet.add(rawInput.contractName)
    }
  }

  for (const label of deploymentInfo.labels) {
    fullyQualifiedNamesSet.add(label.fullyQualifiedName)
  }

  const configArtifacts = await getConfigArtifacts(
    Array.from(fullyQualifiedNamesSet),
    Array.from(contractNamesSet)
  )
  const parsedConfig = makeParsedConfig(
    deploymentInfo,
    actionInputs,
    configArtifacts,
    // On Anvil nodes, we must set `remoteExecution` to `true` because we use the remote execution
    // flow in this case (e.g. we call `manager.claimDeployment` in Solidity).
    !deploymentInfo.isLiveNetwork,
    spinner
  )

  spinner.succeed(`Collected transactions.`)

  let preview: SphinxPreview | undefined
  if (skipPreview) {
    spinner.info(`Skipping preview.`)
  } else {
    preview = getPreview([parsedConfig])

    const emptyDeployment = parsedConfig.actionInputs.every(
      (action) => action.skip
    )

    spinner.stop()
    if (emptyDeployment) {
      if (!silent) {
        spinner.info(`Nothing to deploy exiting early.`)
        const previewString = getPreviewString(preview, false)
        console.log(previewString)
      }
      return { parsedConfig, preview }
    } else {
      const previewString = getPreviewString(preview, true)
      await prompt(previewString)
    }
  }

  const { authRoot, bundleInfoArray } = await getBundleInfoArray(
    configArtifacts,
    [parsedConfig]
  )
  if (bundleInfoArray.length !== 1) {
    throw new Error(
      `Bundle info array has incorrect length. Should never happen`
    )
  }
  const bundleInfo = bundleInfoArray[0]

  const sphinxABI =
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    require(resolve(`${artifactFolder}/Sphinx.sol/Sphinx.json`)).abi
  const sphinxIface = new ethers.Interface(sphinxABI)
  const deployTaskFragment = sphinxIface.fragments
    .filter(ethers.Fragment.isFunction)
    .find((fragment) => fragment.name === 'sphinxDeployTask')
  if (!deployTaskFragment) {
    throw new Error(`'sphinxDeployTask' not found in ABI. Should never happen.`)
  }

  const deployTaskData = sphinxIface.encodeFunctionData(deployTaskFragment, [
    network,
    authRoot,
    bundleInfo,
  ])

  const forgeScriptDeployArgs = [
    'script',
    scriptPath,
    '--sig',
    deployTaskData,
    '--fork-url',
    forkUrl,
    '--broadcast',
  ]
  if (verify) {
    forgeScriptDeployArgs.push('--verify')
  }
  if (targetContract) {
    forgeScriptDeployArgs.push('--target-contract', targetContract)
  }

  spinner.start(`Deploying...`)

  const { code, stdout, stderr } = await spawnAsync(
    'forge',
    forgeScriptDeployArgs
  )

  spinner.stop()
  if (code !== 0) {
    // The `stdout` contains the trace of the error.
    console.log(stdout)
    // The `stderr` contains the error message.
    console.log(stderr)
    process.exit(1)
  } else if (!silent) {
    console.log(stdout)
  }

  spinner.start(`Writing deployment artifacts...`)

  const containsDeployment = actionInputs.some((a) => {
    const isDeployment =
      isRawDeployContractActionInput(a) ||
      isRawCreate2ActionInput(a) ||
      isRawCreateActionInput(a)
    return isDeployment && !a.skip
  })

  if (containsDeployment) {
    const broadcastFilePath = join(
      broadcastFolder,
      basename(scriptPath),
      chainId.toString(),
      `${remove0x(deployTaskFragment.selector)}-latest.json`
    )

    const receipts: Array<FoundryBroadcastReceipt> = JSON.parse(
      readFileSync(broadcastFilePath, 'utf-8')
    ).receipts

    const provider = new SphinxJsonRpcProvider(forkUrl)
    // TODO: write deployment artifacts for create2 and create3
    const deploymentArtifactPath = await writeDeploymentArtifacts(
      provider,
      parsedConfig,
      receipts,
      deploymentFolder,
      configArtifacts
    )
    spinner.succeed(`Wrote deployment artifacts to: ${deploymentArtifactPath}`)
  } else {
    spinner.succeed(`No deployment artifacts to write.`)
  }

  return { parsedConfig, preview }
}
