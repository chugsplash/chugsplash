import { join, resolve } from 'path'
import { readFileSync, existsSync, unlinkSync } from 'fs'
import { spawnSync } from 'child_process'

import {
  ProjectDeployment,
  ProposalRequest,
  ProposalRequestLeaf,
  RoleType,
  WEBSITE_URL,
  elementsEqual,
  execAsync,
  getAuthLeafSignerInfo,
  getDiff,
  getDiffString,
  getProjectBundleInfo,
  getProjectDeploymentForChain,
  hyperlink,
  makeAuthBundle,
  relayIPFSCommit,
  relayProposal,
  signAuthRootMetaTxn,
  userConfirmation,
} from '@sphinx-labs/core'
import ora from 'ora'
import { blue } from 'chalk'

import { decodeProposalOutput } from '../../foundry/decode'
import { getFoundryConfigOptions } from '../../foundry/options'

const pluginRootPath =
  process.env.DEV_FILE_PATH ?? './node_modules/@sphinx-labs/plugins/'

/**
 * @notice Calls the `sphinxProposeTask` Solidity function, then converts the output into a format
 * that can be sent to the back-end.
 *
 * @param dryRun If true, the proposal will not be relayed to the back-end.
 * @param targetContract The name of the contract within the script file. Necessary when there are
 * multiple contracts in the specified script.
 */
export const propose = async (
  confirm: boolean,
  isTestnet: boolean,
  dryRun: boolean,
  scriptPath: string,
  targetContract?: string
): Promise<{
  proposalRequest: ProposalRequest | undefined
  ipfsData: string[] | undefined
}> => {
  const apiKey = process.env.SPHINX_API_KEY
  if (!apiKey) {
    throw new Error("You must specify a 'SPHINX_API_KEY' environment variable.")
  }

  // We compile the contracts to make sure we're using the latest versions. This command
  // displays the compilation process to the user in real time.
  const { status } = spawnSync(`forge`, ['build'], { stdio: 'inherit' })
  // Exit the process if compilation fails.
  if (status !== 0) {
    process.exit(1)
  }

  const spinner = ora()
  spinner.start(`Running simulation...`)

  const sphinxArtifactDir = `${pluginRootPath}out/artifacts`
  const SphinxPluginTypesABI =
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    require(resolve(
      `${sphinxArtifactDir}/SphinxPluginTypes.sol/SphinxPluginTypes.json`
    )).abi

  const { cachePath } = await getFoundryConfigOptions()
  const proposalOutputPath = join(cachePath, 'sphinx-proposal-output.txt')

  // Delete the ProposalOutput file if it already exists. This isn't strictly necessary, since an
  // existing file would be overwritten automatically when we call `sphinxProposeTask`, but this
  // ensures that we don't accidentally display an outdated preview to the user.
  if (existsSync(proposalOutputPath)) {
    unlinkSync(proposalOutputPath)
  }

  const forgeScriptArgs = [
    'script',
    scriptPath,
    '--sig',
    "'sphinxProposeTask(bool,string)'",
    isTestnet,
    proposalOutputPath,
  ]
  if (targetContract) {
    forgeScriptArgs.push('--target-contract', targetContract)
  }

  try {
    await execAsync(`forge ${forgeScriptArgs.join(' ')}`)
  } catch (e) {
    spinner.stop()
    // The `stdout` contains the trace of the error.
    console.log(e.stdout)
    // The `stderr` contains the error message.
    console.log(e.stderr)
    process.exit(1)
  }

  spinner.succeed(`Finished simulation.`)
  spinner.start(`Parsing simulation results...`)

  const abiEncodedProposalOutput = readFileSync(proposalOutputPath, 'utf8')
  const { proposerAddress, metaTxnSignature, bundleInfoArray, authRoot } =
    decodeProposalOutput(abiEncodedProposalOutput, SphinxPluginTypesABI)

  const diff = getDiff(bundleInfoArray.map((b) => b.compilerConfig))
  if (confirm) {
    spinner.succeed(`Parsed simulation results.`)
  } else {
    const diffString = getDiffString(diff)
    spinner.stop()
    await userConfirmation(diffString)
  }

  spinner.start(`Running proposal...`)

  const shouldBeEqual = bundleInfoArray.map(({ compilerConfig }) => {
    return {
      newConfig: compilerConfig.newConfig,
      authAddress: compilerConfig.authAddress,
      managerAddress: compilerConfig.managerAddress,
    }
  })
  if (!elementsEqual(shouldBeEqual)) {
    throw new Error(
      `Detected different SphinxConfig values for different chains. This is currently unsupported.` +
        `Please use the same config on all chains.`
    )
  }
  // Since we know that the following fields are the same for each `compilerConfig`, we get their
  // values here.
  const { newConfig, authAddress, managerAddress } =
    bundleInfoArray[0].compilerConfig

  const projectDeployments: Array<ProjectDeployment> = []
  const compilerConfigs: {
    [ipfsHash: string]: string
  } = {}
  const gasEstimates: ProposalRequest['gasEstimates'] = []
  for (const bundleInfoOnChain of bundleInfoArray) {
    const { authLeafs, configUri, compilerConfig, actionBundle, targetBundle } =
      bundleInfoOnChain

    let estimatedGas = 0
    estimatedGas += actionBundle.actions
      .map((a) => Number(a.gas))
      .reduce((a, b) => a + b, 0)
    estimatedGas += targetBundle.targets.length * 200_000
    // Add a constant amount of gas to account for the cost of executing each auth leaf. For
    // context, it costs ~350k gas to execute a Setup leaf that adds a single proposer and manager,
    // using a single owner as the signer. It costs ~100k gas to execute a Proposal leaf.
    estimatedGas += authLeafs.length * 450_000
    gasEstimates.push({
      estimatedGas: estimatedGas.toString(),
      chainId: Number(compilerConfig.chainId),
    })

    const projectDeployment = getProjectDeploymentForChain(
      authLeafs,
      compilerConfig,
      configUri,
      actionBundle,
      targetBundle
    )
    if (projectDeployment) {
      projectDeployments.push(projectDeployment)
    }

    compilerConfigs[configUri] = JSON.stringify(compilerConfig, null, 2)
  }

  const emptyBundle = bundleInfoArray.every((b) => b.authLeafs.length === 0)
  if (emptyBundle) {
    spinner.succeed(
      `Skipping proposal because there is nothing to propose on any chain.`
    )
    return { proposalRequest: undefined, ipfsData: undefined }
  }

  const chainStatus = bundleInfoArray.map((b) => ({
    chainId: Number(b.compilerConfig.chainId),
    numLeaves: b.authLeafs.length,
  }))

  const proposalRequestLeafs: Array<ProposalRequestLeaf> = []
  for (const { compilerConfig, authLeafs } of bundleInfoArray) {
    for (const { leaf, leafFunctionName, proof } of authLeafs) {
      const { data, chainId, index, to } = leaf
      const { owners, threshold } = newConfig

      const proposers = compilerConfig.initialState.firstProposalOccurred
        ? compilerConfig.initialState.proposers
        : newConfig.proposers

      const { leafThreshold, roleType } = getAuthLeafSignerInfo(
        threshold,
        leafFunctionName
      )

      let signerAddresses: string[]
      if (roleType === RoleType.OWNER) {
        signerAddresses = owners
      } else if (roleType === RoleType.PROPOSER) {
        signerAddresses = proposers
      } else {
        throw new Error(`Invalid role type: ${roleType}. Should never happen.`)
      }

      const signers = signerAddresses.map((addr) => {
        const signature =
          addr === proposerAddress ? metaTxnSignature : undefined
        return {
          address: addr,
          signature,
          isProposer: proposers.includes(addr),
        }
      })

      proposalRequestLeafs.push({
        chainId: Number(chainId),
        index: Number(index),
        to,
        leafType: leafFunctionName,
        data,
        siblings: proof,
        threshold: Number(leafThreshold),
        signers,
      })
    }
  }

  const managerVersionString = `v${newConfig.version.major}.${newConfig.version.minor}.${newConfig.version.patch}`

  const proposalRequest: ProposalRequest = {
    apiKey,
    orgId: newConfig.orgId,
    isTestnet,
    chainIds: bundleInfoArray.map(({ compilerConfig }) =>
      Number(compilerConfig.chainId)
    ),
    deploymentName: newConfig.projectName,
    owners: newConfig.owners,
    threshold: Number(newConfig.threshold),
    canonicalConfig: '{}', // Deprecated field
    authAddress,
    managerAddress,
    managerVersion: managerVersionString,
    projectDeployments,
    gasEstimates,
    diff,
    tree: {
      root: authRoot,
      chainStatus,
      leaves: proposalRequestLeafs,
    },
  }

  const compilerConfigArray = Object.values(compilerConfigs)
  if (!dryRun) {
    const websiteLink = blue(hyperlink('website', WEBSITE_URL))
    await relayProposal(proposalRequest)
    await relayIPFSCommit(apiKey, newConfig.orgId, compilerConfigArray)
    spinner.succeed(
      `Proposal succeeded! Go to ${websiteLink} to approve the deployment.`
    )
  } else {
    spinner.succeed(`Proposal dry run succeeded!`)
  }
  return { proposalRequest, ipfsData: compilerConfigArray }
}