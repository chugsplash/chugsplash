import { yellow, green, blue, bold } from 'chalk'

import { ParsedConfig } from './config/types'
import {
  arraysEqual,
  getNetworkNameForChainId,
  getNetworkTag,
  prettyFunctionCall,
  prettyRawFunctionCall,
} from './utils'

type PreviewElement = DecodedAction | { to: string; data: string }

export type SphinxPreview = {
  networks: Array<{
    networkTags: Array<string>
    executing: Array<PreviewElement>
    skipping: Array<PreviewElement>
  }>
  unlabeledAddresses: Set<string>
}

export const isDecodedAction = (
  element: PreviewElement
): element is DecodedAction =>
  (element as DecodedAction).name !== undefined &&
  (element as DecodedAction).functionName !== undefined &&
  (element as DecodedAction).variables !== undefined

/**
 * @notice Returns a string that describes the changes that will be made to a set of contracts.
 */
export const getPreviewString = (
  preview: SphinxPreview,
  includeConfirmQuestion: boolean
): string => {
  let previewString = ''

  const skippingReason = `${yellow.bold(`Reason:`)} ${yellow(
    `Already executed.`
  )}`

  for (const { networkTags, executing, skipping } of preview.networks) {
    // Get the preview string for the networks.
    const networkTagsArray: Array<string> = []
    if (networkTags.length === 1) {
      networkTagsArray.push(`${bold(`Network:`)} ${networkTags[0]}`)
    } else {
      networkTagsArray.push(bold.underline(`Networks:`))
      const networks = networkTags.map((tag, i) => `${i + 1}. ${tag}`)
      networkTagsArray.push(...networks)
    }
    previewString += `${networkTagsArray.join('\n')}\n`

    // Get the preview string for the actions that will be executed.
    const executingArray: Array<string> = []
    if (executing.length === 0) {
      executingArray.push(green.underline.bold(`Nothing to execute.`))
    } else {
      executingArray.push(green.underline.bold(`Executing:`))
      for (let i = 0; i < executing.length; i++) {
        const element = executing[i]

        if (isDecodedAction(element)) {
          const { referenceName, functionName, variables, address } = element
          const actionStr = prettyFunctionCall(
            referenceName,
            address,
            functionName,
            variables,
            5,
            3
          )

          executingArray.push(green(`${i + 1}. ${actionStr}`))
        } else {
          const { to, data } = element
          const actionStr = prettyRawFunctionCall(to, data)
          executingArray.push(green(`${i + 1}. ${actionStr}`))
        }
      }
    }
    previewString += `${executingArray.join('\n')}\n`

    // Get the preview string for the actions that will be skipped.
    if (skipping.length > 0) {
      const skippingArray: Array<string> = []
      skippingArray.push(yellow.underline.bold(`Skipping:`))
      skippingArray.push(skippingReason)
      for (let i = 0; i < skipping.length; i++) {
        const element = skipping[i]
        const functionCallStr = isDecodedAction(element)
          ? prettyFunctionCall(
              element.referenceName,
              element.address,
              element.functionName,
              element.variables,
              5,
              3
            )
          : prettyRawFunctionCall(element.to, element.data)

        const skippingStr = yellow(`${i + 1}. ${functionCallStr}`)
        skippingArray.push(skippingStr)
      }
      previewString += `${skippingArray.join('\n')}\n`
    }

    previewString += '\n'
  }

  // Warn about unlabeled addresses
  if (preview.unlabeledAddresses.size > 0) {
    const troubleshootingGuideLink = blue.underline(
      `https://github.com/sphinx-labs/sphinx/blob/main/docs/troubleshooting-guide.md#labeling-contracts\n\n`
    )
    previewString += `${yellow.bold(
      `Warning: Sphinx can't infer the contracts that correspond to the following addresses:\n`
    )}`
    previewString += `${Array.from(preview.unlabeledAddresses)
      .map((e) => yellow(`- ${e}`))
      .join('\n')}\n`
    previewString +=
      yellow(
        `If you'd like Sphinx to verify any of these contracts on Etherscan or create their deployment artifacts,\n` +
          `please label them in your script. See the troubleshooting guide for more information:\n`
      ) + troubleshootingGuideLink
  }

  if (includeConfirmQuestion) {
    previewString += `Confirm? [y/n]`
  }
  return previewString
}

export const getPreview = (
  parsedConfigs: Array<ParsedConfig>
): SphinxPreview => {
  const networks: {
    [networkTag: string]: {
      executing: Array<PreviewElement>
      skipping: Array<PreviewElement>
      unlabeledAddresses: Array<string>
    }
  } = {}

  for (const parsedConfig of parsedConfigs) {
    const executing: Array<PreviewElement> = []
    const skipping: Array<PreviewElement> = []

    const {
      chainId,
      initialState,
      actions,
      isLiveNetwork,
      unlabeledAddresses,
    } = parsedConfig

    const networkName = getNetworkNameForChainId(BigInt(chainId))
    const networkTag = getNetworkTag(
      networkName,
      isLiveNetwork,
      BigInt(chainId)
    )

    // If there aren't any transactions to execute on the current network, we set the current
    // network's preview to be empty. This applies even if the Gnosis Safe and Sphinx Module haven't
    // been deployed yet because we don't currently allow the user to deploy the Safe and Module
    // without executing a deployment.
    if (actions.length > 0) {
      if (!initialState.isSafeDeployed) {
        executing.push({
          referenceName: 'GnosisSafe',
          functionName: 'deploy',
          variables: [],
          address: parsedConfig.safeAddress,
        })
      }
      if (!initialState.isModuleDeployed) {
        executing.push({
          referenceName: 'SphinxModule',
          functionName: 'deploy',
          variables: [],
          address: parsedConfig.moduleAddress,
        })
      }

      for (const action of actions) {
        const { decodedAction } = action
        executing.push(decodedAction)
      }
    }

    networks[networkTag] = { executing, skipping, unlabeledAddresses }
  }

  // Next, we group networks that have the same executing and skipping arrays.
  const preview: SphinxPreview = {
    networks: [],
    unlabeledAddresses: new Set([]),
  }
  for (const [
    networkTag,
    { executing, skipping, unlabeledAddresses },
  ] of Object.entries(networks)) {
    const existingNetwork = preview.networks.find(
      (e) =>
        arraysEqual(e.executing, executing) && arraysEqual(e.skipping, skipping)
    )

    for (const address of unlabeledAddresses) {
      preview.unlabeledAddresses.add(address)
    }

    if (existingNetwork) {
      existingNetwork.networkTags.push(networkTag)
    } else {
      preview.networks.push({
        networkTags: [networkTag],
        executing,
        skipping,
      })
    }
  }

  return preview
}
