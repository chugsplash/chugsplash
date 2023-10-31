import { join, sep } from 'path'
import { existsSync, mkdirSync, writeFileSync } from 'fs'

import { ConstructorFragment, FunctionFragment, ethers } from 'ethers'
import {
  BundledSphinxAction,
  ConfigArtifacts,
  ParsedConfig,
  getNetworkDirName,
  getNetworkNameForChainId,
  recursivelyConvertResult,
} from '@sphinx-labs/core'
import { SphinxManagerABI } from '@sphinx-labs/contracts'

import { FoundryBroadcast } from './types'

export const writeDeploymentArtifacts = async (
  provider: ethers.Provider,
  parsedConfig: ParsedConfig,
  bundledActions: Array<BundledSphinxAction>,
  broadcast: FoundryBroadcast,
  deploymentFolderPath: string,
  configArtifacts: ConfigArtifacts
): Promise<string> => {
  const managerInterface = new ethers.Interface(SphinxManagerABI)

  const executeActionsFragment = managerInterface.fragments
    .filter(FunctionFragment.isFragment)
    .find((f) => f.name === 'executeInitialActions')
  if (!executeActionsFragment) {
    throw new Error(
      `Could not find 'executeInitialActions' in the SphinxManager ABI. Should never happen.`
    )
  }

  const networkName = getNetworkNameForChainId(BigInt(parsedConfig.chainId))
  const networkDirName = getNetworkDirName(
    networkName,
    parsedConfig.isLiveNetwork,
    Number(parsedConfig.chainId)
  )

  const networkPath = join(deploymentFolderPath, networkDirName)
  if (!existsSync(networkPath)) {
    mkdirSync(networkPath, { recursive: true })
  }

  const numDeployments: { [contractName: string]: number | undefined } = {}
  for (const action of bundledActions) {
    for (const address of Object.keys(action.contracts)) {
      const { fullyQualifiedName, initCodeWithArgs } = action.contracts[address]
      const { artifact, buildInfo } = configArtifacts[fullyQualifiedName]
      const { bytecode, abi, metadata, contractName } = artifact

      const tx = broadcast.transactions.find((t) => {
        if (!t.transaction.to) {
          return false
        }

        const to = ethers.getAddress(t.transaction.to)
        if (
          to === parsedConfig.managerAddress &&
          t.transaction.data.startsWith(executeActionsFragment.selector)
        ) {
          const decodedResult = managerInterface.decodeFunctionData(
            executeActionsFragment,
            t.transaction.data
          )
          const { _actions } = recursivelyConvertResult(
            executeActionsFragment.inputs,
            decodedResult
          ) as any

          return _actions.some((a) => a.index === action.action.index)
        }
      })
      if (!tx) {
        throw new Error(
          `Could not find broadcasted transaction for ${fullyQualifiedName}. Should never happen.`
        )
      }

      const receipt = broadcast.receipts.find(
        (r) => r.transactionHash === tx.hash
      )
      if (!receipt) {
        throw new Error(
          `Could not find transaction receipt. Should never happen.`
        )
      }

      const iface = new ethers.Interface(abi)
      const coder = ethers.AbiCoder.defaultAbiCoder()

      // Get the ABI encoded constructor arguments. We use the length of the `artifact.bytecode` to
      // determine where the contract's creation code ends and the constructor arguments begin. This
      // method works even if the `artifact.bytecode` contains externally linked library placeholders
      // or immutable variable placeholders, which are always the same length as the real values.
      const encodedConstructorArgs = ethers.dataSlice(
        initCodeWithArgs,
        ethers.dataLength(bytecode)
      )

      const constructorFragment = iface.fragments.find(
        ConstructorFragment.isFragment
      )
      const constructorArgValues = constructorFragment
        ? coder.decode(constructorFragment.inputs, encodedConstructorArgs)
        : []
      const storageLayout = artifact.storageLayout ?? { storage: [], types: {} }
      const { devdoc, userdoc } =
        typeof metadata === 'string'
          ? JSON.parse(metadata).output
          : metadata.output

      // Define the deployment artifact for the deployed contract.
      const contractArtifact = {
        address,
        abi,
        transactionHash: receipt.transactionHash,
        solcInputHash: buildInfo.id,
        receipt: {
          ...receipt,
          gasUsed: receipt.gasUsed.toString(),
          cumulativeGasUsed: receipt.cumulativeGasUsed.toString(),
          // Exclude the `gasPrice` if it's undefined
          ...(receipt.effectiveGasPrice && {
            gasPrice: receipt.effectiveGasPrice.toString(),
          }),
        },
        numDeployments: 1,
        metadata:
          typeof metadata === 'string' ? metadata : JSON.stringify(metadata),
        args: constructorArgValues,
        bytecode,
        deployedBytecode: await provider.getCode(address),
        devdoc,
        userdoc,
        storageLayout,
      }

      const previousNumDeployments = numDeployments[contractName] ?? 0

      const fileName =
        previousNumDeployments > 0
          ? `${contractName}_${previousNumDeployments}.json`
          : `${contractName}.json`

      numDeployments[contractName] = previousNumDeployments + 1

      // Write the deployment artifact for the deployed contract.
      const artifactPath = join(deploymentFolderPath, networkDirName, fileName)
      writeFileSync(artifactPath, JSON.stringify(contractArtifact, null, '\t'))
    }
  }

  const deploymentArtifactsPath = join(
    deploymentFolderPath,
    networkDirName,
    sep
  )
  return deploymentArtifactsPath
}
