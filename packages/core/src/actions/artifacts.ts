import { ConstructorFragment, ethers } from 'ethers'

import { ConfigArtifacts, ParsedConfig } from '../config/types'
import {
  CompilerOutput,
  SolidityStorageLayout,
} from '../languages/solidity/types'
import {
  writeDeploymentFolderForNetwork,
  getFunctionArgValueArray,
  writeDeploymentArtifact,
  isExtendedDeployContractActionInput,
} from '../utils'
import 'core-js/features/array/at'
import { SphinxJsonRpcProvider } from '../provider'
import { SphinxActionType } from './types'

/**
 * Gets the storage layout for a contract.
 *
 * @param contractFullyQualifiedName Fully qualified name of the contract.
 * @param artifactFolder Relative path to the folder where artifacts are stored.
 * @return Storage layout object from the compiler output.
 */
export const getStorageLayout = (
  compilerOutput: CompilerOutput,
  sourceName: string,
  contractName: string
): SolidityStorageLayout => {
  const contractOutput = compilerOutput.contracts[sourceName][contractName]

  // Foundry artifacts do not contain the storage layout field for contracts which have no storage.
  // So we default to an empty storage layout in this case for consistency.
  return contractOutput.storageLayout ?? { storage: [], types: {} }
}

export const getDeployedBytecode = async (
  provider: SphinxJsonRpcProvider,
  address: string
): Promise<string> => {
  const deployedBytecode = await provider.getCode(address)
  return deployedBytecode
}

export const writeDeploymentArtifacts = async (
  provider: ethers.Provider,
  parsedConfig: ParsedConfig,
  deploymentEvents: ethers.EventLog[],
  networkDirName: string,
  deploymentFolderPath: string,
  configArtifacts: ConfigArtifacts
) => {
  writeDeploymentFolderForNetwork(networkDirName, deploymentFolderPath)

  for (const deploymentEvent of deploymentEvents) {
    if (!deploymentEvent.args) {
      throw new Error(`Deployment event has no arguments. Should never happen.`)
    }

    const receipt = await deploymentEvent.getTransactionReceipt()
    const { contractAddress } = deploymentEvent.args

    const action = parsedConfig.actionInputs.find(
      (a) =>
        isExtendedDeployContractActionInput(a) && a.create3Address === contractAddress
    )

    if (!action) {
      throw new Error(
        `Could not find action for contract address ${contractAddress}. Should never happen.`
      )
    }

    // TODO(upgrades)
    // if (parsedConfig.contracts[referenceName].kind === 'proxy') {
    //   // The deployment event is for a default proxy.
    //   const { metadata, storageLayout } =
    //     sphinxBuildInfo.output.contracts[
    //       '@eth-optimism/contracts-bedrock/contracts/universal/Proxy.sol'
    //     ]['Proxy']
    //   const { devdoc, userdoc } =
    //     typeof metadata === 'string'
    //       ? JSON.parse(metadata).output
    //       : metadata.output

    //   // Define the deployment artifact for the proxy.
    //   const proxyArtifact = {
    //     address: contractAddress,
    //     abi: ProxyABI,
    //     transactionHash: deploymentEvent.transactionHash,
    //     solcInputHash: sphinxBuildInfo.id,
    //     receipt: {
    //       ...receipt,
    //       gasUsed: receipt.gasUsed.toString(),
    //       cumulativeGasUsed: receipt.cumulativeGasUsed.toString(),
    //       // Exclude the `gasPrice` if it's undefined
    //       ...(receipt.gasPrice && {
    //         gasPrice: receipt.gasPrice.toString(),
    //       }),
    //     },
    //     numDeployments: 1,
    //     metadata:
    //       typeof metadata === 'string' ? metadata : JSON.stringify(metadata),
    //     args: [managerAddress],
    //     bytecode: ProxyArtifact.bytecode,
    //     deployedBytecode: await provider.getCode(contractAddress),
    //     devdoc,
    //     userdoc,
    //     storageLayout,
    //   }

    //   // Write the deployment artifact for the proxy contract.
    //   writeDeploymentArtifact(
    //     networkDirName,
    //     deploymentFolderPath,
    //     proxyArtifact,
    //     `${referenceName}Proxy`
    //   )
    // } else {

    const { artifact, buildInfo } = configArtifacts[action.fullyQualifiedName]
    const { sourceName, contractName, bytecode, abi } = artifact
    const iface = new ethers.Interface(abi)
    const constructorArgValues = getFunctionArgValueArray(
      action.decodedAction.variables,
      iface.fragments.find(ConstructorFragment.isFragment)
    )
    const { metadata } = buildInfo.output.contracts[sourceName][contractName]
    const storageLayout = getStorageLayout(
      buildInfo.output,
      sourceName,
      contractName
    )
    const { devdoc, userdoc } =
      typeof metadata === 'string'
        ? JSON.parse(metadata).output
        : metadata.output

    // Define the deployment artifact for the deployed contract.
    const contractArtifact = {
      address: contractAddress,
      abi,
      transactionHash: deploymentEvent.transactionHash,
      solcInputHash: buildInfo.id,
      receipt: {
        ...receipt,
        gasUsed: receipt.gasUsed.toString(),
        cumulativeGasUsed: receipt.cumulativeGasUsed.toString(),
        // Exclude the `gasPrice` if it's undefined
        ...(receipt.gasPrice && {
          gasPrice: receipt.gasPrice.toString(),
        }),
      },
      numDeployments: 1,
      metadata:
        typeof metadata === 'string' ? metadata : JSON.stringify(metadata),
      args: constructorArgValues,
      bytecode,
      deployedBytecode: await provider.getCode(contractAddress),
      devdoc,
      userdoc,
      storageLayout,
    }
    // Write the deployment artifact for the deployed contract.
    writeDeploymentArtifact(
      networkDirName,
      deploymentFolderPath,
      contractArtifact,
      action.referenceName
    )
  }
}

export const getStorageSlotKey = (
  fullyQualifiedName: string,
  compilerOutput: CompilerOutput,
  varName: string
): string => {
  const [sourceName, contractName] = fullyQualifiedName.split(':')
  const storageLayout = getStorageLayout(
    compilerOutput,
    sourceName,
    contractName
  )
  const storageObj = storageLayout.storage.find((s) => s.label === varName)

  if (!storageObj) {
    throw new Error(
      `Could not find storage slot key for: ${fullyQualifiedName}`
    )
  }

  return storageObj.slot
}
