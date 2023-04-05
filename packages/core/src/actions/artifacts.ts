import { ethers } from 'ethers'
import ora from 'ora'

import { ParsedChugSplashConfig } from '../config/types'
import {
  ArtifactPaths,
  SolidityStorageLayout,
} from '../languages/solidity/types'
import { Integration } from '../constants'
import {
  createDeploymentFolderForNetwork,
  getConstructorArgs,
  readBuildInfo,
  readContractArtifact,
  writeDeploymentArtifact,
} from '../utils'

import 'core-js/features/array/at'

/**
 * Reads the storageLayout portion of the compiler artifact for a given contract. Reads the
 * artifact from the local file system.
 *
 * @param contractFullyQualifiedName Fully qualified name of the contract.
 * @param artifactFolder Relative path to the folder where artifacts are stored.
 * @return Storage layout object from the compiler output.
 */
export const readStorageLayout = (
  buildInfoPath: string,
  contractFullyQualifiedName: string
): SolidityStorageLayout => {
  const buildInfo = readBuildInfo(buildInfoPath)
  const [sourceName, contractName] = contractFullyQualifiedName.split(':')
  const contractOutput = buildInfo.output.contracts[sourceName][contractName]

  // Foundry artifacts do not contain the storage layout field for contracts which have no storage.
  // So we default to an empty storage layout in this case for consistency.
  return contractOutput.storageLayout ?? { storage: [], types: {} }
}

export const getDeployedBytecode = async (
  provider: ethers.providers.JsonRpcProvider,
  address: string
): Promise<string> => {
  const deployedBytecode = await provider.getCode(address)
  return deployedBytecode
}

export const createDeploymentArtifacts = async (
  provider: ethers.providers.JsonRpcProvider,
  parsedConfig: ParsedChugSplashConfig,
  finalDeploymentTxnHash: string,
  artifactPaths: ArtifactPaths,
  integration: Integration,
  spinner: ora.Ora,
  networkName: string,
  deploymentFolderPath: string
) => {
  spinner.start(`Writing deployment artifacts...`)

  createDeploymentFolderForNetwork(networkName, deploymentFolderPath)

  for (const [referenceName, contractConfig] of Object.entries(
    parsedConfig.contracts
  )) {
    const artifact = readContractArtifact(
      artifactPaths[referenceName].contractArtifactPath,
      integration
    )
    const { sourceName, contractName, bytecode, abi } = artifact

    const buildInfo = readBuildInfo(artifactPaths[referenceName].buildInfoPath)

    const { constructorArgValues } = getConstructorArgs(
      parsedConfig.contracts[referenceName].constructorArgs,
      referenceName,
      abi
    )

    const receipt = await provider.getTransactionReceipt(finalDeploymentTxnHash)

    const metadata =
      buildInfo.output.contracts[sourceName][contractName].metadata

    const { devdoc, userdoc } =
      typeof metadata === 'string'
        ? JSON.parse(metadata).output
        : metadata.output

    const deploymentArtifact = {
      contractName,
      address: contractConfig.proxy,
      abi,
      transactionHash: finalDeploymentTxnHash,
      solcInputHash: buildInfo.id,
      receipt: {
        ...receipt,
        gasUsed: receipt.gasUsed.toString(),
        cumulativeGasUsed: receipt.cumulativeGasUsed.toString(),
        // Exclude the `effectiveGasPrice` if it's undefined, which is the case on Optimism.
        ...(receipt.effectiveGasPrice && {
          effectiveGasPrice: receipt.effectiveGasPrice.toString(),
        }),
      },
      numDeployments: 1,
      metadata:
        typeof metadata === 'string' ? metadata : JSON.stringify(metadata),
      args: constructorArgValues,
      bytecode,
      deployedBytecode: await provider.getCode(contractConfig.proxy),
      devdoc,
      userdoc,
      storageLayout: readStorageLayout(
        artifactPaths[referenceName].buildInfoPath,
        contractConfig.contract
      ),
    }

    writeDeploymentArtifact(
      networkName,
      deploymentFolderPath,
      deploymentArtifact,
      referenceName
    )
  }

  spinner.succeed(`Wrote deployment artifacts.`)
}
