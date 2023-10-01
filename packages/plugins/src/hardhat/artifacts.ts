// import path from 'path'

// import { HardhatRuntimeEnvironment } from 'hardhat/types'
// import {
//   UserContractConfigs,
//   BuildInfo,
//   GetConfigArtifacts,
//   validateBuildInfo,
//   GetProviderForChainId,
//   ConfigArtifacts,
//   SphinxJsonRpcProvider,
//   isHttpNetworkConfig,
//   getNetworkNameForChainId,
// } from '@sphinx-labs/core'

// /**
//  * Retrieves contract build info by name.
//  *
//  * @param sourceName Source file name.
//  * @param contractName Contract name within the source file.
//  * @returns Contract build info.
//  */
// export const getBuildInfo = async (
//   hre: HardhatRuntimeEnvironment,
//   sourceName: string,
//   contractName: string
// ): Promise<BuildInfo> => {
//   let buildInfo
//   try {
//     buildInfo = await hre.artifacts.getBuildInfo(
//       `${sourceName}:${contractName}`
//     )
//   } catch (err) {
//     try {
//       // Try also loading with the short source name, necessary when using the foundry
//       // hardhat plugin
//       const shortSourceName = path.basename(sourceName)
//       buildInfo = await hre.artifacts.getBuildInfo(
//         `${shortSourceName}:${contractName}`
//       )
//     } catch {
//       // Throwing the original error is probably more helpful here because using the
//       // foundry hardhat plugin is not a common usecase.
//       throw err
//     }
//   }

//   // Shouldn't happen, but might as well be safe.
//   if (buildInfo === undefined) {
//     throw new Error(
//       `unable to find build info for contract ${contractName} in ${sourceName}`
//     )
//   }

//   validateBuildInfo(buildInfo, 'hardhat')

//   return buildInfo
// }

// TODO(hardhat): Make sure to use the fully qualified name as the key to the configArtifacts.
// /**
//  * Creates a callback for `getConfigArtifacts`, which is a function that maps each contract in the
//  * config to its artifact and build info. We use a callback to create a standard interface for the
//  * `getConfigArtifacts` function, which has an implementation for Hardhat and Foundry.
//  *
//  * @param hre Hardhat runtime environment.
//  * @param contractConfigs Contract configurations.
//  * @param artifactFolder Path to the artifact folder.
//  * @param buildInfoFolder Path to the build info folder.
//  * @returns Paths to the build info and contract artifact files.
//  */
// export const makeGetConfigArtifacts = (
//   hre: HardhatRuntimeEnvironment
// ): GetConfigArtifacts => {
//   return async (
//     contractConfigs: UserContractConfigs
//   ): Promise<ConfigArtifacts> => {
//     const configArtifacts: ConfigArtifacts = {}
//     for (const [referenceName, contractConfig] of Object.entries(
//       contractConfigs
//     )) {
//       const artifact = hre.artifacts.readArtifactSync(contractConfig.contract)
//       const buildInfo = await getBuildInfo(
//         hre,
//         artifact.sourceName,
//         artifact.contractName
//       )
//       configArtifacts[referenceName] = {
//         artifact,
//         buildInfo,
//       }
//     }
//     return configArtifacts
//   }
// }

// /**
//  * Creates a callback for `getProviderFromChainId`, which is a function that returns a provider
//  * object for a given chain ID. We use a callback to create a standard interface for the
//  * `getProviderFromChainId` function, which has a different implementation in Hardhat and Foundry.
//  *
//  * @param hre Hardhat runtime environment.
//  * @returns The provider object that corresponds to the chain ID.
//  */
// export const makeGetProviderFromChainId = (
//   hre: HardhatRuntimeEnvironment
// ): GetProviderForChainId => {
//   return (chainId: number): SphinxJsonRpcProvider => {
//     const networkConfig = Object.values(hre.config.networks).find(
//       (network) => network.chainId === chainId
//     )
//     if (networkConfig === undefined) {
//       throw new Error(
//         `Unable to find the network ${getNetworkNameForChainId(
//           chainId
//         )} in your Hardhat config.`
//       )
//     }

//     if (!isHttpNetworkConfig(networkConfig)) {
//       throw new Error(
//         `The network in your Hardhat config with chain ID ${networkConfig.chainId} does not appear to be a live network.\n` +
//           `Only live networks are supported in Sphinx configs.`
//       )
//     }

//     return new SphinxJsonRpcProvider(networkConfig.url)
//   }
// }

// /**
//  * Get storage layouts from OpenZeppelin's Network Files for any proxies that are being imported
//  * into Sphinx from the OpenZeppelin Hardhat Upgrades plugin.
//  */
// // export const importOpenZeppelinStorageLayout = async (
// //   hre: HardhatRuntimeEnvironment,
// //   parsedContractConfig: ParsedContractConfig
// // ): Promise<StorageLayout> => {
// //   const { kind } = parsedContractConfig
// //   const proxy = parsedContractConfig.address
// // TODO(upgrades): you may want to change `hre.network.provider` since it may not be tied
// // to the network that we actually care about
// //   const manifest = await Manifest.forNetwork(hre.network.provider)
// //   const deployData = await getDeployData(
// //     hre,
// //     await hre.ethers.getContractFactory(parsedContractConfig.contract),
// //     withValidationDefaults({
// //       kind: toOpenZeppelinContractKind(kind),
// //     })
// //   )

// //   const provider = new SphinxJsonRpcProvider(hre.ethers.provider.connection.url)
// //   const storageLayout = await getStorageLayoutForAddress(
// //     manifest,
// //     deployData.validations,
// //     await getEIP1967ProxyImplementationAddress(provider, proxy)
// //   )
// //   return storageLayout
// // }
