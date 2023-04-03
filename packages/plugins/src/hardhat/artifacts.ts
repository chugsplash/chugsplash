import path from 'path'

import { HardhatRuntimeEnvironment } from 'hardhat/types'
import {
  ArtifactPaths,
  UserContractConfigs,
  getEIP1967ProxyImplementationAddress,
  getOpenZeppelinValidationOpts,
  BuildInfo,
  ParsedContractConfig,
} from '@chugsplash/core'
import {
  Manifest,
  getStorageLayoutForAddress,
  StorageLayout,
} from '@openzeppelin/upgrades-core'
import { getDeployData } from '@openzeppelin/hardhat-upgrades/dist/utils/deploy-impl'

/**
 * Retrieves contract build info by name.
 *
 * @param sourceName Source file name.
 * @param contractName Contract name within the source file.
 * @returns Contract build info.
 */
export const getBuildInfo = async (
  hre: HardhatRuntimeEnvironment,
  sourceName: string,
  contractName: string
): Promise<BuildInfo> => {
  let buildInfo
  try {
    buildInfo = await hre.artifacts.getBuildInfo(
      `${sourceName}:${contractName}`
    )
  } catch (err) {
    try {
      // Try also loading with the short source name, necessary when using the foundry
      // hardhat plugin
      const shortSourceName = path.basename(sourceName)
      buildInfo = await hre.artifacts.getBuildInfo(
        `${shortSourceName}:${contractName}`
      )
    } catch {
      // Throwing the original error is probably more helpful here because using the
      // foundry hardhat plugin is not a common usecase.
      throw err
    }
  }

  // Shouldn't happen, but might as well be safe.
  if (buildInfo === undefined) {
    throw new Error(
      `unable to find build info for contract ${contractName} in ${sourceName}`
    )
  }

  return buildInfo
}

/**
 * Finds the path to the build info file and the contract artifact file for each contract
 * referenced in the given contract configurations.
 *
 * @param hre Hardhat runtime environment.
 * @param contractConfigs Contract configurations.
 * @param artifactFolder Path to the artifact folder.
 * @param buildInfoFolder Path to the build info folder.
 * @returns Paths to the build info and contract artifact files.
 */
export const getArtifactPaths = async (
  hre: HardhatRuntimeEnvironment,
  contractConfigs: UserContractConfigs,
  artifactFolder: string,
  buildInfoFolder: string
): Promise<ArtifactPaths> => {
  const artifactPaths: ArtifactPaths = {}
  for (const [referenceName, contractConfig] of Object.entries(
    contractConfigs
  )) {
    const { sourceName, contractName } = hre.artifacts.readArtifactSync(
      contractConfig.contract
    )
    const buildInfo = await getBuildInfo(hre, sourceName, contractName)
    artifactPaths[referenceName] = {
      buildInfoPath: path.join(buildInfoFolder, `${buildInfo.id}.json`),
      contractArtifactPath: path.join(
        artifactFolder,
        sourceName,
        `${contractName}.json`
      ),
    }
  }
  return artifactPaths
}

/**
 * Get storage layouts from OpenZeppelin's Network Files for any proxies that are being imported
 * into ChugSplash from the OpenZeppelin Hardhat Upgrades plugin.
 */
export const importOpenZeppelinStorageLayout = async (
  hre: HardhatRuntimeEnvironment,
  parsedContractConfig: ParsedContractConfig
): Promise<StorageLayout | undefined> => {
  const { proxyType } = parsedContractConfig
  if (
    proxyType === 'oz-transparent' ||
    proxyType === 'oz-ownable-uups' ||
    proxyType === 'oz-access-control-uups'
  ) {
    const proxy = parsedContractConfig.proxy
    const isProxyDeployed = await hre.ethers.provider.getCode(proxy)
    if (isProxyDeployed) {
      const manifest = await Manifest.forNetwork(hre.network.provider)
      const deployData = await getDeployData(
        hre,
        await hre.ethers.getContractFactory(parsedContractConfig.contract),
        getOpenZeppelinValidationOpts(
          parsedContractConfig.proxyType,
          parsedContractConfig
        )
      )
      const storageLayout = await getStorageLayoutForAddress(
        manifest,
        deployData.validations,
        await getEIP1967ProxyImplementationAddress(hre.ethers.provider, proxy)
      )
      return storageLayout
    }
  }
}
