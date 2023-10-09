import * as fs from 'fs'
import path, { join } from 'path'
import { promisify } from 'util'

import {
  BuildInfo,
  ContractArtifact,
} from '@sphinx-labs/core/dist/languages/solidity/types'
import {
  parseFoundryArtifact,
  validateBuildInfo,
  execAsync,
  getNetworkNameForChainId,
} from '@sphinx-labs/core/dist/utils'
import { SphinxJsonRpcProvider } from '@sphinx-labs/core/dist/provider'
import {
  ConfigArtifacts,
  GetConfigArtifacts,
  GetProviderForChainId,
  RawSphinxActionInput,
} from '@sphinx-labs/core/dist/config/types'
import { parse } from 'semver'
import chain from 'stream-chain'
import parser from 'stream-json'
import { streamValues } from 'stream-json/streamers/StreamValues'

const readFileAsync = promisify(fs.readFile)

export const streamJsonFile = async (filePath: string) => {
  const pipeline = new chain([
    fs.createReadStream(filePath),
    parser(),
    streamValues(),
    (data) => {
      return data
    },
  ])

  let buildInfo
  pipeline.on('data', (b) => {
    buildInfo = b.value
  })

  await new Promise((resolve) => pipeline.on('finish', resolve))
  return buildInfo
}

export const getBuildInfo = (
  buildInfos: Array<{
    buildInfo: BuildInfo
    name: string
  }>,
  sourceName: string
):
  | {
      buildInfo: BuildInfo
      name: string
    }
  | false => {
  // Find the correct build info file
  for (const input of buildInfos) {
    if (input?.buildInfo.output?.contracts[sourceName] !== undefined) {
      validateBuildInfo(input.buildInfo, 'foundry')
      return input
    }
  }

  return false
}

export const messageArtifactNotFound = (
  contractNameOrFullyQualifiedName: string
): string => {
  return (
    `Could not find artifact for: ${contractNameOrFullyQualifiedName}. Please make sure that this contract\n` +
    `exists in your contract, script, or test directory.`
  )
}

export const messageMultipleArtifactsFound = (
  contractNameOrFullyQualifiedName: string
): string => {
  return (
    `Detected multiple contracts with the name ${contractNameOrFullyQualifiedName}. Please use the fully \n` +
    `qualified name for this contract in the format: 'path/to/file/File.sol:MyContract'`
  )
}

export const getContractArtifact = async (
  fullyQualifiedName: string,
  artifactFolder: string
): Promise<ContractArtifact> => {
  // The basename will be in the format `SomeFile.sol:MyContract`.
  const basename = path.basename(fullyQualifiedName)

  const [sourceName, contractName] = basename.split(':')
  const artifactPath = join(artifactFolder, sourceName, `${contractName}.json`)
  if (!fs.existsSync(artifactPath)) {
    throw new Error(
      `Could not find artifact for: ${fullyQualifiedName}. Please reload your artifacts by running:\n` +
        `forge clean`
    )
  }
  return parseFoundryArtifact(
    JSON.parse(await readFileAsync(artifactPath, 'utf8'))
  )
}

/**
 * Creates a callback for `getProviderFromChainId`, which is a function that returns a provider
 * object for a given chain ID. We use a callback to create a standard interface for the
 * `getProviderFromChainId` function, which has a different implementation in Hardhat and Foundry.
 *
 * @param rpcEndpoints A map of chain aliases to RPC urls.
 * @returns The provider object that corresponds to the chain ID.
 */
export const makeGetProviderFromChainId = async (rpcEndpoints: {
  [chainAlias: string]: string
}): Promise<GetProviderForChainId> => {
  const urls = Object.values(rpcEndpoints)
  const networks = await Promise.all(
    urls.map(async (url) => {
      const provider = new SphinxJsonRpcProvider(url)
      try {
        // We put this RPC call in a try/catch because it may not be possible to connect to some of
        // the RPC endpoints in the foundry.toml file. For example, the user may have a local RPC
        // endpoint that is not currently running.
        const { chainId, name: networkName } = await provider.getNetwork()
        return { chainId: Number(chainId), url, networkName }
      } catch (err) {
        undefined
      }
    })
  )

  return (chainId: number): SphinxJsonRpcProvider => {
    const network = networks.find((n) => n && n.chainId === chainId)
    if (network === undefined) {
      throw new Error(
        `Could not find an RPC endpoint in your foundry.toml for the network: ${getNetworkNameForChainId(
          BigInt(chainId)
        )}.`
      )
    }

    return new SphinxJsonRpcProvider(network.url)
  }
}

/**
 * TODO: Reduce the memory footprint of this function by using a stream parser to read in the build
 * info files and only actually store the parts of the build info files which are really necessary.
 * This is important for making sure we do not run out of memory loading the build info files of large
 * projects.
 *
 * Creates a callback for `getConfigArtifacts`, which is a function that maps each contract in the
 * config to its artifact and build info. We use a callback to create a standard interface for the
 * `getConfigArtifacts` function, which has a separate implementation for the Hardhat and Foundry
 * plugin.
 */
export const makeGetConfigArtifacts = (
  artifactFolder: string,
  buildInfoFolder: string,
  cachePath: string
): GetConfigArtifacts => {
  return async (actions: Array<RawSphinxActionInput>) => {
    // Check if the cache directory exists, and create it if not
    if (!fs.existsSync(cachePath)) {
      fs.mkdirSync(cachePath)
    }

    const buildInfoCacheFilePath = join(cachePath, 'sphinx-cache.json')
    // We keep track of the last modified time in each build info file so we can easily find the most recently generated build info files
    // We also keep track of all the contract files output by each build info file, so we can easily look up the required file for each contract artifact
    let buildInfoCache: Record<
      string,
      {
        name: string
        time: number
        contracts: string[]
      }
    > = fs.existsSync(buildInfoCacheFilePath)
      ? JSON.parse(fs.readFileSync(buildInfoCacheFilePath, 'utf8'))
      : {}

    const buildInfoPath = join(buildInfoFolder)

    // Find all the build info files and their last modified time
    const buildInfoFileNames = fs
      .readdirSync(buildInfoPath)
      .filter((fileName) => {
        return fileName.endsWith('.json')
      })

    const cachedNames = Object.keys(buildInfoCache)
    // If there is only one build info file and it is not in the cache,
    // then clear the cache b/c the user must have force recompiled
    if (
      buildInfoFileNames.length === 1 &&
      (!cachedNames.includes(buildInfoFileNames[0]) ||
        // handles an edge case where the user made a change and then reverted it and force recompiled
        buildInfoFileNames.length > 1)
    ) {
      buildInfoCache = {}
    }

    const buildInfoFileNamesWithTime = buildInfoFileNames
      .map((fileName) => ({
        name: fileName,
        time: fs.statSync(path.join(buildInfoPath, fileName)).mtime.getTime(),
      }))
      .sort((a, b) => b.time - a.time)

    // Read all of the new/modified files and update the cache to reflect the changes
    // We intentionally do not cache the files we read here because we do not know if they
    // will be used or not and storing all of them can result in memory issues if there are
    // a lot of large build info files which can happen in large projects.
    await Promise.all(
      buildInfoFileNamesWithTime
        .filter((file) => buildInfoCache[file.name]?.time !== file.time)
        .map(async (file) => {
          // If the file exists in the cache and the time has changed, then we just update the time
          if (
            buildInfoCache[file.name]?.time &&
            buildInfoCache[file.name]?.time !== file.time
          ) {
            buildInfoCache[file.name].time = file.time
            return
          }

          const buildInfo = await streamJsonFile(
            join(buildInfoFolder, file.name)
          )

          // Update the build info file dictionary in the cache
          buildInfoCache[file.name] = {
            name: file.name,
            time: file.time,
            contracts: Object.keys(buildInfo.output.contracts),
          }
        })
    )
    // Just make sure the files are sorted by time
    const sortedCachedFiles = Object.values(buildInfoCache).sort(
      (a, b) => b.time - a.time
    )

    // Look through the cache, read all the contract artifacts, and find all of the required build
    // info files names. We get the artifacts every action, even if it'll be skipped, because the
    // artifact is necessary when we're creating the preview, which includes skipped actions.
    // We read in and store all of the required build info files here. This sometimes means we
    // read files twice (above, and then again here) which is not ideal, but reduces the memory
    // footprint of this function significantly in large projects.
    const toReadFiles: string[] = []
    const localBuildInfoCache = {}
    const resolved = await Promise.all(
      actions.map(async ({ fullyQualifiedName }) => {
        const artifact = await getContractArtifact(
          fullyQualifiedName,
          artifactFolder
        )

        // Look through the cache for the first build info file that contains the contract
        for (const file of sortedCachedFiles) {
          if (file.contracts?.includes(artifact.sourceName)) {
            const buildInfo =
              file.name in localBuildInfoCache
                ? (localBuildInfoCache[file.name] as BuildInfo)
                : undefined

            // Keep track of if we need to read the file or not
            if (!buildInfo && !toReadFiles.includes(file.name)) {
              toReadFiles.push(file.name)
            }

            return {
              fullyQualifiedName,
              artifact,
              buildInfoName: file.name,
              buildInfo,
            }
          }
        }

        // Throw an error if no build info file is found in the cache for this contract
        // This should only happen if the user manually deletes a build info file
        throw new Error(
          `Failed to find build info for ${artifact.sourceName}. Try recompiling with force: forge build --force`
        )
      })
    )

    // TODO(test): do this after deciding whether you can remove writing the config artifacts. run
    // `forge clean` then `npx sphinx deploy script/MyScript.s.sol --network anvil --broadcast`.
    // when i tried to do this, it froze for a couple mins, then i exited out.

    // Read any build info files that we didn't already have in memory
    await Promise.all(
      toReadFiles.map(async (file) => {
        try {
          const buildInfo = await streamJsonFile(join(buildInfoFolder, file))
          localBuildInfoCache[file] = buildInfo
        } catch (e) {
          // Throw an error if we can't read the file
          // This should only happen if the user manually deleted the file
          throw new Error(
            `Failed to read file ${file}. Try recompiling with force: forge build --force`
          )
        }
      })
    )

    // Combine the cached build infos with the contract artifacts
    const completeArtifacts = resolved.map((artifactInfo) => {
      return {
        ...artifactInfo,
        buildInfo: localBuildInfoCache[artifactInfo.buildInfoName],
      }
    })

    // Write the updated build info cache
    fs.writeFileSync(
      buildInfoCacheFilePath,
      JSON.stringify(buildInfoCache, null, 2)
    )

    const configArtifacts: ConfigArtifacts = {}

    for (const {
      fullyQualifiedName,
      artifact,
      buildInfo,
    } of completeArtifacts) {
      configArtifacts[fullyQualifiedName] = {
        artifact,
        buildInfo,
      }
    }

    return configArtifacts
  }
}

/**
 * Attempts to infer the default solc version given by `solc --version`. If this fails, it will
 * return the default solc version used by Foundry's "Getting Started" guide, which is 0.8.19.
 */
export const inferSolcVersion = async (): Promise<string> => {
  // This is the default solc version used by Foundry's "Getting Started" guide.
  const defaultSolcVersion = '0.8.19'
  try {
    const solcVersionOutput = await execAsync('solc --version')
    const solcVersionRaw = solcVersionOutput.stdout.split('Version: ')[1]
    const parsed = parse(solcVersionRaw)
    return parsed ? parsed.toString() : defaultSolcVersion
  } catch (err) {
    return defaultSolcVersion
  }
}
