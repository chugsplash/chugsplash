import path, { basename, dirname, join, relative } from 'path'
import { promisify } from 'util'
import {
  createReadStream,
  existsSync,
  readFile,
  readFileSync,
  readdirSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from 'fs'
import { execSync, spawnSync } from 'child_process'

import {
  BuildInfo,
  CompilerOutputContracts,
} from '@sphinx-labs/core/dist/languages/solidity/types'
import {
  decodeDeterministicDeploymentProxyData,
  exceedsContractSizeLimit,
  execAsync,
  formatSolcLongVersion,
  getBytesLength,
  hasParentheses,
  isArrayMixed,
  isDataHexString,
  isDefined,
  isNormalizedAddress,
  spawnAsync,
  trimQuotes,
  zeroOutLibraryReferences,
} from '@sphinx-labs/core/dist/utils'
import {
  ActionInput,
  ConfigArtifacts,
  DeploymentInfo,
  GetConfigArtifacts,
  InitialChainState,
  NetworkConfig,
  ParsedVariable,
  InternalSphinxConfig,
  UserSphinxConfigWithAddresses,
  BuildInfos,
} from '@sphinx-labs/core/dist/config/types'
import { parse } from 'semver'
import chain from 'stream-chain'
import parser from 'stream-json'
import { ignore } from 'stream-json/filters/Ignore'
import { pick } from 'stream-json/filters/Pick'
import { streamObject } from 'stream-json/streamers/StreamObject'
import { streamValues } from 'stream-json/streamers/StreamValues'
import {
  ExecutionMode,
  ParsedContractDeployment,
  SphinxJsonRpcProvider,
} from '@sphinx-labs/core'
import ora from 'ora'
import {
  ContractArtifact,
  LinkReferences,
  add0x,
  parseFoundryContractArtifact,
  recursivelyConvertResult,
  getSystemContractInfo,
  DETERMINISTIC_DEPLOYMENT_PROXY_ADDRESS,
  CONTRACTS_LIBRARY_VERSION,
  SPHINX_NETWORKS,
  NetworkType,
  ParsedAccountAccess,
  AccountAccessKind,
  AccountAccess,
} from '@sphinx-labs/contracts'
import { ConstructorFragment, ethers } from 'ethers'

import {
  FoundryMultiChainDryRun,
  FoundrySingleChainBroadcast,
  FoundrySingleChainDryRun,
  FoundryToml,
} from '../types'
import { AssertNoLinkedLibraries } from '../../cli/types'
import { BuildInfoTemplate, trimObjectToType } from './trim'
import { assertValidNodeVersion } from '../../cli/utils'
import {
  InvalidFirstSigArgumentErrorMessage,
  SigCalledWithNoArgsErrorMessage,
  SphinxConfigMainnetsContainsTestnetsErrorMessage,
  SphinxConfigTestnetsContainsMainnetsErrorMessage,
  getFailedRequestErrorMessage,
  getMissingEndpointErrorMessage,
  getMixedNetworkTypeErrorMessage,
  getUnsupportedNetworkErrorMessage,
} from '../error-messages'
import { contractsExceedSizeLimitErrorMessage } from '../../error-messages'

const readFileAsync = promisify(readFile)

/**
 * @field contracts An array where each element corresponds to a contract in the
 * `BuildInfo.output.contracts` object. We use this array to match collected contract init code to
 * its artifact (in the `isInitCodeMatch` function).
 */
type BuildInfoCacheEntry = {
  name: string
  time: number
  contracts: Array<{
    fullyQualifiedName: string
    bytecode: string
    linkReferences: LinkReferences
    constructorFragment?: ethers.ConstructorFragment
  }>
}

export const streamBuildInfoCacheContracts = async (filePath: string) => {
  const pipeline = new chain([
    createReadStream(filePath),
    parser(),
    pick({ filter: 'output' }),
    pick({ filter: 'contracts' }),
    streamObject(),
    (data: { key: string; value: CompilerOutputContracts[string] }) => {
      const sourceName = data.key

      const contracts: BuildInfoCacheEntry['contracts'] = []
      for (const [contractName, contract] of Object.entries(data.value)) {
        const iface = new ethers.Interface(contract.abi)
        const constructorFragment = iface.fragments.find(
          ConstructorFragment.isFragment
        )

        contracts.push({
          fullyQualifiedName: `${sourceName}:${contractName}`,
          bytecode: add0x(contract.evm.bytecode.object),
          linkReferences: contract.evm.bytecode.linkReferences,
          constructorFragment,
        })
      }
      return contracts
    },
  ])

  const buildInfoCacheContracts: BuildInfoCacheEntry['contracts'] = []
  pipeline.on('data', (name) => {
    buildInfoCacheContracts.push(name)
  })

  await new Promise((resolve) => pipeline.on('finish', resolve))
  return buildInfoCacheContracts
}

export const streamBuildInfo = async (filePath: string) => {
  const pipeline = new chain([
    createReadStream(filePath),
    parser(),
    ignore({ filter: 'output' }),
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

export const messageArtifactNotFound = (fullyQualifiedName: string): string => {
  return (
    `Could not find artifact for: ${fullyQualifiedName}. Please reload your artifacts by running:\n` +
    `forge clean`
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

/**
 * @notice Read a Foundry contract artifact from the file system.
 * @dev The location of an artifact file will be nested in the artifacts folder if there's more than
 * one contract in the contract source directory with the same name. This function ensures that we
 * retrieve the correct contract artifact in all cases. It works by checking the deepest possible
 * file location, and searching shallower directories until the file is found or until all
 * possibilities are exhausted.
 *
 * Example: Consider a project with a file structure where the project root is at
 * '/Users/dev/myRepo', and the contract is defined in
 * '/Users/dev/myRepo/src/tokens/MyFile.sol:MyContract'. The function will first try to find the
 * artifact in 'myRepo/artifacts/src/tokens/MyFile/MyContract.json'. If not found, it will then try
 * 'myRepo/artifacts/tokens/MyFile/MyContract.json' (notice that 'src/' is removed in this attempt),
 * and finally 'myRepo/artifacts/MyFile/MyContract.json' (notice that 'src/tokens/ is removed in
 * this attempt). If the artifact is still not found, it throws an error.
 */
export const readContractArtifact = async (
  fullyQualifiedName: string,
  projectRoot: string,
  artifactFolder: string
): Promise<ContractArtifact> => {
  // Get the source file name (e.g. `MyFile.sol`) and contract name (e.g. `MyContractName`).
  const [sourceFileName, contractName] = path
    .basename(fullyQualifiedName)
    .split(':')

  // Removes the source file name and the contract name from the path. For example, if the fully
  // qualified name is `/Users/dev/myRepo/src/MySourceFile.sol:MyContractName`, then the source
  // directory path will be `/Users/dev/myRepo/src/`.
  const sourceDirectoryPath = dirname(fullyQualifiedName)

  // The source directory path can either be an absolute path or a path relative to the project
  // root. We change it to always be a path relative to the project root because this is the only
  // relevant segment of the path for retrieving the artifact.
  const relativeSourceDirPath = path.relative(projectRoot, sourceDirectoryPath)

  // Split the relative source directory path into parts.
  let pathParts = relativeSourceDirPath.split(path.sep)
  // Loop through the path parts. We start with the entire relative path on the first iteration, and
  // we remove the base directory on each iteration. We'll keep looping until we find a path that
  // contains a contract artifact, or until we run out of path parts. For example, if the initial
  // relative path is 'myDir/contracts/tokens/', we'll start with this entire path on the first
  // iteration, then 'contracts/tokens/' on the second iteration, and 'tokens/' on the third.
  while (pathParts.length > 0) {
    const joinedPathParts = pathParts.join(path.sep)
    const currentPath = join(
      artifactFolder,
      joinedPathParts,
      sourceFileName,
      `${contractName}.json`
    )

    if (existsSync(currentPath)) {
      return parseFoundryContractArtifact(
        JSON.parse(await readFileAsync(currentPath, 'utf8'))
      )
    }

    // Remove the base path part.
    pathParts = pathParts.slice(1)
  }

  // If we make it to this point, the artifact must exist at the most shallow level of the artifacts
  // directory, or not exist at all.

  const shortestPath = join(
    artifactFolder,
    sourceFileName,
    `${contractName}.json`
  )
  if (existsSync(shortestPath)) {
    return parseFoundryContractArtifact(
      JSON.parse(await readFileAsync(shortestPath, 'utf8'))
    )
  }

  throw new Error(messageArtifactNotFound(fullyQualifiedName))
}

/**
 * Returns the init code of every contract deployment collected from a Forge script.
 */
export const getInitCodeWithArgsArray = (
  accountAccesses: Array<ParsedAccountAccess>
): Array<string> => {
  const flat = accountAccesses.flatMap((access) => [
    access.root,
    ...access.nested,
  ])

  return flat
    .filter((accountAccess) => accountAccess.kind === AccountAccessKind.Create)
    .map((accountAccess) => accountAccess.data)
}

/**
 * Compile the contracts using Forge.
 *
 * @param force Force re-compile the contracts. This ensures that we're using the most recent
 * artifacts for the user's contracts. This is mostly out of an abundance of caution, since using an
 * incorrect contract artifact will prevent us from creating the correct contract deployment
 * artifact and verifying the contract on Etherscan. It's best to force re-compile as late as
 * possible in commands like the Deploy and Propose CLI commands because recompilation can take a
 * very long time. If recompilation occurs early and the user runs into errors later in the command,
 * they'll spend a lot of time waiting for recompilation to occur each time they run the command.
 * It's fine for recompilation to occur after running the user's Forge script because Foundry
 * automatically compiles the necessary contracts before executing it.
 */
export const compile = (
  silent: boolean,
  force: boolean,
  buildInfo: boolean
): void => {
  const forgeBuildArgs = ['build']

  if (silent) {
    forgeBuildArgs.push('--silent')
  }
  if (force) {
    forgeBuildArgs.push('--force')
  }
  if (buildInfo) {
    forgeBuildArgs.push('--build-info')
  }

  // We use `spawnSync` to display the compilation process to the user as it occurs. Compiler errors
  // will be displayed to the user even if the `silent` flag is included.
  const { status: compilationStatus } = spawnSync(`forge`, forgeBuildArgs, {
    stdio: 'inherit',
  })
  if (compilationStatus !== 0) {
    process.exit(1)
  }
}

/**
 * Throws an error if there are any linked libraries in `scriptPath`. If a `targetContract` is
 * defined, this function simply builds fully qualified name, then uses it to load the script's
 * artifact. If a `targetContract` isn't defined, this function searches the build info cache to
 * find the most recent fully qualified name for the given `sourceName`. We use the build info cache
 * because it's more reliable than searching Foundry's artifact file tree, which can be
 * unpredictable due to the potentially nested structure of artifact file locations, and due to the
 * fact that we don't know the fully qualified name in advance.
 */
export const assertNoLinkedLibraries: AssertNoLinkedLibraries = async (
  scriptPath: string,
  cachePath: string,
  artifactFolder: string,
  projectRoot: string,
  targetContract?: string
): Promise<void> => {
  const fullyQualifiedName = targetContract
    ? `${scriptPath}:${targetContract}`
    : // Find the fully qualified name using its source name. We can safely assume
      // that there's a single fully qualified name in the array returned by
      // `findFullyQualifiedNames` because the user's Forge script was executed successfully before
      // this function was called, which means there must only be a single contract.
      findFullyQualifiedNames(scriptPath, cachePath, projectRoot)[0]
  const artifact = await readContractArtifact(
    fullyQualifiedName,
    projectRoot,
    artifactFolder
  )

  const containsLibrary =
    Object.keys(artifact.linkReferences).length > 0 ||
    Object.keys(artifact.deployedLinkReferences).length > 0
  if (containsLibrary) {
    throw new Error(
      `Detected linked library in: ${fullyQualifiedName}\n` +
        `You must remove all linked libraries in this file because Sphinx currently doesn't support them.`
    )
  }
}

/**
 * Returns an array of the most recent fully qualified names for the given `sourceName`. This
 * function searches the build info cache for the most recent build info file that contains a fully
 * qualified name starting with `sourceName`. Returns an empty array if there is no such fully
 * qualified name in any of the cached build info files.
 */
const findFullyQualifiedNames = (
  rawSourceName: string,
  cachePath: string,
  projectRoot: string
): Array<string> => {
  const buildInfoCacheFilePath = join(cachePath, 'sphinx-cache.json')

  // Normalize the source name so that it conforms to the format of the fully qualified names in the
  // build info cache. The normalized format is "path/to/file.sol".
  const sourceName = relative(projectRoot, rawSourceName)

  const buildInfoCache: Record<string, BuildInfoCacheEntry> = JSON.parse(
    readFileSync(buildInfoCacheFilePath, 'utf8')
  )

  // Sort the build info files from most recent to least recent.
  const sortedCachedFiles = Object.values(buildInfoCache).sort(
    (a, b) => b.time - a.time
  )

  for (const { contracts } of sortedCachedFiles) {
    const fullyQualifiedNames = contracts
      .filter((contract) => contract.fullyQualifiedName.startsWith(sourceName))
      .map((contract) => contract.fullyQualifiedName)

    if (fullyQualifiedNames.length > 0) {
      return fullyQualifiedNames
    }
  }

  return []
}

/**
 * Creates a callback for `getConfigArtifacts`, which is a function that maps each contract in the
 * config to its artifact and build info. We use a callback to create a standard interface for the
 * `getConfigArtifacts` function, which may be used by Sphinx's future Hardhat plugin.
 *
 * @dev We do not use this function directly, instead we call it via SphinxContext to facilitate
 * dependency injection.
 */
export const makeGetConfigArtifacts = (
  artifactFolder: string,
  buildInfoFolder: string,
  projectRoot: string,
  cachePath: string
): GetConfigArtifacts => {
  return async (initCodeWithArgsIncludingDuplicates: Array<string>) => {
    // Remove duplicates from the array. This is a performance optimization that prevents us from
    // needing to search for the same artifact multiple times.
    const initCodeWithArgsArray = Array.from(
      new Set(initCodeWithArgsIncludingDuplicates)
    )

    const buildInfoCacheFilePath = join(cachePath, 'sphinx-cache.json')
    // We keep track of the last modified time in each build info file so we can easily find the most recently generated build info files
    // We also keep track of all the contract files output by each build info file, so we can easily look up the required file for each contract artifact
    let buildInfoCache: Record<string, BuildInfoCacheEntry> = existsSync(
      buildInfoCacheFilePath
    )
      ? JSON.parse(readFileSync(buildInfoCacheFilePath, 'utf8'))
      : {}

    const buildInfoPath = join(buildInfoFolder)

    // Find all the build info files and their last modified time
    const buildInfoFileNames = readdirSync(buildInfoPath).filter((fileName) => {
      return fileName.endsWith('.json')
    })

    const cachedNames = Object.keys(buildInfoCache)
    // If there is only one build info file and it is not in the cache,
    // then clear the cache b/c the user must have force recompiled
    if (
      buildInfoFileNames.length === 1 &&
      !cachedNames.includes(buildInfoFileNames[0])
    ) {
      buildInfoCache = {}
    }

    // Remove any files in the cache that no longer exist
    for (const cachedName of cachedNames) {
      if (!buildInfoFileNames.includes(cachedName)) {
        delete buildInfoCache[cachedName]
      }
    }

    const buildInfoFileNamesWithTime = buildInfoFileNames
      .map((fileName) => ({
        name: fileName,
        time: statSync(path.join(buildInfoPath, fileName)).mtime.getTime(),
      }))
      .sort((a, b) => b.time - a.time)

    // Read all of the new/modified files and update the cache to reflect the changes
    // We intentionally do not cache the files we read here because we do not know if they
    // will be used or not and storing all of them can result in memory issues if there are
    // a lot of large build info files which can happen in large projects.
    for (const file of buildInfoFileNamesWithTime) {
      // If the file exists in the cache and the time has changed, then we just update the time
      if (
        buildInfoCache[file.name]?.time &&
        buildInfoCache[file.name]?.time !== file.time
      ) {
        buildInfoCache[file.name].time = file.time
      } else if (!buildInfoCache[file.name]) {
        // Update the build info file dictionary in the cache
        buildInfoCache[file.name] = {
          name: file.name,
          time: file.time,
          contracts: await streamBuildInfoCacheContracts(
            join(buildInfoFolder, file.name)
          ),
        }
      }
    }

    // Just make sure the files are sorted by time
    const sortedCachedFiles = Object.values(buildInfoCache).sort(
      (a, b) => b.time - a.time
    )

    // Look through the cache, read all the contract artifacts, and find all of the required build
    // info files names. We get the artifacts for every action, even if it'll be skipped, because the
    // artifact is necessary when we're creating the deployment preview, which includes skipped actions.
    const toReadFiles: string[] = []
    const localBuildInfoCache = {}

    const fullyQualifiedNamePromises = initCodeWithArgsArray.map(
      async (initCodeWithArgs) => {
        // Look through the cache for the first build info file that contains the contract
        for (const file of sortedCachedFiles) {
          const contract = file.contracts.find((ct) => {
            const { bytecode, constructorFragment, linkReferences } = ct

            return isInitCodeMatch(initCodeWithArgs, {
              bytecode,
              linkReferences,
              constructorFragment,
            })
          })

          if (contract) {
            // Keep track of if we need to read the file or not
            if (!toReadFiles.includes(file.name)) {
              toReadFiles.push(file.name)
            }

            const artifact = await readContractArtifact(
              contract.fullyQualifiedName,
              projectRoot,
              artifactFolder
            )

            return {
              fullyQualifiedName: contract.fullyQualifiedName,
              artifact,
              buildInfoName: file.name,
            }
          }
        }
      }
    )

    // Resolve the promises, then filter out any that resolved to `undefined`, which will happen if
    // we couldn't find an artifact for a contract's init code. We can fail to find the artifact
    // either because the user defined the contract as inline bytecode (e.g. a `CREATE3` proxy) or
    // the user deleted a build info file.
    const resolved = (await Promise.all(fullyQualifiedNamePromises)).filter(
      isDefined
    )

    // Read any build info files that we didn't already have in memory. This sometimes means we read
    // files twice (above, and then again here) which is not ideal, but reduces the memory footprint
    // of this function significantly in large projects.
    await Promise.all(
      toReadFiles.map(async (file) => {
        const fullFilePath = join(buildInfoFolder, file)
        if (!existsSync(fullFilePath)) {
          if (existsSync(buildInfoCacheFilePath)) {
            unlinkSync(buildInfoCacheFilePath)
          }
          throw new Error(
            `Build info cache is outdated, please run 'forge build --force' then try again.`
          )
        } else {
          const buildInfo = await streamBuildInfo(fullFilePath)
          localBuildInfoCache[file] = buildInfo
        }
      })
    )

    // Combine the cached build infos with the contract artifacts
    const completeArtifacts = resolved.map((artifactInfo) => {
      return {
        ...artifactInfo,
        buildInfo: trimObjectToType<BuildInfo>(
          localBuildInfoCache[artifactInfo.buildInfoName],
          BuildInfoTemplate
        ),
      }
    })

    // Write the updated build info cache
    writeFileSync(
      buildInfoCacheFilePath,
      JSON.stringify(buildInfoCache, null, 2)
    )

    const configArtifacts: ConfigArtifacts = {}
    const buildInfos: BuildInfos = {}

    for (const {
      fullyQualifiedName,
      artifact,
      buildInfo,
    } of completeArtifacts) {
      buildInfo.solcLongVersion = formatSolcLongVersion(
        buildInfo.solcLongVersion
      )

      buildInfos[buildInfo.id] = buildInfo

      configArtifacts[fullyQualifiedName] = {
        artifact,
        buildInfoId: buildInfo.id,
      }
    }

    return { configArtifacts, buildInfos }
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

/**
 * Returns `true` if the given contract init code with constructor args belongs to the given
 * contract artifact, and returns `false` otherwise. There may be a couple differences between this
 * init code (which we'll call the "actual" init code) and the contract artifact's `bytecode` field.
 * We'll need to account for these differences when determining whether the init code belongs to the
 * artifact.
 *
 * These differences are:
 * 1. The actual init code may be appended with ABI-encoded constructor arguments, whereas the
 * artifact's init code is not.
 * 2. If the contract uses libraries, the actual init code contains library addresses, whereas the
 * artifact's init code contains library placeholders. For more info on library placeholders,
 * see: https://docs.soliditylang.org/en/v0.8.23/using-the-compiler.html#library-linking
 *
 * This function does not need to handle immutable variable references because these references only
 * exist in the runtime bytecode and not the init code. This is made clear by the Solidity docs,
 * which only have a `deployedBytecode.immutableReferences` field:
 * https://docs.soliditylang.org/en/v0.8.23/using-the-compiler.html#library-linking
 *
 * @param artifact Artifact info. This object contains only the necessary variables from the
 * artifact because we store these variables in a cache file. Storing the entire artifact in the
 * cache would result in an enormous cache size because we need to store the artifact info for
 * each contract in the user's repository.
 */
export const isInitCodeMatch = (
  actualInitCodeWithArgs: string,
  artifact: {
    bytecode: string
    linkReferences: LinkReferences
    constructorFragment?: ethers.ConstructorFragment
  }
): boolean => {
  const coder = ethers.AbiCoder.defaultAbiCoder()

  const artifactBytecodeLength = getBytesLength(artifact.bytecode)
  const actualInitCodeLength = getBytesLength(actualInitCodeWithArgs)

  // Return `false` if the length of the artifact's init code is greater than the length of the
  // actual init code. It's necessary to explicitly check this because the `ethers.dataSlice` call,
  // which we'll execute soon, reverts with an out-of-bounds error under this condition.
  if (artifactBytecodeLength > actualInitCodeLength) {
    return false
  }

  // Split the actual init code into two parts:
  // 1. The init code without the constructor arguments
  // 2. The ABI encoded constructor arguments
  //
  // We use the length of the `artifact.bytecode` to determine where the contract's creation code
  // ends and the constructor arguments begin. This works even if the `artifact.bytecode` contains
  // externally linked library placeholders, which are always the same length as the real values.
  const actualInitCodeNoArgs = ethers.dataSlice(
    actualInitCodeWithArgs,
    0,
    artifactBytecodeLength
  )
  const encodedArgs = ethers.dataSlice(
    actualInitCodeWithArgs,
    artifactBytecodeLength
  )

  if (artifact.constructorFragment) {
    // ABI-decode the constructor arguments. This will throw an error if the decoding fails.
    try {
      coder.decode(artifact.constructorFragment.inputs, encodedArgs)
    } catch {
      return false
    }
  } else if (
    // If there's no constructor fragment, the length of the artifact's init code and the actual
    // init code must match. They must match for contracts without a constructor fragment because
    // the artifact's init code does _not_ include constructor arguments, whereas the actual init
    // code does.
    artifactBytecodeLength !== actualInitCodeLength
  ) {
    return false
  }

  // Replace the library references with zeros in both init codes. In the actual init code, this
  // will replace the actual library addresses. In the artifact's init code, this will replace the
  // library placeholders with zeros.
  const artifactInitCodeNoLibraries = zeroOutLibraryReferences(
    artifact.bytecode,
    artifact.linkReferences
  )
  const actualInitCodeNoLibraries = zeroOutLibraryReferences(
    actualInitCodeNoArgs,
    artifact.linkReferences
  )

  // Check if we've found a match.
  return (
    artifactInitCodeNoLibraries.toLowerCase() ===
    actualInitCodeNoLibraries.toLowerCase()
  )
}

export const getSphinxConfigFromScript = async (
  scriptPath: string,
  sphinxPluginTypesInterface: ethers.Interface,
  targetContract?: string,
  spinner?: ora.Ora
): Promise<UserSphinxConfigWithAddresses> => {
  const json = await callForgeScriptFunction<{
    0: {
      value: string
    }
  }>(
    scriptPath,
    'userSphinxConfigABIEncoded()',
    [],
    undefined,
    targetContract,
    spinner
  )

  const returned = json.returns[0].value
  // ABI decode the gas array.
  const coder = ethers.AbiCoder.defaultAbiCoder()
  const sphinxConfigFragment = findFunctionFragment(
    sphinxPluginTypesInterface,
    'userSphinxConfigType'
  )

  const decoded = coder.decode(
    [...sphinxConfigFragment.outputs, 'address', 'address'],
    returned
  )

  const { userSphinxConfig } = recursivelyConvertResult(
    sphinxConfigFragment.outputs,
    decoded
  ) as any

  const parsed: UserSphinxConfigWithAddresses = {
    projectName: userSphinxConfig.projectName,
    testnets: userSphinxConfig.testnets,
    mainnets: userSphinxConfig.mainnets,
    safeAddress: decoded[1],
    moduleAddress: decoded[2],
  }

  return parsed
}

type ForgeScriptResponse<T> = {
  logs: Array<string>
  returns: T
}

export const getForgeScriptArgs = (
  scriptPath: string,
  signature: string,
  args: string[],
  forkUrl?: string,
  targetContract?: string,
  silent: boolean = true,
  json: boolean = true,
  broadcast: boolean = false
) => {
  const forgeScriptArgs = [
    'script',
    scriptPath,
    ...(forkUrl ? ['--rpc-url', forkUrl] : []),
    '--sig',
    signature,
    ...args,
  ]

  if (silent) {
    forgeScriptArgs.push('--silent')
  }

  if (json) {
    forgeScriptArgs.push('--json')
  }

  if (broadcast) {
    forgeScriptArgs.push('--broadcast')
  }

  if (targetContract) {
    forgeScriptArgs.push('--target-contract', targetContract)
  }

  return forgeScriptArgs
}

export const callForgeScriptFunction = async <T>(
  scriptPath: string,
  signature: string,
  args: string[],
  forkUrl?: string,
  targetContract?: string,
  spinner?: ora.Ora
): Promise<ForgeScriptResponse<T>> => {
  // First we call without silent or json and detect any failures
  // We have to do this b/c the returned `code` will be 0 even if the script failed.
  // Also the trace isn't output if `--silent` or `--json` is enabled, so we also have
  // to do this to provide a useful trace to the user.
  const testScriptArgs = getForgeScriptArgs(
    scriptPath,
    signature,
    args,
    forkUrl,
    targetContract,
    false,
    false
  )
  const {
    code: testCode,
    stdout: testOut,
    stderr: testErr,
  } = await spawnAsync('forge', testScriptArgs, {
    // We specify build info to be false so that calling the script does not cause the users entire
    // project to be rebuilt if they have `build_info=true` defined in their foundry.toml file.
    // We do need the build info, but that is generated when we compile at the beginning of the script.
    FOUNDRY_BUILD_INFO: 'false',
  })

  if (testCode !== 0) {
    spinner?.stop()
    // The `stdout` contains the trace of the error.
    console.log(testOut)
    // The `stderr` contains the error message.
    console.log(testErr)
    process.exit(1)
  }

  // Then call with silent and json, and parse the result
  const forgeScriptArgs = getForgeScriptArgs(
    scriptPath,
    signature,
    args,
    forkUrl,
    targetContract,
    true,
    true
  )

  const { code, stdout, stderr } = await spawnAsync('forge', forgeScriptArgs, {
    // We specify build info to be false so that calling the script does not cause the users entire
    // project to be rebuilt if they have `build_info=true` defined in their foundry.toml file.
    // We do need the build info, but that is generated when we compile at the beginning of the script.
    FOUNDRY_BUILD_INFO: 'false',
  })

  // For good measure, we still read the code and error if necessary but this is unlikely to be triggered
  if (code !== 0) {
    spinner?.stop()
    // The `stdout` contains the trace of the error.
    console.log(stdout)
    // The `stderr` contains the error message.
    console.log(stderr)
    process.exit(1)
  }

  // Attempt to parse the `stdout`. This could fail if the user supplies an invalid Etherscan API
  // key or URL in their `foundry.toml`. In this scenario, Foundry does not throw an error; instead,
  // it writes a message to `stdout` that contains "etherscan: Failed to deserialize response".
  try {
    return JSON.parse(stdout)
  } catch {
    throw new Error(`Failed to parse Foundry output. Reason:\n${stdout}`)
  }
}

export const isFoundryMultiChainDryRun = (
  dryRun: FoundrySingleChainDryRun | FoundryMultiChainDryRun
): dryRun is FoundryMultiChainDryRun => {
  const multiChainDryRun = dryRun as FoundryMultiChainDryRun
  return (
    Array.isArray(multiChainDryRun.deployments) &&
    !isFoundrySingleChainDryRun(dryRun)
  )
}

export const isFoundrySingleChainDryRun = (
  dryRun: FoundrySingleChainDryRun | FoundryMultiChainDryRun
): dryRun is FoundrySingleChainDryRun => {
  const singleChainDryRun = dryRun as FoundrySingleChainDryRun
  return (
    Array.isArray(singleChainDryRun.transactions) &&
    Array.isArray(singleChainDryRun.receipts) &&
    Array.isArray(singleChainDryRun.libraries)
  )
}

/**
 * Read a Foundry multi-chain dry run file.
 *
 * @param timeThreshold The earliest time that the dry run file could have been written. This
 * function will return `undefined` if the dry run file was modified earlier than this time. This
 * ensures that we don't read a dry run file from an outdated dry run.
 */
export const readFoundryMultiChainDryRun = (
  broadcastFolder: string,
  scriptPath: string,
  functionNameOrSelector: string,
  timeThreshold: Date
): FoundryMultiChainDryRun | undefined => {
  // An example of the file location:
  // `broadcast/multi/dry-run/MyScript.s.sol-latest/myScriptFunctionName.json`
  const dryRunPath = join(
    broadcastFolder,
    'multi',
    'dry-run',
    `${basename(scriptPath)}-latest`,
    `${functionNameOrSelector}.json`
  )

  // Check that the file exists and it was modified later than the supplied time threshold. If the
  // file doesn't exist, this means there weren't any broadcasted transactions. If the file exists
  // but it was modified earlier than the time threshold, this means there's an outdated dry run at
  // the file location, which will also means there weren't any broadcasted transactions in the most
  // recent Forge script run. We don't want to use an outdated file because it likely contains a
  // different deployment.
  if (existsSync(dryRunPath) && statSync(dryRunPath).mtime > timeThreshold) {
    return JSON.parse(readFileSync(dryRunPath, 'utf8'))
  } else {
    return undefined
  }
}

export const getFoundrySingleChainDryRunPath = (
  broadcastFolder: string,
  scriptPath: string,
  chainId: string | bigint | number,
  functionNameOrSelector: string
): string => {
  // If the script is in a subdirectory (e.g. script/my/path/MyScript.s.sol), Foundry still only
  // uses only the script's file name, not its entire path. An example of the file location:
  // broadcast/MyScriptName/31337/dry-run/myScriptFunctionName-latest.json
  return join(
    broadcastFolder,
    basename(scriptPath),
    chainId.toString(),
    'dry-run',
    `${functionNameOrSelector}-latest.json`
  )
}

/**
 * @param timeThreshold The earliest time that the dry run file could have been written. This
 * function will return `undefined` if the dry run file was modified earlier than this time. This
 * ensures that we don't read a dry run file from an outdated dry run.
 */
export const readFoundrySingleChainBroadcast = (
  broadcastFolder: string,
  scriptPath: string,
  chainId: string | number | bigint,
  functionNameOrSelector: string,
  timeThreshold: Date
): FoundrySingleChainBroadcast | undefined => {
  const broadcastFilePath = join(
    broadcastFolder,
    basename(scriptPath),
    chainId.toString(),
    `${functionNameOrSelector}-latest.json`
  )

  // Check that the file exists and it was modified later than the supplied time threshold. If the
  // file doesn't exist, this means there weren't any broadcasted transactions. If the file exists
  // but it was modified earlier than the time threshold, this means there's an outdated broadcast
  // at the file location, which will also means there weren't any broadcasted transactions in the
  // most recent Forge script run. We don't want to use an outdated file because it likely contains
  // a different deployment.
  if (
    existsSync(broadcastFilePath) &&
    statSync(broadcastFilePath).mtime > timeThreshold
  ) {
    return JSON.parse(readFileSync(broadcastFilePath, 'utf8'))
  } else {
    return undefined
  }
}

/**
 * Read a Foundry multi-chain dry run file.
 *
 * @param timeThreshold The earliest time that the dry run file could have been written. This
 * function will return `undefined` if the dry run file was modified earlier than this time. This
 * ensures that we don't read a dry run file from an outdated dry run.
 */
export const readFoundrySingleChainDryRun = (
  broadcastFolder: string,
  scriptPath: string,
  chainId: string | bigint | number,
  functionNameOrSelector: string,
  timeThreshold: Date
): FoundrySingleChainDryRun | undefined => {
  const dryRunPath = getFoundrySingleChainDryRunPath(
    broadcastFolder,
    scriptPath,
    chainId.toString(),
    functionNameOrSelector
  )

  // Check that the file exists and it was modified later than the supplied time threshold. If the
  // file doesn't exist, this means there weren't any broadcasted transactions. If the file exists
  // but it was modified earlier than the time threshold, this means there's an outdated dry run at
  // the file location, which will also means there weren't any broadcasted transactions in the most
  // recent Forge script run. We don't want to use an outdated file because it likely contains a
  // different deployment.
  if (existsSync(dryRunPath) && statSync(dryRunPath).mtime > timeThreshold) {
    return JSON.parse(readFileSync(dryRunPath, 'utf8'))
  } else {
    return undefined
  }
}

export const readInterface = (
  artifactFolder: string,
  contractName: string
): ethers.Interface => {
  const abi: Array<any> =
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    require(path.resolve(
      `${artifactFolder}/${contractName}.sol/${contractName}.json`
    )).abi
  return new ethers.Interface(abi)
}

export const findFunctionFragment = (
  iface: ethers.Interface,
  fragmentName: string
): ethers.FunctionFragment => {
  const functionFragment = iface.fragments
    .filter(ethers.Fragment.isFunction)
    .find((fragment) => fragment.name === fragmentName)
  if (!functionFragment) {
    throw new Error(`Fragment not found in ABI.`)
  }
  return functionFragment
}

export const convertLibraryFormat = (
  librariesArray: Array<string>
): Array<string> => {
  return librariesArray.map((libraryString) => {
    // Splitting by both ':' and '='
    const parts = libraryString.split(/[:=]/)
    if (parts.length !== 3) {
      throw new Error('Invalid library string format.')
    }

    const [filePath, contractName, address] = parts
    return `${filePath}:${contractName}=${ethers.getAddress(address)}`
  })
}

/**
 * Recursively replaces environment variable placeholders in the input with their actual values.
 * This function does not mutate the original object.
 */
export const replaceEnvVariables = (input: ParsedVariable): any => {
  // Regular expression to match environment variables in the form ${VAR_NAME}
  const envVarRegex = /\$\{((\w|\s)+)\}/g

  // Function to replace environment variables in a string with their values
  const replaceEnvVar = (str: string): string => {
    // Trim whitespace and then replace environment variables
    return str.trim().replace(envVarRegex, (_, envVar) => {
      return process.env[envVar.trim()] || ''
    })
  }

  if (typeof input === 'string') {
    // If the input is a string, replace environment variables in it
    return replaceEnvVar(input)
  } else if (Array.isArray(input)) {
    // If the input is an array, recursively process each element
    return input.map((element) => replaceEnvVariables(element))
  } else if (typeof input === 'object' && input !== null) {
    // If the input is an object, recursively process each property
    const result: { [key: string]: ParsedVariable } = {}
    for (const key in input) {
      if (input.hasOwnProperty(key)) {
        result[key] = replaceEnvVariables(input[key])
      }
    }
    return result
  } else {
    // For booleans and numbers, return as is
    return input
  }
}

/**
 * Searches the `configArtifacts` to find the fully qualified name for the given `initCodeWithArgs`.
 * Returns `undefined` if the `initCodeWithArgs` does not exist in the `configArtifacts`, which
 * means that it does not belong to a source file.
 */
export const findFullyQualifiedNameForInitCode = (
  initCodeWithArgs: string,
  configArtifacts: ConfigArtifacts
): string | undefined => {
  for (const fullyQualifiedName of Object.keys(configArtifacts)) {
    const { artifact } = configArtifacts[fullyQualifiedName]
    const { bytecode, linkReferences, abi } = artifact

    const iface = new ethers.Interface(abi)
    const constructorFragment = iface.fragments.find(
      ConstructorFragment.isFragment
    )

    if (
      isInitCodeMatch(initCodeWithArgs, {
        bytecode,
        linkReferences,
        constructorFragment,
      })
    ) {
      return fullyQualifiedName
    }
  }
  // If we make it to this point, we couldn't find a fully qualified name for this init code.
  return undefined
}

/**
 * Returns the fully qualified name that corresponds to a given address. Returns `undefined` if no
 * fully qualified name exists for the address. This function will only find a fully qualified name
 * if it corresponds to a contract deployed in one of the actions.
 */
export const findFullyQualifiedNameForAddress = (
  address: string,
  accountAccesses: Array<ParsedAccountAccess>,
  configArtifacts: ConfigArtifacts
): string | undefined => {
  const flat = accountAccesses.flatMap((access) => [
    access.root,
    ...access.nested,
  ])

  const createAccountAccess = flat.find(
    (accountAccess) =>
      accountAccess.kind === AccountAccessKind.Create &&
      accountAccess.account === address
  )

  if (createAccountAccess) {
    const initCodeWithArgs = createAccountAccess.data
    return findFullyQualifiedNameForInitCode(initCodeWithArgs, configArtifacts)
  } else {
    return undefined
  }
}

/**
 * Write the ABI encoded Sphinx system contracts to the file system. This is useful when we need to
 * pass the system contract info from TypeScript to a Forge script. We write it to the file system
 * instead of passing it in as a parameter to the Forge script because it's possible to hit Node's
 * `spawn` input size limit if the data is large. This is particularly a concern because the data
 * contains contract init code.
 */
export const writeSystemContracts = (
  sphinxPluginTypesInterface: ethers.Interface,
  cachePath: string
): string => {
  const systemContractsArrayFragment = findFunctionFragment(
    sphinxPluginTypesInterface,
    'systemContractInfoArrayType'
  )

  const coder = ethers.AbiCoder.defaultAbiCoder()
  const filePath = join(cachePath, 'sphinx-system-contracts.txt')

  const encodedSystemContractArray = coder.encode(
    systemContractsArrayFragment.outputs,
    [getSystemContractInfo()]
  )

  // Write the ABI encoded data to the file system. We'll read it in the Forge script. We do this
  // instead of passing in the data as a parameter to the Forge script because it's possible to hit
  // Node's `spawn` input size limit if the data is large. This is particularly a concern because
  // the data contains contract init code.
  writeFileSync(filePath, encodedSystemContractArray)

  return filePath
}

/**
 * Checks if the `accountAccess` and the `nextAccountAccess` form a valid `CREATE2` deployment
 * through the Deterministic Deployment Proxy (i.e. Foundry's default `CREATE2` deployer).
 */
export const isCreate2AccountAccess = (
  root: AccountAccess,
  nested: Array<AccountAccess>
): boolean => {
  if (nested.length === 0) {
    return false
  }
  const nextAccountAccess = nested[0]

  // The first `AccountAccess` must be a `Call` to the Deterministic Deployment Proxy, and must have
  // data that's at least 32 bytes long, since the data is a 32-byte salt appended with the
  // contract's init code.
  const isCreate2Call =
    root.kind === AccountAccessKind.Call &&
    root.account === DETERMINISTIC_DEPLOYMENT_PROXY_ADDRESS &&
    getBytesLength(root.data) >= 32
  if (!isCreate2Call) {
    return false
  }

  const { create2Address } = decodeDeterministicDeploymentProxyData(root.data)

  // The next `AccountAccess` must be a `Create` kind, and must be sent from the Deterministic
  // Deployment Proxy to the expected `CREATE2` address.
  return (
    nextAccountAccess.kind === AccountAccessKind.Create &&
    nextAccountAccess.accessor === DETERMINISTIC_DEPLOYMENT_PROXY_ADDRESS &&
    nextAccountAccess.account === create2Address
  )
}

/**
 * Returns an array containing all of the nested contract deployments in an `AccountAccess`. This
 * function finds nested contract deployments by iterating through the `AccountAccess` objects that
 * occur after the `AccountAccess`.
 *
 * @param nextAccountAccesses An array of all the `AccountAccess` objects that occur after the
 * current `AccountAccess`. The current `AccountAccess` is not included in this array.
 *
 * @returns An object containing the parsed contract deployments (which each correspond to a fully
 * qualified name) and the unlabeled contract deployments (which don't correspond to a fully
 * qualified name).
 */
export const parseNestedContractDeployments = (
  nested: Array<AccountAccess>,
  configArtifacts: ConfigArtifacts
): {
  parsedContracts: ActionInput['contracts']
  unlabeled: NetworkConfig['unlabeledContracts']
} => {
  const parsedContracts: Array<ParsedContractDeployment> = []
  const unlabeled: NetworkConfig['unlabeledContracts'] = []

  // Iterate through the `AccountAccess` elements.
  for (const accountAccess of nested) {
    if (accountAccess.kind === AccountAccessKind.Create) {
      const initCodeWithArgs = accountAccess.data
      const address = accountAccess.account

      const fullyQualifiedName = findFullyQualifiedNameForInitCode(
        initCodeWithArgs,
        configArtifacts
      )

      if (fullyQualifiedName) {
        parsedContracts.push({
          address,
          initCodeWithArgs,
          fullyQualifiedName,
        })
      } else {
        unlabeled.push({
          address,
          initCodeWithArgs,
        })
      }
    }
  }

  return { parsedContracts, unlabeled }
}

/**
 * Checks that our dependencies are installed with the correct versions including:
 * NodeJS >= v16.16
 * Foundry (some commit that includes all our changes to the state diff)
 * Sphinx Library Contracts (whatever version required by CONTRACTS_LIBRARY_VERSION)
 */
export const assertValidVersions = async (
  scriptPath: string,
  targetContract?: string
): Promise<void> => {
  // Validate that the user has a compatible NodeJS version installed
  assertValidNodeVersion()

  // Validate the user's Sphinx dependencies. Specifically, we retrieve the Sphinx contracts library
  // version and empirically check that our changes to Foundry are included in the version they have
  // installed (using create2 factory in scripts, recording the call to the create2 factory, etc).
  const output = await callForgeScriptFunction<{
    libraryVersion: { value: string }
    forkInstalled: { value: string }
  }>(
    scriptPath,
    'sphinxValidate()',
    ['--always-use-create-2-factory'],
    undefined,
    targetContract
  )

  const libraryVersion = output.returns.libraryVersion.value
    // If the version number contains any quotes we remove them
    .replace(/"/g, '');
  const forkInstalled = output.returns.forkInstalled.value

  if (libraryVersion !== CONTRACTS_LIBRARY_VERSION) {
    throw Error(
      `The version of the Sphinx library contracts does not match the Sphinx plugin version. Please\n` +
        `update the library contracts by running the command:\n` +
        `npx sphinx install`
    )
  }

  if (forkInstalled === 'false') {
    throw new Error(
      `Detected invalid Foundry version. Please update to the latest version of Foundry by running:\n` +
        `foundryup`
    )
  }
}

export const isDeploymentInfo = (obj: any): obj is DeploymentInfo => {
  return (
    typeof obj === 'object' &&
    obj !== null &&
    isNormalizedAddress(obj.safeAddress) &&
    isNormalizedAddress(obj.moduleAddress) &&
    typeof obj.requireSuccess === 'boolean' &&
    isNormalizedAddress(obj.executorAddress) &&
    typeof obj.nonce === 'string' &&
    typeof obj.chainId === 'string' &&
    typeof obj.blockGasLimit === 'string' &&
    typeof obj.safeInitData === 'string' &&
    isInternalSphinxConfig(obj.newConfig) &&
    isExecutionMode(obj.executionMode) &&
    isInitialChainState(obj.initialState) &&
    typeof obj.arbitraryChain === 'boolean' &&
    typeof obj.sphinxLibraryVersion === 'string' &&
    Array.isArray(obj.accountAccesses) &&
    obj.accountAccesses.every(isParsedAccountAccess) &&
    Array.isArray(obj.gasEstimates) &&
    obj.gasEstimates.every((e) => typeof e === 'string')
  )
}

const isInternalSphinxConfig = (obj: any): obj is InternalSphinxConfig => {
  return (
    typeof obj === 'object' &&
    obj !== null &&
    typeof obj.projectName === 'string' &&
    Array.isArray(obj.mainnets) &&
    obj.mainnets.every((m) => typeof m === 'string') &&
    Array.isArray(obj.testnets) &&
    obj.testnets.every((t) => typeof t === 'string')
  )
}

const isExecutionMode = (obj: any): obj is ExecutionMode => {
  return Object.values(ExecutionMode).includes(obj)
}

const isInitialChainState = (obj: any): obj is InitialChainState => {
  return (
    typeof obj === 'object' &&
    obj !== null &&
    typeof obj.isSafeDeployed === 'boolean' &&
    typeof obj.isModuleDeployed === 'boolean' &&
    typeof obj.isExecuting === 'boolean'
  )
}

const isParsedAccountAccess = (obj: any): obj is ParsedAccountAccess => {
  return (
    typeof obj === 'object' &&
    obj !== null &&
    isAccountAccess(obj.root) &&
    Array.isArray(obj.nested) &&
    obj.nested.every(isAccountAccess)
  )
}

const isAccountAccess = (obj: any): obj is AccountAccess => {
  return (
    typeof obj === 'object' &&
    obj !== null &&
    typeof obj.chainInfo === 'object' &&
    obj.chainInfo !== null &&
    typeof obj.chainInfo.forkId === 'string' &&
    typeof obj.chainInfo.chainId === 'string' &&
    Object.values(AccountAccessKind).includes(obj.kind) &&
    typeof obj.account === 'string' &&
    typeof obj.accessor === 'string' &&
    typeof obj.initialized === 'boolean' &&
    typeof obj.oldBalance === 'string' &&
    typeof obj.newBalance === 'string' &&
    typeof obj.deployedCode === 'string' &&
    typeof obj.value === 'string' &&
    typeof obj.data === 'string' &&
    typeof obj.reverted === 'boolean' &&
    Array.isArray(obj.storageAccesses) &&
    obj.storageAccesses.every(isStorageAccess)
  )
}

const isStorageAccess = (
  obj: any
): obj is AccountAccess['storageAccesses'][number] => {
  return (
    typeof obj === 'object' &&
    obj !== null &&
    typeof obj.account === 'string' &&
    typeof obj.slot === 'string' &&
    typeof obj.isWrite === 'boolean' &&
    typeof obj.previousValue === 'string' &&
    typeof obj.newValue === 'string' &&
    typeof obj.reverted === 'boolean'
  )
}

/**
 * Parses the parameters to `--sig` specified by the user. We attempt to maintain the exact same
 * interface as Foundry's `--sig` parameter to make it easy for users to migrate their Forge Scripts
 * to Sphinx.
 *
 * @returns { string } The 0x-prefixed raw calldata.
 */
export const parseScriptFunctionCalldata = async (
  sig: Array<string>,
  spawnAsyncFunction: typeof spawnAsync = spawnAsync
): Promise<string> => {
  if (sig.length === 0) {
    throw new Error(SigCalledWithNoArgsErrorMessage)
  }

  // Differentiate between a function signature and raw calldata by checking if the first parameter
  // has parentheses (e.g. `run()`). This is a reasonable approach because Foundry doesn't allow
  // function signatures without parentheses. For example, specifying `run` would cause an error.
  if (hasParentheses(sig[0])) {
    // Use `cast` to get the function selector, which is a four-byte hex string, as well as the ABI
    // encoded arguments. We use `cast` to provide an identical interface for the `--sig` parameter.
    // Using EthersJS doesn't work because it can't parse stringified function arguments, like
    // `(2, 3)`, which `cast` interprets correctly as a struct's arguments.
    const selectorOutput = await spawnAsyncFunction(`cast`, ['sig', sig[0]])
    if (selectorOutput.code !== 0) {
      // The `stdout` is an empty string, so we throw the error using the `stderr`.
      throw new Error(selectorOutput.stderr)
    }

    const abiEncodeOutput = await spawnAsyncFunction(`cast`, [
      'abi-encode',
      ...sig,
    ])
    if (abiEncodeOutput.code !== 0) {
      // The `stdout` is an empty string, so we throw the error using the `stderr`.
      throw new Error(abiEncodeOutput.stderr)
    }

    // Remove the trailing `\n` from the strings.
    const trimmedSelector = selectorOutput.stdout.replace(/\n/g, '')
    const trimmedAbiEncodedData = abiEncodeOutput.stdout.replace(/\n/g, '')

    const calldata = ethers.concat([trimmedSelector, trimmedAbiEncodedData])
    return calldata
  } else if (sig.length === 1) {
    // Check if the user entered raw calldata.

    // Remove leading and trailing quotes. For some reason, Foundry allows raw calldata wrapped in
    // an arbitrary number of strings, e.g. """"0x1234"""".
    const trimmed = trimQuotes(sig[0])
    // Add an 0x-prefix if it doesn't already exist. Foundry allows calldata without an 0x-prefix,
    // but we need to add the prefix it to continue the validation.
    const with0x = add0x(trimmed)

    if (isDataHexString(with0x)) {
      return add0x(trimmed)
    }
  }
  throw new Error(InvalidFirstSigArgumentErrorMessage)
}

export const validateProposalNetworks = async (
  cliNetworks: Array<string>,
  configTestnets: Array<string>,
  configMainnets: Array<string>,
  rpcEndpoints: FoundryToml['rpcEndpoints']
): Promise<{ rpcUrls: Array<string>; isTestnet: boolean }> => {
  if (cliNetworks.length === 0) {
    throw new Error(`Expected at least one network, but none were supplied.`)
  }

  let resolvedNetworks: Array<string>
  if (cliNetworks.length === 1 && cliNetworks[0] === 'mainnets') {
    if (configMainnets.length === 0) {
      throw new Error(
        `Your 'sphinxConfig.mainnets' array must contain at least one network when\n` +
          `you use the '--mainnets' flag.`
      )
    }

    resolvedNetworks = configMainnets
  } else if (cliNetworks.length === 1 && cliNetworks[0] === 'testnets') {
    if (configTestnets.length === 0) {
      throw new Error(
        `Your 'sphinxConfig.testnets' array must contain at least one network when\n` +
          `you use the '--testnets' flag.`
      )
    }
    resolvedNetworks = configTestnets
  } else {
    resolvedNetworks = cliNetworks
  }

  const networkPromises = resolvedNetworks.map(async (network) => {
    const rpcUrl = rpcEndpoints[network]
    if (!rpcUrl) {
      return { type: 'missingEndpoint', network }
    }

    const provider = new SphinxJsonRpcProvider(rpcUrl)
    let networkInfo: ethers.Network
    try {
      networkInfo = await provider.getNetwork()
    } catch {
      return { type: 'failedRequest', network }
    }

    const sphinxNetwork = SPHINX_NETWORKS.find(
      (supportedNetwork) => supportedNetwork.chainId === networkInfo.chainId
    )

    if (!sphinxNetwork) {
      return {
        type: 'unsupported',
        network,
        chainId: networkInfo.chainId.toString(),
      }
    }

    return {
      type: 'valid',
      rpcUrl,
      network,
      networkType: sphinxNetwork.networkType,
    }
  })

  const results = await Promise.allSettled(networkPromises)

  const missingEndpoint: Array<string> = []
  const failedRequest: Array<string> = []
  const unsupported: Array<{ chainId: string; networkName: string }> = []
  const localNetwork: Array<string> = []
  const valid: Array<{
    network: string
    networkType: NetworkType
    rpcUrl: string
  }> = []
  for (const result of results) {
    if (result.status === 'rejected') {
      throw new Error(`Promise was rejected. Should never happen.`)
    }

    const value = result.value
    if (value.type === 'missingEndpoint') {
      missingEndpoint.push(value.network)
    } else if (value.type === 'failedRequest') {
      console.log(value.rpcUrl)
      failedRequest.push(value.network)
    } else if (value.type === 'unsupported') {
      // Narrow the TypeScript type.
      if (value.chainId === undefined) {
        throw new Error(`Chain ID is undefined. Should never happen.`)
      }
      unsupported.push({ networkName: value.network, chainId: value.chainId })
    } else if (value.type === 'localNetwork') {
      localNetwork.push(value.network)
    } else if (value.type === 'valid') {
      const { rpcUrl, network, networkType } = value
      // Narrow the TypeScript types.
      if (
        rpcUrl === undefined ||
        network === undefined ||
        networkType === undefined
      ) {
        throw new Error(`Network field(s) are undefined. Should never happen.`)
      }
      valid.push({ rpcUrl, network, networkType })
    } else {
      throw new Error(`Unknown result: ${value.type}. Should never happen.`)
    }
  }

  if (missingEndpoint.length > 0) {
    throw new Error(getMissingEndpointErrorMessage(missingEndpoint))
  }

  if (failedRequest.length > 0) {
    throw new Error(getFailedRequestErrorMessage(failedRequest))
  }

  if (unsupported.length > 0) {
    throw new Error(getUnsupportedNetworkErrorMessage(unsupported))
  }

  // Check if the array contains a mix of test networks and production networks. We check this after
  // resolving the promises above because we need to know all of the network types.
  const networkTypes = valid.map(({ networkType }) => networkType)
  const isMixed = isArrayMixed(networkTypes)
  if (isMixed) {
    throw new Error(getMixedNetworkTypeErrorMessage(valid))
  }

  // Check if the array contains all testnets or production networks. We know that the array doesn't
  // contain mixed network types because we already checked this.
  const isTestnet = networkTypes.every(
    (networkType) => networkType === 'Testnet'
  )

  if (cliNetworks.length === 1 && cliNetworks[0] === 'mainnets' && isTestnet) {
    throw new Error(SphinxConfigMainnetsContainsTestnetsErrorMessage)
  } else if (
    cliNetworks.length === 1 &&
    cliNetworks[0] === 'testnets' &&
    !isTestnet
  ) {
    throw new Error(SphinxConfigTestnetsContainsMainnetsErrorMessage)
  }

  const rpcUrls = valid.map(({ rpcUrl }) => rpcUrl)
  return { rpcUrls, isTestnet }
}

export const assertContractSizeLimitNotExceeded = (
  accountAccesses: Array<ParsedAccountAccess>,
  configArtifacts: ConfigArtifacts
): void => {
  const flat = accountAccesses.flatMap((access) => [
    access.root,
    ...access.nested,
  ])

  const tooLarge: Array<{ address: string; fullyQualifiedName?: string }> = []

  for (const access of flat) {
    if (
      access.kind === AccountAccessKind.Create &&
      exceedsContractSizeLimit(access.deployedCode)
    ) {
      const initCodeWithArgs = access.data
      // Find the fully qualified name if it exists. This call returns `undefined` if the contract
      // is unlabeled.
      const fullyQualifiedName = findFullyQualifiedNameForInitCode(
        initCodeWithArgs,
        configArtifacts
      )

      tooLarge.push({ address: access.account, fullyQualifiedName })
    }
  }

  if (tooLarge.length > 0) {
    throw new Error(contractsExceedSizeLimitErrorMessage(tooLarge))
  }
}

export const getCurrentGitCommitHash = (): string | null => {
  let commitHash: string
  try {
    // Get the current git commit. We call this command with "2>/dev/null" to discard the `stderr`.
    // If we don't discard the `stderr`, the following statement would be displayed to the user if
    // they aren't in a git repository:
    //
    // "fatal: not a git repository (or any of the parent directories): .git".
    commitHash = execSync('git rev-parse HEAD 2>/dev/null').toString().trim()
  } catch {
    return null
  }

  if (commitHash.length !== 40) {
    throw new Error(`Git commit hash is an unexpected length: ${commitHash}`)
  }

  return commitHash
}
