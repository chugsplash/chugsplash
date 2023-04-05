import { CompilerInput, SolcBuild } from 'hardhat/types'
import { getCompilersDir } from 'hardhat/internal/util/global-dir'
import {
  CompilerDownloader,
  CompilerPlatform,
} from 'hardhat/internal/solidity/compiler/downloader'
import { providers } from 'ethers'

import { CanonicalChugSplashConfig } from '../../config/types'
import { ChugSplashBundles, makeBundlesFromConfig } from '../../actions'
import {
  CompilerOutputContracts,
  CompilerOutputMetadata,
  CompilerOutputSources,
} from './types'
import { getCanonicalConfigArtifacts } from '../../utils'

export const bundleRemoteSubtask = async (args: {
  provider: providers.Provider
  canonicalConfig: CanonicalChugSplashConfig
}): Promise<ChugSplashBundles> => {
  const { provider, canonicalConfig } = args

  const artifacts = await getCanonicalConfigArtifacts(canonicalConfig)

  return makeBundlesFromConfig(provider, canonicalConfig, artifacts)
}

// Credit: NomicFoundation
// https://github.com/NomicFoundation/hardhat/blob/main/packages/hardhat-core/src/builtin-tasks/compile.ts
export const getSolcBuild = async (solcVersion: string): Promise<SolcBuild> => {
  const compilersCache = await getCompilersDir()

  const compilerPlatform = CompilerDownloader.getCompilerPlatform()
  const downloader = CompilerDownloader.getConcurrencySafeDownloader(
    compilerPlatform,
    compilersCache
  )

  const isCompilerDownloaded = await downloader.isCompilerDownloaded(
    solcVersion
  )

  if (!isCompilerDownloaded) {
    await downloader.downloadCompiler(solcVersion)
  }

  const compiler = await downloader.getCompiler(solcVersion)

  if (compiler !== undefined) {
    return compiler
  }

  const wasmDownloader = CompilerDownloader.getConcurrencySafeDownloader(
    CompilerPlatform.WASM,
    compilersCache
  )

  const isWasmCompilerDownloader = await wasmDownloader.isCompilerDownloaded(
    solcVersion
  )

  if (!isWasmCompilerDownloader) {
    await wasmDownloader.downloadCompiler(solcVersion)
  }

  const wasmCompiler = await wasmDownloader.getCompiler(solcVersion)

  if (wasmCompiler === undefined) {
    throw new Error(`Could not get WASM compiler.`)
  }

  return wasmCompiler
}

/**
 * Returns the minimum compiler input necessary to compile a given source name. All contracts that
 * are imported in the given source must be included in the minimum compiler input.
 *
 * @param fullCompilerInput The full compiler input object.
 * @param fullOutputSources The full compiler output source object.
 * @param sourceName The source name.
 * @returns Minimum compiler input necessary to compile the source name.
 */
export const getMinimumCompilerInput = (
  fullCompilerInput: CompilerInput,
  fullOutputContracts: CompilerOutputContracts,
  sourceName: string,
  contractName: string
): CompilerInput => {
  const contractOutput = fullOutputContracts[sourceName][contractName]
  const metadata: CompilerOutputMetadata =
    typeof contractOutput.metadata === 'string'
      ? JSON.parse(contractOutput.metadata)
      : contractOutput.metadata

  const minimumSources: CompilerInput['sources'] = {}
  for (const newSourceName of Object.keys(metadata.sources)) {
    minimumSources[newSourceName] = fullCompilerInput.sources[newSourceName]
  }

  const { language, settings } = fullCompilerInput
  const minimumCompilerInput: CompilerInput = {
    language,
    settings,
    sources: minimumSources,
  }

  return minimumCompilerInput
}

/**
 * Recursively get the minimum list of source names necessary to compile a given source name. All
 * source names that are referenced in the given source name must be included in this list.
 *
 * @param sourceName The source name.
 * @param fullOutputSources The full compiler output source object.
 * @param contractAstIdsToSourceNames Mapping from contract AST IDs to source names.
 * @param minimumSourceNames Array of minimum source names.
 * @returns
 */
export const getMinimumSourceNames = (
  sourceName: string,
  fullOutputSources: CompilerOutputSources,
  contractAstIdsToSourceNames: { [astId: number]: string },
  minimumSourceNames: string[]
): string[] => {
  // The exported symbols object contains the AST IDs corresponding to the contracts that must be
  // included in the list of minimum source names for the given source.
  const exportedSymbols = fullOutputSources[sourceName].ast.exportedSymbols

  if (exportedSymbols) {
    for (const astIds of Object.values(exportedSymbols)) {
      if (astIds === undefined) {
        continue
      } else if (astIds.length > 1) {
        throw new Error(
          `Detected more than one AST ID for: ${sourceName}. Please report this error.`
        )
      }
      const astId = astIds[0]
      const nextSourceName = contractAstIdsToSourceNames[astId]
      if (!minimumSourceNames.includes(nextSourceName)) {
        minimumSourceNames.push(nextSourceName)
        minimumSourceNames = getMinimumSourceNames(
          nextSourceName,
          fullOutputSources,
          contractAstIdsToSourceNames,
          minimumSourceNames
        )
      }
    }
  }
  return minimumSourceNames
}
