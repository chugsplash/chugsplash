import { create, IPFSHTTPClient } from 'ipfs-http-client'

import { HumanReadableActions, SphinxBundles } from '../actions/types'
import {
  callWithTimeout,
  getConfigArtifactsRemote,
  isExtendedFunctionCallActionInput,
} from '../utils'
import {
  CompilerConfig,
  ConfigArtifacts,
  ExtendedDeployContractActionInput,
} from './types'
import { makeBundlesFromConfig } from '../actions/bundle'
import { SemVer } from '../types'

const parseCompilerAction = (
  action: ExtendedDeployContractActionInput | ExtendedDeployContractActionInput
) => {
  action.actionType = BigInt(action.actionType)
  if (isExtendedFunctionCallActionInput(action)) {
    action.nonce = BigInt(action.nonce)
  }

  return action
}

const parseCompilerVersion = (version: SemVer) => {
  version.major = BigInt(version.major)
  version.minor = BigInt(version.minor)
  version.patch = BigInt(version.patch)
  return version
}

// Todo ensure all of the bigints are properly parsed in the compiler config
const parseCompilerConfigBigInts = (config: CompilerConfig) => {
  config.chainId = BigInt(config.chainId)
  config.actionInputs = config.actionInputs.map(parseCompilerAction)
  config.newConfig.threshold = BigInt(config.newConfig.threshold)
  config.newConfig.version = parseCompilerVersion(config.newConfig.version)
  config.initialState.version = parseCompilerVersion(
    config.initialState.version
  )
  return config
}

export const sphinxFetchSubtask = async (args: {
  configUri: string
  ipfsUrl?: string
}): Promise<CompilerConfig> => {
  let config: CompilerConfig
  let ipfs: IPFSHTTPClient
  if (args.ipfsUrl) {
    ipfs = create({
      url: args.ipfsUrl,
    })
  } else if (process.env.IPFS_PROJECT_ID && process.env.IPFS_API_KEY_SECRET) {
    const projectCredentials = `${process.env.IPFS_PROJECT_ID}:${process.env.IPFS_API_KEY_SECRET}`
    ipfs = create({
      host: 'ipfs.infura.io',
      port: 5001,
      protocol: 'https',
      headers: {
        authorization: `Basic ${Buffer.from(projectCredentials).toString(
          'base64'
        )}`,
      },
    })
  } else {
    throw new Error(
      'You must either set your IPFS credentials in an environment file or call this task with an IPFS url.'
    )
  }

  if (args.configUri.startsWith('ipfs://')) {
    const decoder = new TextDecoder()
    let data = ''
    const stream = await ipfs.cat(args.configUri.replace('ipfs://', ''))
    for await (const chunk of stream) {
      // Chunks of data are returned as a Uint8Array. Convert it back to a string
      data += decoder.decode(chunk, { stream: true })
    }
    config = JSON.parse(data)
  } else {
    throw new Error('unsupported URI type')
  }

  // TODO(ryan): Can you document why we need to do this?
  // TODO(ryan): I think the fetch subtask is only called by the remote executor. Do we need to call
  // this anywher in the user-facing logic too?
  return parseCompilerConfigBigInts(config)
}

/**
 * Compiles a remote SphinxBundle from a uri.
 *
 * @param configUri URI of the SphinxBundle to compile.
 * @param provider JSON RPC provider.
 * @returns Compiled SphinxBundle.
 */
export const compileRemoteBundles = async (
  configUri: string
): Promise<{
  bundles: SphinxBundles
  compilerConfig: CompilerConfig
  configArtifacts: ConfigArtifacts
  humanReadableActions: HumanReadableActions
}> => {
  const compilerConfig = await callWithTimeout<CompilerConfig>(
    sphinxFetchSubtask({ configUri }),
    30000,
    'Failed to fetch config file from IPFS'
  )

  const configArtifacts = await getConfigArtifactsRemote(compilerConfig)

  const { bundles, humanReadableActions } = makeBundlesFromConfig(
    compilerConfig,
    configArtifacts
  )
  return {
    bundles,
    compilerConfig,
    configArtifacts,
    humanReadableActions,
  }
}
