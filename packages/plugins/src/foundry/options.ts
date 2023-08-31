import { exec } from 'child_process'
import { join, resolve } from 'path'
import { promisify } from 'util'

export type FoundryToml = {
  artifactFolder: string
  buildInfoFolder: string
  deploymentFolder: string
  compilerConfigFolder: string
  cachePath: string
  storageLayout: boolean
  gasEstimates: boolean
  rpcEndpoints: { [chainAlias: string]: string }
}

export const cleanPath = (dirtyPath: string) => {
  let cleanQuotes = dirtyPath.replace(/'/g, '')
  cleanQuotes = cleanQuotes.replace(/"/g, '')
  return cleanQuotes.trim()
}

export const resolvePaths = (outPath: string, buildInfoPath: string) => {
  const artifactFolder = resolve(outPath)
  const buildInfoFolder = resolve(buildInfoPath)
  const deploymentFolder = resolve('deployments')
  const compilerConfigFolder = resolve('.compiler-configs')

  return {
    artifactFolder,
    buildInfoFolder,
    deploymentFolder,
    compilerConfigFolder,
  }
}

/**
 * @notice Gets fields from the user's foundry.toml file.
 *
 * Note that most of these fields can be overridden via a `FOUNDRY_` or `DAPP_` environment variable
 * (source: https://book.getfoundry.sh/reference/config/overview#environment-variables). These env
 * variables are injected into the output of `forge config` automatically, so there's no additional
 * parsing needed to support them.
 */
export const getFoundryConfigOptions = async (): Promise<FoundryToml> => {
  const execAsync = promisify(exec)

  const forgeConfigOutput = await execAsync('forge config --json')
  const forgeConfig = JSON.parse(forgeConfigOutput.stdout)

  const buildInfoPath =
    forgeConfig.build_info_path ?? join(forgeConfig.out, 'build-info')

  const cachePath = forgeConfig.cache_path
  const rpcEndpoints = parseRpcEndpoints(forgeConfig.rpc_endpoints)

  // Since foundry force recompiles after changing the foundry.toml file, we can assume that the contract
  // artifacts will contain the necessary info as long as the config includes the expected options
  const storageLayout = forgeConfig.extra_output.includes('storageLayout')
  const gasEstimates = forgeConfig.extra_output.includes('evm.gasEstimates')

  return {
    ...resolvePaths(forgeConfig.out, buildInfoPath),
    storageLayout,
    gasEstimates,
    cachePath,
    rpcEndpoints,
  }
}

/**
 * @notice Parses the RPC endpoings in a foundry.toml file.
 *
 * @param rpcEndpoints The unparsed RPC endpoints object. The value of an endpoint can be either an
 * RPC URL or an environment variable that contains an RPC URL. An example of an environment
 * variable is "${RPC_ENDPOINT}}". Whitespace is allowed, so "   ${  RPC_ENDPOINT   }  " is also
 * valid.
 *
 * @returns An object where the keys are the chain aliases and the values are the RPC URLs. Note
 * that if the value of an RPC endpoint is an environment variable, but the environment variable
 * does not exist, then the endpoint will not be included in the returned object.
 */
export const parseRpcEndpoints = (rpcEndpoints: {
  [chainAlias: string]: string
}): { [chainAlias: string]: string } => {
  const result: { [key: string]: string } = {}
  for (const key in rpcEndpoints) {
    if (rpcEndpoints.hasOwnProperty(key)) {
      // Removes whitespace at the beginning and end of the string
      const trimmed = rpcEndpoints[key].trim()

      result[key] = trimmed.replace(/\$\{((\w|\s)+)\}/g, (_, envVar) => {
        return process.env[envVar.trim()] || ''
      })
    }
  }
  return result
}
