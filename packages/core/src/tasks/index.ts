import process from 'process'

import * as dotenv from 'dotenv'
import Hash from 'ipfs-only-hash'
import { create } from 'ipfs-http-client'

import {
  BuildInfoInputs,
  ConfigArtifacts,
  CompilerConfig,
  ParsedConfig,
} from '../config/types'
import { getMinimumCompilerInput } from '../languages'
import { SphinxBundles, HumanReadableAction } from '../actions'

// Load environment variables from .env
dotenv.config()

export const sphinxCommitAbstractSubtask = async (
  parsedConfig: ParsedConfig,
  commitToIpfs: boolean,
  configArtifacts: ConfigArtifacts,
  ipfsUrl?: string
): Promise<{
  configUri: string
  compilerConfig: CompilerConfig
}> => {
  const sphinxInputs: Array<BuildInfoInputs> = []

  for (const actionInput of parsedConfig.actionInputs) {
    for (const address of Object.keys(actionInput.contracts)) {
      const { fullyQualifiedName } = actionInput.contracts[address]

      const { buildInfo, artifact } = configArtifacts[fullyQualifiedName]

      const prevSphinxInput = sphinxInputs.find(
        (input) => input.solcLongVersion === buildInfo.solcLongVersion
      )

      const { language, settings, sources } = getMinimumCompilerInput(
        buildInfo.input,
        artifact.metadata
      )

      if (prevSphinxInput === undefined) {
        const sphinxInput: BuildInfoInputs = {
          solcVersion: buildInfo.solcVersion,
          solcLongVersion: buildInfo.solcLongVersion,
          id: buildInfo.id,
          input: {
            language,
            settings,
            sources,
          },
        }
        sphinxInputs.push(sphinxInput)
      } else {
        prevSphinxInput.input.sources = {
          ...prevSphinxInput.input.sources,
          ...sources,
        }
      }
    }
  }

  const compilerConfig: CompilerConfig = {
    ...parsedConfig,
    inputs: sphinxInputs,
  }

  const ipfsData = JSON.stringify(compilerConfig, null, 2)

  let ipfsHash
  if (!commitToIpfs) {
    // Get the IPFS hash without publishing anything on IPFS.
    ipfsHash = await Hash.of(ipfsData)
  } else if (ipfsUrl) {
    const ipfs = create({
      url: ipfsUrl,
    })
    ipfsHash = (await ipfs.add(ipfsData)).path
  } else if (process.env.IPFS_PROJECT_ID && process.env.IPFS_API_KEY_SECRET) {
    const projectCredentials = `${process.env.IPFS_PROJECT_ID}:${process.env.IPFS_API_KEY_SECRET}`
    const ipfs = create({
      host: 'ipfs.infura.io',
      port: 5001,
      protocol: 'https',
      headers: {
        authorization: `Basic ${Buffer.from(projectCredentials).toString(
          'base64'
        )}`,
      },
    })
    ipfsHash = (await ipfs.add(ipfsData)).path
  } else {
    throw new Error(
      `To commit to IPFS, you must first setup an IPFS project with
Infura: https://app.infura.io/. Once you've done this, copy and paste the following
variables into your .env file:

IPFS_PROJECT_ID: ...
IPFS_API_KEY_SECRET: ...
        `
    )
  }

  const configUri = `ipfs://${ipfsHash}`

  return { configUri, compilerConfig }
}

export const getProjectBundleInfo = async (
  parsedConfig: ParsedConfig,
  configArtifacts: ConfigArtifacts
): Promise<{
  configUri: string
  compilerConfig: CompilerConfig
}> => {
  const { configUri, compilerConfig } = await sphinxCommitAbstractSubtask(
    parsedConfig,
    false,
    configArtifacts
  )

  return { configUri, compilerConfig }
}
