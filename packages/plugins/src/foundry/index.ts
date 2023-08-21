import {
  getSphinxRegistryReadOnly,
  getPreviousConfigUri,
  ensureSphinxInitialized,
  SphinxJsonRpcProvider,
} from '@sphinx-labs/core'
import { ethers } from 'ethers'

const args = process.argv.slice(2)
const command = args[0]

;(async () => {
  switch (command) {
    case 'getPreviousConfigUri': {
      const rpcUrl = args[1]
      const proxyAddress = args[2]
      const provider = new SphinxJsonRpcProvider(rpcUrl)
      const registry = await getSphinxRegistryReadOnly(provider)

      const configUri = await getPreviousConfigUri(
        provider,
        registry,
        proxyAddress
      )

      const exists = configUri !== undefined

      const encodedConfigUri = ethers.AbiCoder.defaultAbiCoder().encode(
        ['bool', 'string'],
        [exists, configUri ?? '']
      )

      process.stdout.write(encodedConfigUri)
      break
    }
    case 'deployOnAnvil': {
      const rpcUrl = args[1]
      const provider = new SphinxJsonRpcProvider(rpcUrl)
      const wallet = new ethers.Wallet(
        '0xdbda1821b80551c9d65939329250298aa3472ba22feea921c0cf5d620ea67b97',
        provider
      )

      try {
        await ensureSphinxInitialized(provider, wallet, [], [], [])
      } catch (e) {
        // The 'could not detect network' error will occur on the in-process Anvil node,
        // since we can't access it in TypeScript.
        if (!e.reason.includes('could not detect network')) {
          throw e
        }
      }

      break
    }
  }
})()
