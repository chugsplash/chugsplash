import { utils } from 'ethers'
import { CustomChain } from '@nomiclabs/hardhat-etherscan/dist/src/types'

export const CONTRACT_SIZE_LIMIT = 24576 // bytes

export const WEBSITE_URL = `https://chugsplash.io`

// Etherscan constants
export const customChains: CustomChain[] = []

export const EXECUTION_BUFFER_MULTIPLIER = 2
export type Integration = 'hardhat' | 'foundry'

export type Keyword = '{preserve}' | '{gap}'
type Keywords = {
  preserve: Keyword
  gap: Keyword
}

export const keywords: Keywords = {
  preserve: '{preserve}',
  gap: '{gap}',
}

export const REMOTE_EXECUTOR_ROLE = utils.keccak256(
  utils.toUtf8Bytes('REMOTE_EXECUTOR_ROLE')
)

export const CALLER_ROLE = utils.keccak256(utils.toUtf8Bytes('CALLER_ROLE'))

export const MANAGED_PROPOSER_ROLE = utils.keccak256(
  utils.toUtf8Bytes('MANAGED_PROPOSER_ROLE')
)

export const CURRENT_CHUGSPLASH_MANAGER_VERSION = {
  major: 1,
  minor: 0,
  patch: 0,
}

export const LAYERZERO_ENDPOINT_ADDRESSES = {
  5: {
    address: '0xbfD2135BFfbb0B5378b56643c2Df8a87552Bfa23',
    lzChainId: 10121,
  },
  420: {
    address: '0xae92d5aD7583AD66E49A0c67BAd18F6ba52dDDc1',
    lzChainId: 10132,
  },
}
