import { UserConfigWithOptions } from '@sphinx-labs/core'

const ownerAddress = '0x9fd58Bf0F2E6125Ffb0CBFa9AE91893Dbc1D5c51'

// Used for testing the website, please do not delete
const config: UserConfigWithOptions = {
  projectName: 'Foundry Deployment',
  options: {
    orgId: 'clku3cgou00002gcb837z3j3j',
    owners: [ownerAddress],
    ownerThreshold: 1,
    testnets: [
      'arbitrum-goerli',
      'gnosis-chiado',
      'maticmum',
      'bnbt',
      'optimism-goerli',
      'goerli',
    ],
    mainnets: ['ethereum', 'optimism'],
    proposers: [ownerAddress],
  },
  contracts: {
    TestContract: {
      contract: 'Stateless',
      kind: 'immutable',
      constructorArgs: {
        _immutableUint: 1,
        _immutableAddress: '0x' + '11'.repeat(20),
      },
    },
  },
}

export default config
