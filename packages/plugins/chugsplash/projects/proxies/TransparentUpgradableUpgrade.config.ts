import { UserProjectConfig } from '@chugsplash/core'

import { fetchBuildInfo } from '../../../test/constants'

const projectName = 'TransparentUpgradableToken'

const config: UserProjectConfig = {
  contracts: {
    Token: {
      contract: 'TransparentUpgradableV2',
      variables: {
        newInt: 1,
        originalInt: 1,
        _initialized: 1,
        _initializing: false,
        'ContextUpgradeable:__gap': '{ gap }',
        'OwnableUpgradeable:__gap': '{ gap }',
        _owner: '0x1111111111111111111111111111111111111111',
      },
      address: '0xC469e7aE4aD962c30c7111dc580B4adbc7E914DD',
      kind: 'oz-transparent',
      // We must specify these explicitly because newer versions of OpenZeppelin's Hardhat plugin
      // don't create the Network file in the `.openzeppelin/` folder anymore:
      // https://docs.openzeppelin.com/upgrades-plugins/1.x/network-files#temporary-files
      previousBuildInfo: `artifacts/build-info/${fetchBuildInfo()}`,
      previousFullyQualifiedName:
        'contracts/test/TransparentUpgradableV1.sol:TransparentUpgradableV1',
    },
  },
}

export { config, projectName }