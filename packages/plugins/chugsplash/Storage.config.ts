import { UserChugSplashConfig } from '@chugsplash/core'
import { ethers } from 'ethers'

import { variables, constructorArgs } from '../test/constants'

const projectName = 'My First Project'

const config: UserChugSplashConfig = {
  // Configuration options for the project:
  options: {
    organizationID: ethers.utils.keccak256(
      ethers.utils.toUtf8Bytes(projectName)
    ),
    projectName,
  },
  contracts: {
    MyStorage: {
      contract: 'Storage',
      constructorArgs,
      variables,
    },
    MySimpleStorage: {
      contract: 'SimpleStorage',
      variables: {
        myStorage: '{{ MyStorage }}',
        myStateless: '{{ Stateless }}',
      },
    },
    Stateless: {
      contract: 'Stateless',
      kind: 'no-proxy',
      constructorArgs: {
        _immutableUint: 1,
      },
    },
  },
}

export default config
