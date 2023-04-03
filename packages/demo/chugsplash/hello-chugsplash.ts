import { UserChugSplashConfig } from '@chugsplash/core'
import { constants } from 'ethers'

const config: UserChugSplashConfig = {
  options: {
    organizationID: constants.HashZero,
    projectName: 'Hello ChugSplash',
    organizationID: constants.HashZero,
  },
  contracts: {
    MyFirstContract: {
      contract: 'HelloChugSplash',
      unsafeAllowEmptyPush: true,
      variables: {
        number: 1,
        stored: true,
        storageName: 'First',
        otherStorage: '0x1111111111111111111111111111111111111111',
      },
    },
  },
}

export default config
