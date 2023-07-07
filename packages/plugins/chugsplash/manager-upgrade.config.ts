import { UserChugSplashConfig } from '@chugsplash/core'

// This config tests an upgrade of the ChugSplashManager's version. We use a config
// with a different owner because otherwise the ChugSplashManager's version will
// be altered for all subsequent tests in the test suite.
const config: UserChugSplashConfig = {
  projects: {
    ManagerUpgrade: {
      contracts: {
        Stateless: {
          contract: 'Stateless',
          kind: 'immutable',
          constructorArgs: {
            _immutableUint: 1,
            _immutableAddress: '{{ Stateless }}',
          },
        },
      },
    },
  },
}

export default config
