import { UserProjectConfig } from '@chugsplash/core'

import {
  invalidConstructorArgsPartOne,
  invalidConstructorArgsPartTwo,
} from '../../../test/constants'

const projectName = 'ConstructorArgValidation'

const config: UserProjectConfig = {
  contracts: {
    ConstructorArgsValidationPartOne: {
      contract: 'ConstructorArgsValidationPartOne',
      kind: 'proxy',
      constructorArgs: {
        ...invalidConstructorArgsPartOne,
        _immutableUint: 1,
      },
    },
    ConstructorArgsValidationPartTwo: {
      contract: 'ConstructorArgsValidationPartTwo',
      kind: 'proxy',
      constructorArgs: {
        ...invalidConstructorArgsPartTwo,
      },
    },
  },
}

export { config, projectName }
