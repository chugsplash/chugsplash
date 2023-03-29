import { UserChugSplashConfig } from '@chugsplash/core'

const config: UserChugSplashConfig = {
  // Configuration options for the project:
  options: {
    projectName: 'Variable Validation',
  },
  contracts: {
    VariableValidation: {
      contract: 'VariableValidation',
      constructorArgs: {},
      variables: {
        arrayInt8: [0, 1, 2],
        int8OutsideRange: 255,
        uint8OutsideRange: 256,
        intAddress: 1,
        arrayAddress: ['0x00000000'],
        shortAddress: '0x00000000',
        intBytes32: 1,
        arrayBytes32: [1],
        shortBytes32: '0x00000000',
        oddStaticBytes: '0xabcdefghijklmno',
        oddDynamicBytes: '0xabcde',
        longBytes8: '0x' + '11'.repeat(32),
        malformedBytes16: '11'.repeat(16),
        intBoolean: 1,
        stringBoolean: 'true',
        arrayBoolean: [true, false],
        oversizedArray: [1, 2, 3],
        oversizedNestedArray: [
          [1, 2],
          [1, 2],
          [1, 2],
        ],
        invalidBoolArray: ['hello', 'world'],
        invalidBytes32Array: [1, 2],
        invalidAddressArray: ['0x00000000', '0x00000000'],
        invalidStringStringMapping: {
          testKey: 1,
        },
        invalidStringIntMapping: {
          testKey: true,
        },
        invalidNestedStringIntBoolMapping: {
          testKey: {
            testKey: true,
          },
        },
        // variables that are not in the contract
        extraVar: 214830928,
        anotherExtraVar: [],
      },
    },
  },
}

export default config
