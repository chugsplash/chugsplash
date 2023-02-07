# @chugsplash/executor

## 0.5.3

### Patch Changes

- fdb9e62: Set executor default port
- ed17785: Updates the executor to use the latest version of BaseServiceV2.
- Updated dependencies [8c88808]
- Updated dependencies [dfa0381]
- Updated dependencies [1b08f02]
- Updated dependencies [4029daf]
- Updated dependencies [a37d5c3]
- Updated dependencies [b343641]
  - @chugsplash/core@0.5.0

## 0.5.2

### Patch Changes

- 5a135ec: Fix issue verifying ChugSplash contracts

## 0.5.1

### Patch Changes

- 48088b2: Add timeout on analytics functions
- Updated dependencies [48088b2]
  - @chugsplash/core@0.4.1

## 0.5.0

### Minor Changes

- 8df582d: Fix(pg): Refactor tasks to remove dependencies on hardhat

### Patch Changes

- 9edf09b: Fix bug where executor wasn't detecting old approvals
- 57a367d: Fix issue where executor always tries to execute locally
- 1cbd07b: Set `strictNullChecks` to true for TypeScript
- c379fb6: Use artifact paths object instead of inferring artifacts
- 60d7adc: Make executors permissioned
- 8df582d: Feat(core): Add support for local analytics
- 40f0d0a: Add OpenZeppelin storage slot checker
- Updated dependencies [ad46bbc]
- Updated dependencies [042541b]
- Updated dependencies [c8664a2]
- Updated dependencies [57a367d]
- Updated dependencies [1cbd07b]
- Updated dependencies [c379fb6]
- Updated dependencies [ba517ad]
- Updated dependencies [2e41b30]
- Updated dependencies [60d7adc]
- Updated dependencies [f14cc8d]
- Updated dependencies [8df582d]
- Updated dependencies [deca63d]
- Updated dependencies [cb3a70d]
- Updated dependencies [d481925]
- Updated dependencies [2b8af04]
- Updated dependencies [6c07d41]
- Updated dependencies [8df582d]
- Updated dependencies [0443459]
- Updated dependencies [40f0d0a]
- Updated dependencies [2201f3a]
  - @chugsplash/core@0.4.0

## 0.4.14

### Patch Changes

- 2267ec4: Bump versions
- Updated dependencies [2267ec4]
  - @chugsplash/core@0.3.24

## 0.4.13

### Patch Changes

- 7cd5e1b: Add blockchain network parameter to execution event
- d6984ec: Override transaction gas prices to use EIP-1559 if supported by the network
- 532d586: Support defining executor port with environment variable
- Updated dependencies [d6984ec]
  - @chugsplash/core@0.3.23

## 0.4.12

### Patch Changes

- 1cb43e7: Fix Etherscan bug that was caused by an incorrect calculation of implementation addresses
- a60020a: Remove Infura as RPC URL service
- 64e57d6: Better support for deploying containerized executor with Terraform
- Updated dependencies [1cb43e7]
- Updated dependencies [acfe88d]
- Updated dependencies [fdf512b]
  - @chugsplash/core@0.3.22

## 0.4.11

### Patch Changes

- 74a61c0: Change deployment process so that ChugSplash addresses are calculated based on multisig address
- 2dbf187: Change @nomiclabs/hardhat-etherscan from devDependency to dependency
- dba31f7: Write canonical config to file system when using executing bundles locally
- Updated dependencies [74a61c0]
- Updated dependencies [baf3ac1]
- Updated dependencies [89cd352]
- Updated dependencies [dba31f7]
- Updated dependencies [c9eeb47]
  - @chugsplash/core@0.3.21

## 0.4.10

### Patch Changes

- 921f917: Improved logs for funding and post-execution actions
- d8554c0: Prefix logs with [ChugSplash]
- 780a395: Standardize logger messages
- 335dfc7: Adds more logs to the ChugSplash setup process in the executor
- Updated dependencies [3f6cabd]
- Updated dependencies [921f917]
- Updated dependencies [d8554c0]
- Updated dependencies [780a395]
- Updated dependencies [335dfc7]
- Updated dependencies [ba24573]
- Updated dependencies [276d5ea]
  - @chugsplash/core@0.3.20

## 0.4.9

### Patch Changes

- 52d0556: Change the ContractConfig's "address" field to "proxy"
- 65bc432: Execution gas cost estimation bug fixes
- e7ae731: Improve execution cost estimation
- Updated dependencies [52d0556]
- Updated dependencies [65bc432]
- Updated dependencies [38c62b5]
- Updated dependencies [e7ae731]
- Updated dependencies [2652df5]
  - @chugsplash/core@0.3.19

## 0.4.8

### Patch Changes

- d7fff20: Several improvements / bug fixes discovered when deploying on Optimism's devnet.
- 7e8dd1e: Removes the projectOwner from the ChugSplash config
- Updated dependencies [d7fff20]
- Updated dependencies [7e8dd1e]
  - @chugsplash/core@0.3.17

## 0.4.7

### Patch Changes

- d458d93: Wrap Etherscan verification attempts in try/catch blocks
- 16348b2: Make the ChugSplashRegistry proxy's address deterministic
- 0b52005: Remove redundant Proxy verification attempts. Link ChugSplashManager proxy with its implementation on Etherscan.
- c5ec8e4: Replace incorrect use of the `getDefaultProxyAddress` function
- ee3ae13: Remove HRE dependency from execution logic and move to core package
- fb1168f: Make executor most robust to errors and cancelled bundles. Ensure that executor receives payment.
- f217221: Use the executor to deploy and verify the ChugSplash predeployed contracts
- 780e54f: Submit the minimum compiler input necessary to verify contracts on Etherscan
- da5cb35: Move the logic that initializes the ChugSplash predeploys into the executor.
- 5406b7b: Update canonical ChugSplash config type usage
- Updated dependencies [74da4d0]
- Updated dependencies [7a1737e]
- Updated dependencies [c32f23e]
- Updated dependencies [16348b2]
- Updated dependencies [fd5177e]
- Updated dependencies [e1af6e3]
- Updated dependencies [3572abd]
- Updated dependencies [ec87d11]
- Updated dependencies [c5ec8e4]
- Updated dependencies [9ebc63c]
- Updated dependencies [ee3ae13]
- Updated dependencies [0c30af0]
- Updated dependencies [6276a86]
- Updated dependencies [fb1168f]
- Updated dependencies [6a2644e]
- Updated dependencies [64463f1]
- Updated dependencies [fc8cfd3]
- Updated dependencies [f217221]
- Updated dependencies [780e54f]
- Updated dependencies [ec41164]
- Updated dependencies [da5cb35]
- Updated dependencies [5406b7b]
  - @chugsplash/core@0.3.16

## 0.4.6

### Patch Changes

- 7c367b4: Updates the chugsplash-execute task

## 0.4.5

### Patch Changes

- 8323afb: Add deployment artifact generation on the user's side

## 0.4.4

### Patch Changes

- 15ebe78: Hardhat task bug fixes and improvements

## 0.4.3

### Patch Changes

- b653177: Remove parallel deployments due to bug on live networks

## 0.4.2

### Patch Changes

- 61f8ca1: Use environment variable `HARDHAT_NETWORK` to determine executor's network
- f199dff: Verify proxy as part of ChugSplash config verification
- a43e0e3: Add Docker configuration for executor
- 12a7f34: Improve execution speed with parallelization
- 07a32f6: Run the executor using the HRE executable

## 0.4.1

### Patch Changes

- 7b33791: Integrate etherscan verification into executor

## 0.4.0

### Minor Changes

- 21df9d7: Add Etherscan verification in executor

### Patch Changes

- 071d867: Implemented minimal standalone executor
- afe99ad: Verify ChugSpash predeploy contracts

## 0.3.2

### Patch Changes

- 03d557c: Bump all versions

## 0.3.1

### Patch Changes

- 557e3bd: Bump versions

## 0.3.0

### Minor Changes

- 52c7f6c: Bump all packages

## 0.2.0

### Minor Changes

- 4c73fc1: Updates ChugSplash executor to include basic execution flow.
