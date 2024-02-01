import {
  isRawCreate2ActionInput,
  isRawFunctionCallActionInput,
  isString,
  ActionInput,
  ConfigArtifacts,
  DeploymentInfo,
  FunctionCallActionInput,
  ParsedConfig,
  RawActionInput,
  RawCreate2ActionInput,
  RawFunctionCallActionInput,
  SphinxActionType,
  networkEnumToName,
  assertValidProjectName,
  ParsedContractDeployment,
  DecodedAction,
  ParsedVariable,
  getAbiEncodedConstructorArgs,
  decodeCall,
} from '@sphinx-labs/core'
import { AbiCoder, ConstructorFragment, ethers } from 'ethers'
import {
  DETERMINISTIC_DEPLOYMENT_PROXY_ADDRESS,
  Operation,
  recursivelyConvertResult,
  getCurrentGitCommitHash,
} from '@sphinx-labs/contracts'

import { FoundrySingleChainDryRun } from './types'
import {
  convertLibraryFormat,
  findFullyQualifiedNameForAddress,
  findFullyQualifiedNameForInitCode,
  findFunctionFragment,
} from './utils'

export const decodeDeploymentInfo = (
  abiEncodedDeploymentInfo: string,
  sphinxPluginTypesInterface: ethers.Interface
): DeploymentInfo => {
  const deploymentInfoFragment = findFunctionFragment(
    sphinxPluginTypesInterface,
    'getDeploymentInfo'
  )

  const deploymentInfoResult = AbiCoder.defaultAbiCoder().decode(
    deploymentInfoFragment.outputs,
    abiEncodedDeploymentInfo
  )

  const { deploymentInfo: deploymentInfoBigInt } = recursivelyConvertResult(
    deploymentInfoFragment.outputs,
    deploymentInfoResult
  ) as any

  const {
    safeAddress,
    moduleAddress,
    executorAddress,
    nonce,
    chainId,
    blockGasLimit,
    initialState,
    executionMode,
    newConfig,
    requireSuccess,
    safeInitData,
    arbitraryChain,
    sphinxLibraryVersion,
  } = deploymentInfoBigInt

  const deploymentInfo: DeploymentInfo = {
    safeAddress,
    moduleAddress,
    safeInitData,
    executorAddress,
    requireSuccess,
    nonce: nonce.toString(),
    chainId: chainId.toString(),
    blockGasLimit: blockGasLimit.toString(),
    initialState: {
      ...initialState,
    },
    executionMode: Number(executionMode),
    newConfig: {
      ...newConfig,
      testnets: newConfig.testnets.map(networkEnumToName),
      mainnets: newConfig.mainnets.map(networkEnumToName),
      threshold: newConfig.threshold.toString(),
      saltNonce: newConfig.saltNonce.toString(),
    },
    arbitraryChain,
    sphinxLibraryVersion,
  }

  assertValidProjectName(deploymentInfo.newConfig.projectName)

  return deploymentInfo
}

export const convertFoundryDryRunToActionInputs = (
  deploymentInfo: DeploymentInfo,
  dryRun: FoundrySingleChainDryRun
): Array<RawActionInput> => {
  const notFromGnosisSafe = dryRun.transactions
    .map((t) => t.transaction.from)
    .filter(isString)
    .filter(
      (from) =>
        // Convert the 'from' field to a checksum address.
        ethers.getAddress(from) !== deploymentInfo.safeAddress
    )
  if (notFromGnosisSafe.length > 0) {
    // The user must broadcast/prank from the Gnosis Safe so that the msg.sender for function calls
    // is the same as it would be in a production deployment.
    throw new Error(
      `Sphinx: Detected transaction(s) in the deployment that weren't sent by the user's Safe contracti.\n` +
        `The 'run()' function must have the 'sphinx' modifier and cannot contain any pranks or broadcasts.\n`
    )
  }

  const actionInputs: Array<RawActionInput> = []
  for (const {
    transaction,
    transactionType,
    additionalContracts,
  } of dryRun.transactions) {
    if (transaction.value !== undefined && transaction.value !== '0x0') {
      console.error(
        `Sphinx does not support sending ETH during deployments. Let us know if you want this feature!`
      )
      process.exit(1)
    }

    if (transactionType === 'CREATE') {
      console.error(
        `Sphinx does not support the 'CREATE' opcode, i.e. 'new MyContract(...)'. Please use CREATE2 or CREATE3 instead.`
      )
      process.exit(1)
    } else {
      if (!transaction.to) {
        throw new Error(
          `Transaction does not have the 'to' field. Should never happen.`
        )
      }

      const to = ethers.getAddress(transaction.to)
      if (transactionType === 'CREATE2') {
        if (to !== DETERMINISTIC_DEPLOYMENT_PROXY_ADDRESS) {
          console.error(
            `Detected unsupported CREATE2 factory. Please use the standard factory at: 0x4e59b44847b379578588920cA78FbF26c0B4956C`
          )
          process.exit(1)
        }

        if (!transaction.data || !transaction.gas) {
          throw new Error(
            `CREATE2 transaction is missing field(s). Should never happen.`
          )
        }

        const salt = ethers.dataSlice(transaction.data, 0, 32)
        const initCodeWithArgs = ethers.dataSlice(transaction.data, 32)
        const create2Address = ethers.getCreate2Address(
          to,
          salt,
          ethers.keccak256(initCodeWithArgs)
        )

        const rawCreate2: RawCreate2ActionInput = {
          to,
          create2Address,
          value: transaction.value ?? '0x0',
          operation: Operation.Call,
          txData: transaction.data,
          initCodeWithArgs,
          actionType: SphinxActionType.CALL.toString(),
          gas: transaction.gas,
          additionalContracts,
          requireSuccess: deploymentInfo.requireSuccess,
        }
        actionInputs.push(rawCreate2)
      } else if (transactionType === 'CALL') {
        if (!transaction.data || !transaction.gas) {
          throw new Error(
            `CALL transaction is missing field(s). Should never happen.`
          )
        }

        const rawCall: RawFunctionCallActionInput = {
          actionType: SphinxActionType.CALL.toString(),
          to,
          value: transaction.value ?? '0x0',
          txData: transaction.data,
          operation: Operation.Call,
          gas: transaction.gas,
          additionalContracts,
          requireSuccess: deploymentInfo.requireSuccess,
        }

        actionInputs.push(rawCall)
      } else {
        throw new Error(`Unknown transaction type: ${transactionType}.`)
      }
    }
  }

  return actionInputs
}

export const makeParsedConfig = (
  deploymentInfo: DeploymentInfo,
  rawInputs: Array<RawActionInput>,
  gasEstimates: Array<string>,
  isSystemDeployed: boolean,
  configArtifacts: ConfigArtifacts,
  libraries: Array<string>,
  dryRunPath: string
): ParsedConfig => {
  const {
    safeAddress,
    moduleAddress,
    nonce,
    chainId,
    blockGasLimit,
    newConfig,
    executionMode,
    initialState,
    safeInitData,
    arbitraryChain,
  } = deploymentInfo

  // Each Merkle leaf must have a gas amount that's at most 80% of the block gas limit. This ensures
  // that it's possible to execute the transaction on-chain. Specifically, there must be enough gas
  // to execute the Sphinx Module's logic, which isn't included in the gas estimate of the Merkle
  // leaf. The 80% was chosen arbitrarily.
  const maxAllowedGasPerLeaf = (BigInt(8) * BigInt(blockGasLimit)) / BigInt(10)

  const parsedActionInputs: Array<ActionInput> = []
  const unlabeledContracts: ParsedConfig['unlabeledContracts'] = []
  // We start with an action index of 1 because the `APPROVE` leaf always has an index of 0, which
  // means the `EXECUTE` leaves start with an index of 1.
  let actionIndex = 1
  for (let i = 0; i < rawInputs.length; i++) {
    const input = rawInputs[i]
    const gas = gasEstimates[i]

    if (BigInt(gas) > maxAllowedGasPerLeaf) {
      throw new Error(
        `Estimated gas for a transaction is too close to the block gas limit.`
      )
    }

    const { parsedContracts, unlabeledAdditionalContracts } =
      parseAdditionalContracts(input, configArtifacts)
    unlabeledContracts.push(...unlabeledAdditionalContracts)

    if (isRawCreate2ActionInput(input)) {
      const fullyQualifiedName = findFullyQualifiedNameForInitCode(
        input.initCodeWithArgs,
        configArtifacts
      )

      const decodedAction = makeCreate2DecodedAction(
        input.create2Address,
        input.initCodeWithArgs,
        configArtifacts,
        fullyQualifiedName
      )

      // If the fully qualified name exists, add the contract deployed via `CREATE2` to the list of
      // parsed contracts.
      if (fullyQualifiedName) {
        parsedContracts.push({
          address: input.create2Address,
          fullyQualifiedName,
          initCodeWithArgs: input.initCodeWithArgs,
        })
      } else {
        // We couldn't find the fully qualified name, so the contract must not belong to a source
        // file. We mark it as unlabeled.
        unlabeledContracts.push({
          address: input.create2Address,
          initCodeWithArgs: input.initCodeWithArgs,
        })
      }

      parsedActionInputs.push({
        contracts: parsedContracts,
        index: actionIndex.toString(),
        decodedAction,
        ...input,
        gas,
      })
    } else if (isRawFunctionCallActionInput(input)) {
      // Find the fully qualified name that corresponds to the `to` address, if such a fully
      // qualified name exists. We'll use the fully qualified name to create the decoded action.
      const fullyQualifiedName = findFullyQualifiedNameForAddress(
        input.to,
        rawInputs,
        configArtifacts
      )

      const decodedAction = makeFunctionCallDecodedAction(
        input.to,
        input.txData,
        configArtifacts,
        dryRunPath,
        fullyQualifiedName
      )

      const callInput: FunctionCallActionInput = {
        contracts: parsedContracts,
        index: actionIndex.toString(),
        decodedAction,
        ...input,
        gas,
      }

      parsedActionInputs.push(callInput)
    } else {
      throw new Error(`Unknown action input type. Should never happen.`)
    }
    actionIndex += 1
  }

  const parsedConfig: ParsedConfig = {
    safeAddress,
    moduleAddress,
    safeInitData,
    nonce,
    chainId,
    blockGasLimit,
    newConfig,
    executionMode,
    initialState,
    isSystemDeployed,
    actionInputs: parsedActionInputs,
    unlabeledContracts,
    arbitraryChain,
    executorAddress: deploymentInfo.executorAddress,
    libraries: convertLibraryFormat(libraries),
    gitCommit: getCurrentGitCommitHash(),
  }

  return parsedConfig
}

/**
 * Parse the `additionalContracts` array of the Foundry broadcast, which contains all nested
 * contract deployments for the given action. For example, if a contract deploys another contract in
 * its constructor, the child contract's deployment info would exist in this array.
 */
const parseAdditionalContracts = (
  currentInput: RawActionInput,
  configArtifacts: ConfigArtifacts
): {
  parsedContracts: Array<ParsedContractDeployment>
  unlabeledAdditionalContracts: ParsedConfig['unlabeledContracts']
} => {
  const parsedContracts: Array<ParsedContractDeployment> = []
  const unlabeled: ParsedConfig['unlabeledContracts'] = []
  for (const additionalContract of currentInput.additionalContracts) {
    const address = ethers.getAddress(additionalContract.address)

    const fullyQualifiedName = findFullyQualifiedNameForInitCode(
      additionalContract.initCode,
      configArtifacts
    )
    if (fullyQualifiedName) {
      parsedContracts.push({
        address,
        fullyQualifiedName,
        initCodeWithArgs: additionalContract.initCode,
      })
    } else {
      unlabeled.push({ address, initCodeWithArgs: additionalContract.initCode })
    }
  }

  return {
    parsedContracts,
    unlabeledAdditionalContracts: unlabeled,
  }
}

export const makeCreate2DecodedAction = (
  create2Address: string,
  initCodeWithArgs: string,
  configArtifacts: ConfigArtifacts,
  fullyQualifiedName?: string
): DecodedAction => {
  if (fullyQualifiedName) {
    const coder = ethers.AbiCoder.defaultAbiCoder()
    const { artifact } = configArtifacts[fullyQualifiedName]
    const contractName = fullyQualifiedName.split(':')[1]
    const iface = new ethers.Interface(artifact.abi)
    const constructorFragment = iface.fragments.find(
      ConstructorFragment.isFragment
    )

    let variables: ParsedVariable = {}
    if (constructorFragment) {
      const encodedConstructorArgs = getAbiEncodedConstructorArgs(
        initCodeWithArgs,
        artifact.bytecode
      )
      const constructorArgsResult = coder.decode(
        constructorFragment.inputs,
        encodedConstructorArgs
      )
      variables = recursivelyConvertResult(
        constructorFragment.inputs,
        constructorArgsResult
      ) as ParsedVariable
    }

    return {
      referenceName: contractName,
      functionName: 'deploy',
      variables,
      address: create2Address,
    }
  } else {
    return {
      referenceName: create2Address,
      functionName: 'deploy',
      variables: [],
      address: create2Address,
    }
  }
}

export const makeFunctionCallDecodedAction = (
  to: string,
  data: string,
  configArtifacts: ConfigArtifacts,
  dryRunPath: string,
  fullyQualifiedName?: string
): DecodedAction => {
  if (fullyQualifiedName) {
    const { artifact } = configArtifacts[fullyQualifiedName]
    const contractName = fullyQualifiedName.split(':')[1]
    const iface = new ethers.Interface(artifact.abi)

    // Attempt to decode the call. This will return `undefined` if the call cannot be decoded, which
    // will happen if the function does not exist on the contract. For example, this will return
    // `undefined` if the contract's `fallback` function was called.
    const decoded = decodeCall(iface, data)
    const functionName = decoded ? decoded.functionName : 'call'
    const variables = decoded
      ? decoded.variables
      : [
          data.length > 1000
            ? `Very large calldata. View it in Foundry's dry run file: ${dryRunPath}`
            : data,
        ]

    return {
      referenceName: contractName,
      functionName,
      variables,
      address: to,
    }
  } else {
    const variables = [
      data.length > 1000
        ? `Very large calldata. View it in Foundry's dry run file: ${dryRunPath}`
        : data,
    ]
    return {
      referenceName: to,
      functionName: 'call',
      variables,
      address: '',
    }
  }
}
