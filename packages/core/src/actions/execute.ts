import { ethers } from 'ethers'
import { Logger } from '@eth-optimism/common-ts'
import { HardhatEthersProvider } from '@nomicfoundation/hardhat-ethers/internal/hardhat-ethers-provider'

import {
  BundledSphinxAction,
  SphinxBundles,
  DeploymentState,
  DeploymentStatus,
  HumanReadableAction,
} from './types'
import { getGasPriceOverrides } from '../utils'
import { getInitialActionBundle, getSetStorageActionBundle } from './bundle'
import { SphinxJsonRpcProvider } from '../provider'

export const executeDeployment = async (
  manager: ethers.Contract,
  bundles: SphinxBundles,
  humanReadableActions: Array<HumanReadableAction>,
  blockGasLimit: bigint,
  provider: SphinxJsonRpcProvider | HardhatEthersProvider,
  signer: ethers.Signer,
  logger?: Logger | undefined
): Promise<{
  success: boolean
  receipts: ethers.TransactionReceipt[]
  failureAction?: HumanReadableAction
}> => {
  const { actionBundle, targetBundle } = bundles

  logger?.info(`[Sphinx]: preparing to execute the project...`)

  // We execute all actions in batches to reduce the total number of transactions and reduce the
  // cost of a deployment in general. Approaching the maximum block gas limit can cause
  // transactions to be executed slowly as a result of the algorithms that miners use to select
  // which transactions to include. As a result, we restrict our total gas usage to a fraction of
  // the block gas limit.
  const gasFraction = 2n
  const maxGasLimit = blockGasLimit / gasFraction

  const initialActionBundle = getInitialActionBundle(actionBundle)
  const setStorageActionBundle = getSetStorageActionBundle(actionBundle)

  logger?.info(`[Sphinx]: executing initial actions...`)
  const { status, receipts, failureAction } = await executeBatchActions(
    initialActionBundle,
    false,
    manager,
    maxGasLimit,
    humanReadableActions,
    signer,
    provider,
    logger
  )

  if (status === DeploymentStatus.FAILED) {
    logger?.error(`[Sphinx]: failed to execute initial actions`)
    return { success: false, receipts, failureAction }
  } else if (status === DeploymentStatus.COMPLETED) {
    logger?.info(`[Sphinx]: finished non-proxied deployment early`)
    return { success: true, receipts }
  } else {
    logger?.info(`[Sphinx]: executed initial actions`)
  }

  logger?.info(`[Sphinx]: initiating upgrade...`)
  receipts.push(
    await (
      await manager.initiateUpgrade(
        targetBundle.targets.map((target) => target.target),
        targetBundle.targets.map((target) => target.siblings),
        await getGasPriceOverrides(signer)
      )
    ).wait()
  )
  logger?.info(`[Sphinx]: initiated upgrade`)

  logger?.info(`[Sphinx]: executing 'SET_STORAGE' actions...`)
  const { receipts: setStorageReceipts } = await executeBatchActions(
    setStorageActionBundle,
    true,
    manager,
    maxGasLimit,
    humanReadableActions,
    signer,
    provider,
    logger
  )
  receipts.push(...setStorageReceipts)
  logger?.info(`[Sphinx]: executed 'SET_STORAGE' actions`)

  logger?.info(`[Sphinx]: finalizing upgrade...`)
  receipts.push(
    await (
      await manager.finalizeUpgrade(
        targetBundle.targets.map((target) => target.target),
        targetBundle.targets.map((target) => target.siblings),
        await getGasPriceOverrides(signer)
      )
    ).wait()
  )

  // We're done!
  logger?.info(`[Sphinx]: successfully deployed project`)
  return { success: true, receipts }
}

/**
 * Helper function for finding the maximum number of batch elements that can be executed from a
 * given input list of actions. This is done by performing a binary search over the possible
 * batch sizes and finding the largest batch size that does not exceed the maximum gas limit.
 *
 * @param actions List of actions to execute.
 * @returns Maximum number of actions that can be executed.
 */
const findMaxBatchSize = async (
  actions: BundledSphinxAction[],
  maxGasLimit: bigint
): Promise<number> => {
  // Optimization, try to execute the entire batch at once before going through the hassle of a
  // binary search. Can often save a significant amount of time on execution.
  if (await executable(actions, maxGasLimit)) {
    return actions.length
  }

  // If the full batch size isn't executable, then we need to perform a binary search to find the
  // largest batch size that is actually executable.
  let min = 0
  let max = actions.length
  while (min < max) {
    const mid = Math.ceil((min + max) / 2)
    if (await executable(actions.slice(0, mid), maxGasLimit)) {
      min = mid
    } else {
      max = mid - 1
    }
  }

  // No possible size works, this is a problem and should never happen.
  if (min === 0) {
    throw new Error(
      'unable to find a batch size that does not exceed the block gas limit'
    )
  }

  return min
}

/**
 * Helper function for executing a list of actions in batches.
 *
 * @param actions List of actions to execute.
 */
const executeBatchActions = async (
  actions: BundledSphinxAction[],
  isSetStorageActionArray: boolean,
  manager: ethers.Contract,
  maxGasLimit: bigint,
  humanReadableActions: Array<HumanReadableAction>,
  signer: ethers.Signer,
  provider: SphinxJsonRpcProvider | HardhatEthersProvider,
  logger?: Logger | undefined
): Promise<{
  status: bigint
  receipts: ethers.TransactionReceipt[]
  failureAction?: HumanReadableAction
}> => {
  const receipts: ethers.TransactionReceipt[] = []

  // Pull the deployment state from the contract so we're guaranteed to be up to date.
  const activeDeploymentId = await manager.activeDeploymentId()
  let state: DeploymentState = await manager.deployments(activeDeploymentId)

  // Remove the actions that have already been executed.
  const filtered = actions.filter((action) => {
    return action.action.index >= state.actionsExecuted
  })

  // We can return early if there are no actions to execute.
  if (filtered.length === 0) {
    logger?.info('[Sphinx]: no actions left to execute')
    return { status: state.status, receipts }
  }

  let executed = 0
  while (executed < filtered.length) {
    const mostRecentState: DeploymentState = await manager.deployments(
      activeDeploymentId
    )
    if (mostRecentState.status === DeploymentStatus.FAILED) {
      return { status: mostRecentState.status, receipts }
    }

    // Figure out the maximum number of actions that can be executed in a single batch.
    const batchSize = await findMaxBatchSize(
      filtered.slice(executed),
      maxGasLimit
    )

    // Pull out the next batch of actions.
    const batch = filtered.slice(executed, executed + batchSize)

    // Keep 'em notified.
    logger?.info(
      `[Sphinx]: executing actions ${executed} to ${executed + batchSize} of ${
        filtered.length
      }...`
    )

    // Execute the batch of actions.
    if (isSetStorageActionArray) {
      const tx = await (
        await manager.setStorage(
          batch.map((action) => action.action),
          batch.map((action) => action.siblings),
          await getGasPriceOverrides(signer)
        )
      ).wait()
      receipts.push(tx)
    } else {
      try {
        // Call estimateGas first to check if the transaction will fail.
        // estimateGas provides more information on the failure, allowing us to decode the custom error.
        await provider.estimateGas({
          to: await manager.getAddress(),
          from: await signer.getAddress(),
          data: manager.interface.encodeFunctionData('executeInitialActions', [
            batch.map((action) => action.action),
            batch.map((action) => action.siblings),
          ]),
        })

        const tx = await (
          await manager.executeInitialActions(
            batch.map((action) => action.action),
            batch.map((action) => action.siblings),
            await getGasPriceOverrides(signer)
          )
        ).wait()
        receipts.push(tx)
      } catch (e) {
        // If the deployment failed due to a constructor or call reverting, handle gracefully.
        const revertData = e.data
        const decodedError = manager.interface.parseError(revertData)
        if (decodedError?.name === 'DeploymentFailed') {
          logger?.error(`[Sphinx]: failed to execute initial actions`)
          if (decodedError?.args[0] !== undefined) {
            const failureAction = humanReadableActions[decodedError.args[0]]
            return { status: DeploymentStatus.FAILED, receipts, failureAction }
          }
        } else {
          // Otherwise, rethrow the error
          throw e
        }
      }
    }

    // Return early if the deployment failed.
    state = await manager.deployments(activeDeploymentId)
    if (state.status === DeploymentStatus.FAILED) {
      return { status: state.status, receipts }
    }

    // Move on to the next batch if necessary.
    executed += batchSize
  }

  // Return the final deployment status.
  return { status: state.status, receipts }
}

/**
 * Helper function that determines if a given batch is executable.
 *
 * @param selected Selected actions to execute.
 * @returns True if the batch is executable, false otherwise.
 */
export const executable = async (
  selected: BundledSphinxAction[],
  maxGasLimit: bigint
): Promise<boolean> => {
  const estGasUsed = selected
    .map((action) => action.gas)
    .reduce((a, b) => a + b)

  return maxGasLimit > estGasUsed
}
