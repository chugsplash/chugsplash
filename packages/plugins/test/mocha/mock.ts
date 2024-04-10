import { ConfigArtifacts } from '@sphinx-labs/core'
import sinon from 'sinon'

import { propose } from '../../src/cli/propose'
import { deploy } from '../../src/cli/deploy'
import { makeSphinxContext } from '../../src/cli/context'
import { readContractArtifact } from '../../dist'
import { getDummyNetworkConfig } from './dummy'

/**
 * Make a mocked `SphinxContext` object. Use this function if it's safe to assume that all of
 * `SphinxContext` member functions are mocked. In integration tests, use the
 * `makeMockSphinxContextForIntegrationTests` function instead.
 */
export const makeMockSphinxContext = (
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  mockedFullyQualifiedNames: Array<string>
) => {
  const sphinxContext = makeSphinxContext()

  const isLiveNetwork = sinon
    .stub(sphinxContext, 'isLiveNetwork')
    .returns(Promise.resolve(true))

  const getNetworkGasEstimate = sinon
    .stub(sphinxContext, 'getNetworkGasEstimate')
    .returns(
      Promise.resolve({
        chainId: 0,
        estimatedGas: '0',
        fundsRequested: '0',
        transactions: [],
      })
    )
  const relayProposal = sinon
    .stub(sphinxContext, 'relayProposal')
    .returns(Promise.resolve())
  const prompt = sinon.stub().returns(Promise.resolve())
  const buildNetworkConfigArray = sinon
    .stub(sphinxContext, 'buildNetworkConfigArray')
    .returns(
      Promise.resolve({
        networkConfigArray: [getDummyNetworkConfig()],
        configArtifacts: {},
        isEmpty: false,
      })
    )
  const storeDeploymentConfig = sinon
    .stub(sphinxContext, 'storeDeploymentConfig')
    .returns(Promise.resolve('mock-canonical-config-id'))

  // eslint-disable-next-line @typescript-eslint/no-empty-function
  const assertNoLinkedLibraries = async () => {}

  return {
    isLiveNetwork,
    propose,
    deploy,
    buildNetworkConfigArray,
    getNetworkGasEstimate,
    storeDeploymentConfig,
    relayProposal,
    prompt,
    assertNoLinkedLibraries,
  }
}

/**
 * Make a mock `SphinxContext` to use in integration tests. This object mocks a minimal set of
 * functionality, such as API calls and the user confirmation prompt.
 */
export const makeMockSphinxContextForIntegrationTests = (
  fullyQualifiedNames: Array<string>
) => {
  const {
    prompt,
    relayProposal,
    storeDeploymentConfig,
    isLiveNetwork,
    assertNoLinkedLibraries,
  } = makeMockSphinxContext(fullyQualifiedNames)
  const context = makeSphinxContext()

  context.prompt = prompt
  context.relayProposal = relayProposal
  context.storeDeploymentConfig = storeDeploymentConfig
  context.isLiveNetwork = isLiveNetwork
  context.assertNoLinkedLibraries = assertNoLinkedLibraries

  return { context, prompt }
}
