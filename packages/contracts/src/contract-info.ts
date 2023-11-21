import { ZeroAddress } from 'ethers'

import {
  CompatibilityFallbackHandlerArtifact,
  CreateCallArtifact,
  DefaultCallbackHandlerArtifact,
  GnosisSafeArtifact,
  GnosisSafeL2Artifact,
  GnosisSafeProxyFactoryArtifact,
  ManagedServiceArtifact,
  MultiSendArtifact,
  MultiSendCallOnlyArtifact,
  SimulateTxAccessorArtifact,
  SphinxModuleProxyFactoryArtifact,
} from './ifaces'
import { FoundryContractArtifact, GnosisSafeContractArtifact } from './types'
import {
  getCompatibilityFallbackHandlerAddress,
  getCreateCallAddress,
  getDefaultCallbackHandlerAddress,
  getGnosisSafeAddress,
  getGnosisSafeL2Address,
  getGnosisSafeProxyFactoryAddress,
  getManagedServiceAddress,
  getMultiSendAddress,
  getMultiSendCallOnlyAddress,
  getSimulateTxAccessorAddress,
  getSphinxModuleProxyFactoryAddress,
} from './addresses'
import { getOwnerAddress } from './constants'

export const getSphinxConstants = (): Array<{
  artifact: FoundryContractArtifact | GnosisSafeContractArtifact
  expectedAddress: string
  constructorArgs: any[]
}> => {
  const contractInfo = [
    {
      artifact: ManagedServiceArtifact,
      expectedAddress: getManagedServiceAddress(),
      constructorArgs: [getOwnerAddress()],
    },
    {
      artifact: SphinxModuleProxyFactoryArtifact,
      expectedAddress: getSphinxModuleProxyFactoryAddress(),
      constructorArgs: [],
    },
    {
      artifact: SimulateTxAccessorArtifact,
      expectedAddress: getSimulateTxAccessorAddress(),
      constructorArgs: [],
    },
    {
      artifact: GnosisSafeProxyFactoryArtifact,
      expectedAddress: getGnosisSafeProxyFactoryAddress(),
      constructorArgs: [],
    },
    {
      artifact: DefaultCallbackHandlerArtifact,
      expectedAddress: getDefaultCallbackHandlerAddress(),
      constructorArgs: [],
    },
    {
      artifact: CompatibilityFallbackHandlerArtifact,
      expectedAddress: getCompatibilityFallbackHandlerAddress(),
      constructorArgs: [],
    },
    {
      artifact: CreateCallArtifact,
      expectedAddress: getCreateCallAddress(),
      constructorArgs: [],
    },
    {
      artifact: MultiSendArtifact,
      expectedAddress: getMultiSendAddress(),
      constructorArgs: [],
    },
    {
      artifact: MultiSendCallOnlyArtifact,
      expectedAddress: getMultiSendCallOnlyAddress(),
      constructorArgs: [],
    },
    {
      artifact: GnosisSafeL2Artifact,
      expectedAddress: getGnosisSafeL2Address(),
      constructorArgs: [],
    },
    {
      artifact: GnosisSafeArtifact,
      expectedAddress: getGnosisSafeAddress(),
      constructorArgs: [],
    },
  ]

  return contractInfo
}
