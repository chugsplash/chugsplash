import { StandardMerkleTree } from '@openzeppelin/merkle-tree'
import { AbiCoder } from 'ethers'

// TODO(md): See Solidity docs
export enum LeafType {
  APPROVE,
  EXECUTE,
}

// TODO(md): See Safe docs
export enum Operation {
  Call,
  DelegateCall,
}

// TODO(md): See Solidity docs
export type SphinxLeaf = {
  chainId: bigint
  index: bigint
  data: string
  leafType: LeafType
}

// TODO(md)
export type NetworkDeploymentData = {
  nonce: bigint
  executor: string
  safe: string
  deploymentURI: string
  txs: SphinxTransaction[]
}

export type DeploymentData = Record<number, NetworkDeploymentData>

// TODO(md)
export type SphinxTransaction = {
  to: string
  value: string
  txData: string
  gas: string
  operation: Operation
}

// TODO(md)
export interface SphinxBundle {
  root: string
  leafs: BundledSphinxLeaf[]
}

// TODO(md)
export interface BundledSphinxLeaf {
  leaf: SphinxLeaf
  leafType: LeafType
  proof: string[]
}

// TODO(md)
export const makeSphinxMerkleTree = (
  deploymentData: Record<number, NetworkDeploymentData>
): {
  tree: StandardMerkleTree<(string | bigint | LeafType)[]>
  leafs: Array<SphinxLeaf>
} => {
  const merkleLeafs: Array<SphinxLeaf> = []

  for (const [chainId, data] of Object.entries(deploymentData)) {
    // generate approval leaf data
    const approvalData = AbiCoder.defaultAbiCoder().encode(
      ['address', 'uint', 'uint', 'address', 'string'],
      [
        data.safe,
        data.nonce,
        data.txs.length,
        data.executor,
        data.deploymentURI,
      ]
    )

    // push approval leaf
    merkleLeafs.push({
      chainId: BigInt(chainId),
      index: BigInt(0),
      leafType: LeafType.APPROVE,
      data: approvalData,
    })

    // push transaction leafs
    let index = BigInt(1)
    for (const tx of data.txs) {
      // generate transaction leaf data
      const transactionLeafData = AbiCoder.defaultAbiCoder().encode(
        ['address', 'uint', 'uint', 'bytes', 'uint'],
        [
          tx.to,
          BigInt(tx.value),
          BigInt(tx.gas),
          tx.txData,
          BigInt(tx.operation),
        ]
      )

      merkleLeafs.push({
        chainId: BigInt(chainId),
        index,
        leafType: LeafType.EXECUTE,
        data: transactionLeafData,
      })

      index += BigInt(1)
    }
  }

  const rawLeafArray = merkleLeafs.map((leaf) => Object.values(leaf))
  return {
    tree: StandardMerkleTree.of(rawLeafArray, [
      'uint256',
      'uint256',
      'uint256',
      'bytes',
    ]),
    leafs: merkleLeafs,
  }
}

// TODO(md)
export const makeSphinxBundle = (
  deploymentData: DeploymentData
): SphinxBundle => {
  const { tree, leafs } = makeSphinxMerkleTree(deploymentData)

  return {
    root: tree.root,
    leafs: leafs.map((leaf) => {
      return {
        leaf,
        proof: tree.getProof(Object.values(leaf)),
        leafType: leaf.leafType,
      }
    }),
  }
}
