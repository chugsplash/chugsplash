import { GraphQLClient, gql } from 'graphql-request'

const updateDeploymentMutation = gql`
  mutation UpdateProjectDeployment($input: UpdateProjectDeploymentInput!) {
    UpdateProjectDeployment(input: $input) {
      id
      onChainId
    }
  }
`

const createContractsMutation = gql`
  mutation CreateContract($input: CreateContractsInput!) {
    CreateContracts(input: $input)
  }
`

type DeploymentStatus =
  | 'triggered'
  | 'proposed'
  | 'approved'
  | 'cancelled'
  | 'executed'
  | 'verified'

type Contract = {
  referenceName: string
  contractName: string
  address: string
}

export const updateDeployment = async (
  client: GraphQLClient,
  deploymentId: string,
  chainId: number,
  status: DeploymentStatus,
  contracts: Contract[]
) => {
  if (contracts.length > 0) {
    await client.request(createContractsMutation, {
      input: {
        deploymentId,
        chainId,
        contracts,
        publicKey: process.env.MANAGED_PUBLIC_KEY,
      },
    })
  }

  await client.request(updateDeploymentMutation, {
    input: {
      deploymentId,
      chainId,
      status,
      publicKey: process.env.MANAGED_PUBLIC_KEY,
    },
  })
}
