import { BigNumber, ethers } from 'ethers'

import { Integration } from '../constants'

export const resolveNetworkName = async (
  provider: ethers.providers.Provider,
  integration: Integration
) => {
  const { name: networkName } = await provider.getNetwork()
  if (networkName === 'unknown') {
    if (integration === 'hardhat') {
      return 'hardhat'
    } else if (integration === 'foundry') {
      return 'anvil'
    }
  }
  return networkName
}

export const errorProjectNotRegistered = async (
  provider: ethers.providers.JsonRpcProvider,
  configPath: string,
  integration: Integration
) => {
  const networkName = await resolveNetworkName(provider, integration)

  if (integration === 'hardhat') {
    throw new Error(`This project has not been registered on ${networkName}.
To register the project on this network, run the following command:

npx hardhat chugsplash-register --network <network> --owner <ownerAddress> --config-path ${configPath}
  `)
  } else {
    // TODO - output foundry error
    throw new Error(`This project has not been registered on ${networkName}.
To register the project on this network, call the register function from your script:

chugsplash.register("${configPath}", false);
`)
  }
}

export const errorProjectCurrentlyActive = (
  integration: Integration,
  configPath: string
) => {
  if (integration === 'hardhat') {
    throw new Error(
      `Project is currently active. You must cancel the project in order to withdraw funds:

npx hardhat chugsplash-cancel --network <network> --config-path ${configPath}
        `
    )
  } else {
    // TODO - output foundry error
    throw new Error(`Project is currently active. You must cancel the project in order to withdraw funds:

chugsplash.cancel("${configPath}");
`)
  }
}

export const successfulProposalMessage = async (
  provider: ethers.providers.JsonRpcProvider,
  amount: BigNumber,
  configPath: string,
  integration: Integration
): Promise<string> => {
  const networkName = await resolveNetworkName(provider, integration)

  if (integration === 'hardhat') {
    if (amount.gt(0)) {
      return `Project successfully proposed on ${networkName}. Fund and approve the deployment using the command:

npx hardhat chugsplash-approve --network <network> --amount ${amount} --config-path ${configPath}`
    } else {
      return `Project successfully proposed and funded on ${networkName}. Approve the deployment using the command:

npx hardhat chugsplash-approve --network <network> --config-path ${configPath}`
    }
  } else {
    // TODO - output foundry error
    if (amount.gt(0)) {
      return `Project successfully proposed on ${networkName}. To fund and approve the deployment, call the following functions from your script:

chugsplash.fund("${configPath}", ${amount} ether, false);
chugsplas.approve("${configPath}", false, false);`
    } else {
      return `Project successfully proposed and funded on ${networkName}. To approve the deployment, call the following function from your script:

chugsplas.approve("${configPath}", false, false);`
    }
  }
}

export const alreadyProposedMessage = async (
  provider: ethers.providers.JsonRpcProvider,
  amount: BigNumber,
  configPath: string,
  integration: Integration
): Promise<string> => {
  const networkName = await resolveNetworkName(provider, integration)

  if (integration === 'hardhat') {
    if (amount.gt(0)) {
      return `Project has already been proposed on ${networkName}. Fund and approve the deployment using the command:

npx hardhat chugsplash-approve --network <network> --amount ${amount} --config-path ${configPath}`
    } else {
      return `Project has already been proposed and funded on ${networkName}. Approve the deployment using the command:

npx hardhat chugsplash-approve --network <network> --config-path ${configPath}`
    }
  } else {
    // TODO - output foundry error
    if (amount.gt(0)) {
      return `Project has already been proposed on ${networkName}. To fund and approve the deployment, call the following functions from your script:

      chugsplash.fund("${configPath}", ${amount} ether, false);
      chugsplas.approve("${configPath}", false, false);`
    } else {
      return `Project has already been proposed and funded on ${networkName}. To approve the deployment, call the following function from your script:

chugsplas.approve("${configPath}", false, false);`
    }
  }
}
