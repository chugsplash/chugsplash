import { HardhatRuntimeEnvironment, HardhatUserConfig } from 'hardhat/types'
import { task } from 'hardhat/config'
import * as dotenv from 'dotenv'

// Hardhat plugins
import '@nomiclabs/hardhat-ethers'
import '@openzeppelin/hardhat-upgrades'

import { initializeAndVerifySphinx } from './src/languages/solidity/predeploys'

// Load environment variables from .env
dotenv.config()

const accounts = process.env.PRIVATE_KEY ? [process.env.PRIVATE_KEY] : []

const config: HardhatUserConfig = {
  solidity: {
    version: '0.8.15',
    settings: {
      outputSelection: {
        '*': {
          '*': ['storageLayout'],
        },
      },
      optimizer: {
        enabled: true,
        runs: 200,
      },
      metadata: {
        bytecodeHash: 'none',
      },
    },
  },
  networks: {
    goerli: {
      chainId: 5,
      url: `https://eth-goerli.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`,
      accounts,
    },
    ethereum: {
      chainId: 1,
      url: `https://eth-mainnet.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`,
      accounts,
    },
    'optimism-goerli': {
      chainId: 420,
      url: `https://opt-goerli.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`,
      accounts,
    },
    optimism: {
      chainId: 10,
      url: `https://opt-mainnet.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`,
      accounts,
    },
    arbitrum: {
      chainId: 42161,
      url: `https://arb-mainnet.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`,
      accounts,
    },
    'arbitrum-goerli': {
      chainId: 421613,
      url: `https://arb-goerli.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`,
      accounts,
    },
    bnbt: {
      chainId: 97,
      url: process.env.BNB_TESTNET_URL,
      accounts,
    },
    bnb: {
      chainId: 56,
      url: process.env.BNB_MAINNET_URL,
      accounts,
    },
    'gnosis-chiado': {
      chainId: 10200,
      url: `${process.env.CHIADO_RPC_URL}`,
      accounts,
    },
    gnosis: {
      chainId: 100,
      url: process.env.GNOSIS_MAINNET_URL,
      accounts,
    },
    maticmum: {
      chainId: 80001,
      url: `https://polygon-mumbai.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`,
      accounts,
    },
    polygon: {
      chainId: 137,
      url: `https://polygon-mainnet.g.alchemy.com/v2/${process.env.ALCHEMY_API_KEY}`,
      accounts,
    },
    'polygon-zkevm': {
      chainId: 1101,
      url: `${process.env.POLYGON_ZKEVM_MAINNET_URL}`,
      accounts,
    },
    'polygon-zkevm-testnet': {
      chainId: 1442,
      url: `${process.env.POLYGON_ZKEVM_TESTNET_URL}`,
      accounts,
    },
    linea: {
      chainId: 59144,
      url: `https://linea-mainnet.infura.io/v3/${process.env.INFURA_API_KEY}`,
      accounts,
    },
    'linea-testnet': {
      chainId: 59140,
      url: `https://linea-goerli.infura.io/v3/${process.env.INFURA_API_KEY}`,
      accounts,
    },
    'fantom-testnet': {
      chainId: 4002,
      url: `${process.env.FANTOM_TESTNET_RPC_URL}`,
      accounts,
    },
    fantom: {
      chainId: 250,
      url: `${process.env.FANTOM_MAINNET_RPC_URL}`,
      accounts,
    },
    'avalanche-fiji': {
      chainId: 43113,
      url: `https://avalanche-fuji.infura.io/v3/${process.env.INFURA_API_KEY}`,
      accounts,
    },
    avalanche: {
      chainId: 43114,
      url: `https://avalanche-mainnet.infura.io/v3/${process.env.INFURA_API_KEY}`,
      accounts,
    },
  },
}

task('deploy-system')
  .setDescription('Deploys the Sphinx contracts to the specified network')
  .addParam('systemConfig', 'Path to a Sphinx system config file')
  .setAction(
    async (
      args: {
        systemConfig: string
      },
      hre: HardhatRuntimeEnvironment
    ) => {
      await initializeAndVerifySphinx(args.systemConfig, hre.ethers.provider)
    }
  )

export default config
