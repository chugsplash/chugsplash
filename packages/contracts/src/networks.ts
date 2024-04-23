export type ExplorerName = 'Blockscout' | 'Etherscan'

export type BlockExplorers = {
  etherscan?: {
    apiURL: string
    browserURL: string
    envKey: string
  }
  blockscout?: {
    apiURL: string
    browserURL: string
    envKey: string
    selfHosted: boolean
  }
}

export type SupportedNetwork = {
  name: string
  displayName: string
  chainId: bigint
  rpcUrl: () => string
  rpcUrlId: string
  blockexplorers: BlockExplorers
  currency: string
  dripSize: string
  networkType: NetworkType
  dripVersion: number
  decimals: number
  queryFilterBlockLimit: number
  legacyTx: boolean
  actionGasLimitBuffer: boolean
  eip2028: boolean
  actionTransactionBatching: boolean
  rollupStack?: {
    provider: RollupProvider
    type: RollupType
  }
  hardcodedMerkleLeafGas?: string
}

export type SupportedLocalNetwork = {
  name: string
  chainId: bigint
  networkType: NetworkType
  legacyTx: false
  actionGasLimitBuffer: false
  eip2028: true
  dripSize: string
  currency: string
}

export const SPHINX_LOCAL_NETWORKS: Array<SupportedLocalNetwork> = [
  {
    name: 'anvil',
    chainId: BigInt(31337),
    networkType: 'Local',
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    dripSize: '1',
    currency: 'ETH',
  },
]

export type NetworkType = 'Testnet' | 'Mainnet' | 'Local'
type RollupProvider = 'Conduit' | 'Caldera'
type RollupType = 'OP Stack' | 'Arbitrum'

export const SPHINX_NETWORKS: Array<SupportedNetwork> = [
  {
    name: 'ethereum',
    displayName: 'Ethereum',
    chainId: BigInt(1),
    rpcUrl: () => process.env.ETH_MAINNET_URL!,
    rpcUrlId: 'ETH_MAINNET_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api.etherscan.io/api',
        browserURL: 'https://etherscan.io',
        envKey: 'ETH_ETHERSCAN_API_KEY',
      },
      blockscout: {
        apiURL: 'https://eth.blockscout.com/api',
        browserURL: 'https://eth.blockscout.com/',
        envKey: 'ETH_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'ETH',
    dripSize: '0.15',
    networkType: 'Mainnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'sepolia',
    displayName: 'Sepolia',
    chainId: BigInt(11155111),
    rpcUrl: () => process.env.ETH_SEPOLIA_URL!,
    rpcUrlId: 'ETH_SEPOLIA_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-sepolia.etherscan.io/api',
        browserURL: 'https://sepolia.etherscan.io',
        envKey: 'ETH_ETHERSCAN_API_KEY',
      },
      blockscout: {
        apiURL: 'https://eth-sepolia.blockscout.com/api',
        browserURL: 'https://eth-sepolia.blockscout.com/',
        envKey: 'ETH_SEPOLIA_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'ETH',
    dripSize: '1',
    networkType: 'Testnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'optimism',
    displayName: 'Optimism',
    chainId: BigInt(10),
    rpcUrl: () => process.env.OPT_MAINNET_URL!,
    rpcUrlId: 'OPT_MAINNET_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-optimistic.etherscan.io/api',
        browserURL: 'https://optimistic.etherscan.io/',
        envKey: 'OPT_ETHERSCAN_API_KEY',
      },
      blockscout: {
        apiURL: 'https://optimism.blockscout.com/api',
        browserURL: 'https://optimism.blockscout.com/',
        envKey: 'OPT_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'ETH',
    dripSize: '0.025',
    networkType: 'Mainnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'optimism_sepolia',
    displayName: 'Optimism Sepolia',
    chainId: BigInt(11155420),
    rpcUrl: () => process.env.OPT_SEPOLIA_URL!,
    rpcUrlId: 'OPT_SEPOLIA_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-sepolia-optimism.etherscan.io/api',
        browserURL: 'https://sepolia-optimism.etherscan.io/',
        envKey: 'OPT_ETHERSCAN_API_KEY',
      },
      blockscout: {
        apiURL: 'https://optimism-sepolia.blockscout.com/api',
        browserURL: 'https://optimism-sepolia.blockscout.com/',
        envKey: 'OPT_SEPOLIA_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'ETH',
    dripSize: '0.15',
    networkType: 'Testnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'arbitrum',
    displayName: 'Arbitrum',
    chainId: BigInt(42161),
    rpcUrl: () => process.env.ARB_MAINNET_URL!,
    rpcUrlId: 'ARB_MAINNET_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api.arbiscan.io/api',
        browserURL: 'https://arbiscan.io/',
        envKey: 'ARB_ETHERSCAN_API_KEY',
      },
    },
    currency: 'ETH',
    dripSize: '0.025',
    networkType: 'Mainnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: false,
  },
  {
    name: 'arbitrum_sepolia',
    displayName: 'Arbitrum Sepolia',
    chainId: BigInt(421614),
    rpcUrl: () => process.env.ARB_SEPOLIA_URL!,
    rpcUrlId: 'ARB_SEPOLIA_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-sepolia.arbiscan.io/api',
        browserURL: 'https://sepolia.arbiscan.io/',
        envKey: 'ARB_ETHERSCAN_API_KEY',
      },
    },
    currency: 'ETH',
    dripSize: '0.15',
    networkType: 'Testnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: false,
  },
  {
    name: 'polygon',
    displayName: 'Polygon',
    chainId: BigInt(137),
    rpcUrl: () => `${process.env.POLYGON_MAINNET_URL}`,
    rpcUrlId: 'POLYGON_MAINNET_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api.polygonscan.com/api',
        browserURL: 'https://polygonscan.com',
        envKey: 'POLYGON_ETHERSCAN_API_KEY',
      },
    },
    currency: 'MATIC',
    dripSize: '1',
    networkType: 'Mainnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'polygon_amoy',
    displayName: 'Polygon Amoy',
    chainId: BigInt(80002),
    rpcUrl: () => `${process.env.POLYGON_AMOY_URL}`,
    rpcUrlId: 'POLYGON_AMOY_URL',
    blockexplorers: {},
    currency: 'MATIC',
    dripSize: '1',
    networkType: 'Testnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'bnb',
    displayName: 'Binance Smart Chain',
    chainId: BigInt(56),
    rpcUrl: () => process.env.BNB_MAINNET_URL!,
    rpcUrlId: 'BNB_MAINNET_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api.bscscan.com/api',
        browserURL: 'https://bscscan.com',
        envKey: 'BNB_ETHERSCAN_API_KEY',
      },
    },
    currency: 'BNB',
    dripSize: '0.05',
    networkType: 'Mainnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'bnb_testnet',
    displayName: 'Binance Smart Chain Testnet',
    chainId: BigInt(97),
    rpcUrl: () => process.env.BNB_TESTNET_URL!,
    rpcUrlId: 'BNB_TESTNET_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-testnet.bscscan.com/api',
        browserURL: 'https://testnet.bscscan.com',
        envKey: 'BNB_ETHERSCAN_API_KEY',
      },
    },
    currency: 'BNB',
    dripSize: '0.15',
    networkType: 'Testnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'gnosis',
    displayName: 'Gnosis',
    chainId: BigInt(100),
    rpcUrl: () => process.env.GNOSIS_MAINNET_URL!,
    rpcUrlId: 'GNOSIS_MAINNET_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api.gnosisscan.io/api',
        browserURL: 'https://gnosisscan.io',
        envKey: 'GNOSIS_MAINNET_ETHERSCAN_API_KEY',
      },
      blockscout: {
        apiURL: 'https://gnosis-chiado.blockscout.com/api',
        browserURL: 'https://gnosis-chiado.blockscout.com/',
        envKey: 'GNOSIS_MAINNET_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'xDAI',
    dripSize: '1',
    networkType: 'Mainnet',
    dripVersion: 3,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'gnosis_chiado',
    displayName: 'Gnosis Chiado',
    chainId: BigInt(10200),
    rpcUrl: () => process.env.GNOSIS_CHIADO_URL!,
    rpcUrlId: 'GNOSIS_CHIADO_URL',
    blockexplorers: {
      blockscout: {
        apiURL: 'https://gnosis-chiado.blockscout.com/api',
        browserURL: 'https://gnosis-chiado.blockscout.com',
        envKey: 'GNOSIS_CHIADO_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'xDAI',
    dripSize: '0.15',
    networkType: 'Testnet',
    dripVersion: 3,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'linea',
    displayName: 'Linea',
    chainId: BigInt(59144),
    rpcUrl: () => process.env.LINEA_MAINNET_URL!,
    rpcUrlId: 'LINEA_MAINNET_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api.lineascan.build/api',
        browserURL: 'https://lineascan.build',
        envKey: 'LINEA_ETHERSCAN_API_KEY',
      },
    },
    currency: 'ETH',
    dripSize: '0.025',
    networkType: 'Mainnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: false,
  },
  {
    name: 'linea_sepolia',
    displayName: 'Linea Sepolia',
    chainId: BigInt(59141),
    rpcUrl: () => process.env.LINEA_SEPOLIA_URL!,
    rpcUrlId: 'LINEA_SEPOLIA_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-sepolia.lineascan.build/api',
        browserURL: 'https://sepolia.lineascan.build',
        envKey: 'LINEA_ETHERSCAN_API_KEY',
      },
    },
    currency: 'ETH',
    dripSize: '0.15',
    networkType: 'Testnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: false,
  },
  {
    name: 'polygon_zkevm',
    displayName: 'Polygon zkEVM',
    chainId: BigInt(1101),
    rpcUrl: () => process.env.POLYGON_ZKEVM_MAINNET_URL!,
    rpcUrlId: 'POLYGON_ZKEVM_MAINNET_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-zkevm.polygonscan.com/api',
        browserURL: 'https://zkevm.polygonscan.com/',
        envKey: 'POLYGON_ZKEVM_ETHERSCAN_API_KEY',
      },
      blockscout: {
        apiURL: 'https://zkevm.blockscout.com/api',
        browserURL: 'https://zkevm.blockscout.com/',
        // key is not required on this network
        envKey: 'POLYGON_ZKEVM_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'ETH',
    dripSize: '0.025',
    networkType: 'Mainnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: false,
  },
  {
    name: 'polygon_zkevm_cardona',
    displayName: 'Polygon zkEVM Cardona',
    chainId: BigInt(2442),
    rpcUrl: () => process.env.POLYGON_ZKEVM_CARDONA_URL!,
    rpcUrlId: 'POLYGON_ZKEVM_CARDONA_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-cardona-zkevm.polygonscan.com/api',
        browserURL: 'https://cardona-zkevm.polygonscan.com',
        envKey: 'POLYGON_ZKEVM_ETHERSCAN_API_KEY',
      },
    },
    currency: 'ETH',
    dripSize: '0.15',
    networkType: 'Testnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: false,
  },
  {
    name: 'avalanche',
    displayName: 'Avalanche',
    chainId: BigInt(43114),
    rpcUrl: () => process.env.AVALANCHE_MAINNET_URL!,
    rpcUrlId: 'AVALANCHE_MAINNET_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api.snowtrace.io/api',
        browserURL: 'https://snowtrace.io/',
        envKey: 'AVAX_ETHERSCAN_API_KEY',
      },
    },
    currency: 'AVAX',
    dripSize: '1',
    networkType: 'Mainnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'avalanche_fuji',
    displayName: 'Avalanche Fuji',
    chainId: BigInt(43113),
    rpcUrl: () => process.env.AVALANCHE_FUJI_URL!,
    rpcUrlId: 'AVALANCHE_FUJI_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-testnet.snowtrace.io/api',
        browserURL: 'https://testnet.snowtrace.io/',
        envKey: 'AVAX_ETHERSCAN_API_KEY',
      },
    },
    currency: 'AVAX',
    dripSize: '1',
    networkType: 'Testnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'fantom',
    displayName: 'Fantom',
    chainId: BigInt(250),
    rpcUrl: () => process.env.FANTOM_MAINNET_URL!,
    rpcUrlId: 'FANTOM_MAINNET_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api.ftmscan.com/api',
        browserURL: 'https://ftmscan.com',
        envKey: 'FANTOM_ETHERSCAN_API_KEY',
      },
    },
    currency: 'FTM',
    dripSize: '1',
    networkType: 'Mainnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'fantom_testnet',
    displayName: 'Fantom Testnet',
    chainId: BigInt(4002),
    rpcUrl: () => process.env.FANTOM_TESTNET_URL!,
    rpcUrlId: 'FANTOM_TESTNET_URL',
    blockexplorers: {},
    currency: 'FTM',
    dripSize: '1',
    networkType: 'Testnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'base',
    displayName: 'Base',
    chainId: BigInt(8453),
    rpcUrl: () => process.env.BASE_MAINNET_URL!,
    rpcUrlId: 'BASE_MAINNET_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api.basescan.org/api',
        browserURL: 'https://basescan.org/',
        envKey: 'BASE_ETHERSCAN_API_KEY',
      },
      blockscout: {
        apiURL: 'https://base.blockscout.com/api',
        browserURL: 'https://base.blockscout.com/',
        envKey: 'BASE_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'ETH',
    dripSize: '0.025',
    networkType: 'Mainnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'base_sepolia',
    displayName: 'Base Sepolia',
    chainId: BigInt(84532),
    rpcUrl: () => process.env.BASE_SEPOLIA_URL!,
    rpcUrlId: 'BASE_SEPOLIA_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-sepolia.basescan.org/',
        browserURL: 'https://sepolia.basescan.org/',
        envKey: 'BASE_ETHERSCAN_API_KEY',
      },
      blockscout: {
        apiURL: 'https://base-sepolia.blockscout.com/api',
        browserURL: 'https://base-sepolia.blockscout.com/',
        envKey: 'BASE_SEPOLIA_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'ETH',
    dripSize: '0.15',
    networkType: 'Testnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'celo',
    displayName: 'Celo',
    chainId: BigInt(42220),
    rpcUrl: () => process.env.CELO_MAINNET_URL!,
    rpcUrlId: 'CELO_MAINNET_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api.celoscan.io/api',
        browserURL: 'https://celoscan.io/',
        envKey: 'CELO_ETHERSCAN_API_KEY',
      },
    },
    currency: 'CELO',
    dripSize: '1',
    networkType: 'Mainnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'celo_alfajores',
    displayName: 'Celo Alfajores',
    chainId: BigInt(44787),
    rpcUrl: () => process.env.CELO_ALFAJORES_URL!,
    rpcUrlId: 'CELO_ALFAJORES_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-alfajores.celoscan.io/api',
        browserURL: 'https://alfajores.celoscan.io/',
        envKey: 'CELO_ETHERSCAN_API_KEY',
      },
    },
    currency: 'CELO',
    dripSize: '0.15',
    networkType: 'Testnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'moonriver',
    displayName: 'Moonriver',
    chainId: BigInt(1285),
    rpcUrl: () => process.env.MOONRIVER_MAINNET_URL!,
    rpcUrlId: 'MOONRIVER_MAINNET_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-moonriver.moonscan.io/api',
        browserURL: 'https://moonriver.moonscan.io',
        envKey: 'MOONRIVER_ETHERSCAN_API_KEY',
      },
    },
    currency: 'MOVR',
    dripSize: '0.15',
    queryFilterBlockLimit: 500,
    dripVersion: 2,
    networkType: 'Mainnet',
    decimals: 18,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    hardcodedMerkleLeafGas: (10_500_000).toString(),
    actionTransactionBatching: false,
  },
  {
    name: 'moonbeam',
    displayName: 'Moonbeam',
    chainId: BigInt(1284),
    rpcUrl: () => process.env.MOONBEAM_MAINNET_URL!,
    rpcUrlId: 'MOONBEAM_MAINNET_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-moonbeam.moonscan.io/api',
        browserURL: 'https://moonbeam.moonscan.io',
        envKey: 'MOONBEAM_ETHERSCAN_API_KEY',
      },
    },
    currency: 'GLMR',
    dripSize: '1',
    queryFilterBlockLimit: 500,
    networkType: 'Mainnet',
    dripVersion: 2,
    decimals: 18,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    hardcodedMerkleLeafGas: (10_500_000).toString(),
    actionTransactionBatching: false,
  },
  {
    name: 'moonbase_alpha',
    displayName: 'Moonbase Alpha',
    chainId: BigInt(1287),
    rpcUrl: () => process.env.MOONBASE_ALPHA_URL!,
    rpcUrlId: 'MOONBASE_ALPHA_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-moonbase.moonscan.io/api',
        browserURL: 'https://moonbase.moonscan.io/',
        envKey: 'MOONBEAM_ETHERSCAN_API_KEY',
      },
    },
    currency: 'GLMR',
    dripSize: '0.05',
    queryFilterBlockLimit: 500,
    networkType: 'Testnet',
    dripVersion: 3,
    decimals: 18,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    hardcodedMerkleLeafGas: (10_500_000).toString(),
    actionTransactionBatching: false,
  },
  {
    name: 'fuse',
    displayName: 'Fuse',
    chainId: BigInt(122),
    rpcUrl: () => process.env.FUSE_MAINNET_URL!,
    rpcUrlId: 'FUSE_MAINNET_URL',
    blockexplorers: {
      blockscout: {
        apiURL: 'https://explorer.fuse.io/api',
        browserURL: 'https://explorer.fuse.io',
        envKey: 'FUSE_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'FUSE',
    dripSize: '1',
    networkType: 'Mainnet',
    dripVersion: 2,
    legacyTx: true,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'evmos',
    displayName: 'Evmos',
    chainId: BigInt(9001),
    rpcUrl: () => process.env.EVMOS_MAINNET_URL!,
    rpcUrlId: 'EVMOS_MAINNET_URL',
    blockexplorers: {},
    currency: 'EVMOS',
    dripSize: '1',
    dripVersion: 2,
    networkType: 'Mainnet',
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'evmos_testnet',
    displayName: 'Evmos Testnet',
    chainId: BigInt(9000),
    rpcUrl: () => process.env.EVMOS_TESTNET_URL!,
    rpcUrlId: 'EVMOS_TESTNET_URL',
    blockexplorers: {},
    currency: 'EVMOS',
    dripSize: '0.015',
    networkType: 'Testnet',
    dripVersion: 3,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'kava',
    displayName: 'Kava',
    chainId: BigInt(2222),
    rpcUrl: () => process.env.KAVA_MAINNET_URL!,
    rpcUrlId: 'KAVA_MAINNET_URL',
    blockexplorers: {
      blockscout: {
        apiURL: 'https://kavascan.com/api',
        browserURL: 'https://kavascan.com',
        // key is not required on this network
        envKey: 'KAVA_ETHERSCAN_API_KEY',
        selfHosted: true,
      },
    },
    currency: 'KAVA',
    dripSize: '1',
    dripVersion: 2,
    networkType: 'Mainnet',
    legacyTx: true,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'kava_testnet',
    displayName: 'Kava Testnet',
    chainId: BigInt(2221),
    rpcUrl: () => process.env.KAVA_TESTNET_URL!,
    rpcUrlId: 'KAVA_TESTNET_URL',
    blockexplorers: {},
    currency: 'KAVA',
    dripSize: '1',
    networkType: 'Testnet',
    dripVersion: 2,
    legacyTx: true,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'scroll',
    displayName: 'Scroll',
    chainId: BigInt(534352),
    rpcUrl: () => process.env.SCROLL_MAINNET_RPC_URL!,
    rpcUrlId: 'SCROLL_MAINNET_RPC_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api.scrollscan.com/api',
        browserURL: 'https://scrollscan.com/',
        envKey: 'SCROLL_ETHERSCAN_API_KEY',
      },
      blockscout: {
        apiURL: 'https://blockscout.scroll.io/api',
        browserURL: 'https://blockscout.scroll.io/api',
        // key is not required on this network
        envKey: 'SCROLL_BLOCKSCOUT_API_KEY',
        selfHosted: true,
      },
    },
    currency: 'ETH',
    dripSize: '0.025',
    networkType: 'Mainnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: false,
  },
  {
    name: 'scroll_sepolia',
    displayName: 'Scroll Sepolia',
    chainId: BigInt(534351),
    rpcUrl: () => process.env.SCROLL_SEPOLIA_URL!,
    rpcUrlId: 'SCROLL_SEPOLIA_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-sepolia.scrollscan.com/api',
        browserURL: 'https://sepolia.scrollscan.com/',
        envKey: 'SCROLL_ETHERSCAN_API_KEY',
      },
    },
    currency: 'ETH',
    dripSize: '0.15',
    networkType: 'Testnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: false,
  },
  {
    name: 'rootstock',
    displayName: 'Rootstock',
    chainId: BigInt(30),
    rpcUrl: () => process.env.ROOTSTOCK_MAINNET_URL!,
    rpcUrlId: 'ROOTSTOCK_MAINNET_URL',
    blockexplorers: {
      blockscout: {
        apiURL: 'https://rootstock.blockscout.com/api',
        browserURL: 'https://rootstock.blockscout.com/',
        envKey: 'ROOTSTOCK_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'RBTC',
    dripSize: '0.001',
    dripVersion: 4,
    networkType: 'Mainnet',
    legacyTx: true,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    actionGasLimitBuffer: true,
    eip2028: true,
    actionTransactionBatching: false,
  },
  {
    name: 'rootstock_testnet',
    displayName: 'Rootstock Testnet',
    chainId: BigInt(31),
    rpcUrl: () => process.env.ROOTSTOCK_TESTNET_URL!,
    rpcUrlId: 'ROOTSTOCK_TESTNET_URL',
    blockexplorers: {
      blockscout: {
        apiURL: 'https://rootstock-testnet.blockscout.com/api',
        browserURL: 'https://rootstock-testnet.blockscout.com/',
        envKey: 'ROOTSTOCK_TESTNET_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'RBTC',
    dripSize: '0.001',
    dripVersion: 4,
    networkType: 'Testnet',
    legacyTx: true,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    actionGasLimitBuffer: true,
    eip2028: true,
    actionTransactionBatching: false,
  },
  {
    name: 'zora',
    displayName: 'Zora',
    chainId: BigInt(7777777),
    rpcUrl: () => process.env.ZORA_MAINNET_URL!,
    rpcUrlId: 'ZORA_MAINNET_URL',
    blockexplorers: {
      blockscout: {
        apiURL: 'https://explorer.zora.energy/api',
        browserURL: 'https://explorer.zora.energy/',
        // key is not necessary on this network
        envKey: 'ZORA_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'ETH',
    dripSize: '0.025',
    networkType: 'Mainnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    rollupStack: {
      provider: 'Conduit',
      type: 'OP Stack',
    },
    actionTransactionBatching: true,
  },
  {
    name: 'zora_sepolia',
    displayName: 'Zora Sepolia',
    chainId: BigInt(999999999),
    rpcUrl: () => process.env.ZORA_SEPOLIA_URL!,
    rpcUrlId: 'ZORA_SEPOLIA_URL',
    blockexplorers: {
      blockscout: {
        apiURL: 'https://sepolia.explorer.zora.energy/api',
        browserURL: 'https://sepolia.explorer.zora.energy/',
        // key is not necessary on this network
        envKey: 'ZORA_SEPOLIA_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'ETH',
    dripSize: '0.15',
    networkType: 'Testnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    rollupStack: {
      provider: 'Conduit',
      type: 'OP Stack',
    },
    actionTransactionBatching: true,
  },
  {
    name: 'rari',
    displayName: 'RARI',
    chainId: BigInt(1380012617),
    rpcUrl: () => process.env.RARI_MAINNET_URL!,
    rpcUrlId: 'RARI_MAINNET_URL',
    blockexplorers: {
      blockscout: {
        apiURL: 'https://mainnet.explorer.rarichain.org/api',
        browserURL: 'https://mainnet.explorer.rarichain.org/',
        // key is not necessary on this network
        envKey: 'RARI_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'ETH',
    dripSize: '0.025',
    networkType: 'Mainnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    rollupStack: {
      provider: 'Caldera',
      type: 'Arbitrum',
    },
    actionTransactionBatching: false,
  },
  {
    name: 'rari_sepolia',
    displayName: 'RARI Sepolia',
    chainId: BigInt(1918988905),
    rpcUrl: () => process.env.RARI_SEPOLIA_URL!,
    rpcUrlId: 'RARI_SEPOLIA_URL',
    blockexplorers: {
      blockscout: {
        apiURL: 'https://explorer.rarichain.org/api',
        browserURL: 'https://explorer.rarichain.org/',
        // key is not necessary on this network
        envKey: 'RARI_SEPOLIA_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'ETH',
    dripSize: '0.15',
    networkType: 'Testnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    rollupStack: {
      provider: 'Caldera',
      type: 'Arbitrum',
    },
    actionTransactionBatching: false,
  },
  {
    name: 'blast_sepolia',
    displayName: 'Blast Sepolia',
    chainId: BigInt(168587773),
    rpcUrl: () => process.env.BLAST_SEPOLIA_URL!,
    rpcUrlId: 'BLAST_SEPOLIA_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-sepolia.blastscan.io/api',
        browserURL: 'https://sepolia.blastscan.io/',
        envKey: 'BLAST_ETHERSCAN_API_KEY',
      },
    },
    currency: 'ETH',
    dripSize: '0.025',
    networkType: 'Testnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'blast',
    displayName: 'Blast',
    chainId: BigInt(81457),
    rpcUrl: () => process.env.BLAST_MAINNET_URL!,
    rpcUrlId: 'BLAST_MAINNET_URL',
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api.blastscan.io/api',
        browserURL: 'https://blastscan.io/',
        envKey: 'BLAST_ETHERSCAN_API_KEY',
      },
    },
    currency: 'ETH',
    dripSize: '0.025',
    networkType: 'Mainnet',
    dripVersion: 2,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'taiko_katla',
    displayName: 'Taiko Katla',
    chainId: BigInt(167008),
    rpcUrl: () => process.env.TAIKO_KATLA_URL!,
    rpcUrlId: 'TAIKO_KATLA_URL',
    blockexplorers: {},
    currency: 'ETH',
    dripSize: '0.15',
    networkType: 'Testnet',
    dripVersion: 0,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'mode_sepolia',
    displayName: 'Mode Sepolia',
    chainId: BigInt(919),
    rpcUrl: () => process.env.MODE_SEPOLIA_URL!,
    rpcUrlId: 'MODE_SEPOLIA_URL',
    blockexplorers: {
      blockscout: {
        apiURL: 'https://sepolia.explorer.mode.network/api',
        browserURL: 'https://sepolia.explorer.mode.network/',
        envKey: 'MODE_SEPOLIA_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'ETH',
    dripSize: '0.15',
    networkType: 'Testnet',
    dripVersion: 0,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'mode',
    displayName: 'Mode',
    chainId: BigInt(34443),
    rpcUrl: () => process.env.MODE_MAINNET_URL!,
    rpcUrlId: 'MODE_MAINNET_URL',
    blockexplorers: {
      blockscout: {
        apiURL: 'https://explorer.mode.network/api',
        browserURL: 'https://explorer.mode.network/',
        envKey: 'MODE_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'ETH',
    dripSize: '0.025',
    networkType: 'Mainnet',
    dripVersion: 0,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'darwinia_pangolin',
    displayName: 'Darwinia Pangolin',
    chainId: BigInt(43),
    rpcUrl: () => process.env.DARWINIA_PANGOLIN_URL!,
    rpcUrlId: 'DARWINIA_PANGOLIN_URL',
    blockexplorers: {},
    currency: 'RING',
    dripSize: '1',
    networkType: 'Testnet',
    dripVersion: 0,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    hardcodedMerkleLeafGas: (11200000).toString(),
    actionTransactionBatching: false,
  },
  {
    name: 'mantle_sepolia',
    displayName: 'Mantle Sepolia',
    chainId: BigInt(5003),
    rpcUrl: () => process.env.MANTLE_SEPOLIA_URL!,
    rpcUrlId: 'MANTLE_SEPOLIA_URL',
    blockexplorers: {
      blockscout: {
        browserURL: 'https://explorer.sepolia.mantle.xyz/',
        apiURL: 'https://explorer.sepolia.mantle.xyz/api',
        envKey: 'MANTLE_SEPOLIA_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'MNT',
    dripSize: '1',
    networkType: 'Testnet',
    dripVersion: 0,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'mantle',
    displayName: 'Mantle',
    chainId: BigInt(5000),
    rpcUrl: () => process.env.MANTLE_MAINNET_URL!,
    rpcUrlId: 'MANTLE_MAINNET_URL',
    blockexplorers: {
      blockscout: {
        browserURL: 'https://explorer.mantle.xyz/',
        apiURL: 'https://explorer.mantle.xyz/api',
        envKey: 'MANTLE_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'MNT',
    dripSize: '1',
    networkType: 'Mainnet',
    dripVersion: 0,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: true,
  },
  {
    name: 'astar_zkyoto',
    displayName: 'Astar zkEVM zkyoto',
    chainId: BigInt(6038361),
    rpcUrl: () => process.env.ASTAR_ZKYOTO_URL!,
    rpcUrlId: 'ASTAR_ZKYOTO_URL',
    blockexplorers: {
      blockscout: {
        browserURL: 'https://zkyoto.explorer.startale.com/',
        apiURL: 'https://zkyoto.explorer.startale.com/api',
        envKey: 'ASTAR_ZKYOTO_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'ETH',
    dripSize: '0.15',
    networkType: 'Testnet',
    dripVersion: 0,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: false,
  },
  {
    name: 'astar',
    displayName: 'Astar zkEVM',
    chainId: BigInt(3776),
    rpcUrl: () => process.env.ASTAR_MAINNET_URL!,
    rpcUrlId: 'ASTAR_MAINNET_URL',
    blockexplorers: {
      blockscout: {
        browserURL: 'https://astar-zkevm.explorer.startale.com/',
        apiURL: 'https://astar-zkevm.explorer.startale.com/api',
        envKey: 'ASTAR_BLOCKSCOUT_API_KEY',
        selfHosted: false,
      },
    },
    currency: 'ETH',
    dripSize: '0.025',
    networkType: 'Mainnet',
    dripVersion: 0,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    actionTransactionBatching: false,
  },
  {
    name: 'crab',
    displayName: 'Crab',
    chainId: BigInt(44),
    rpcUrl: () => process.env.DARWINIA_CRAB_MAINNET_URL!,
    rpcUrlId: 'DARWINIA_CRAB_MAINNET_URL',
    blockexplorers: {},
    currency: 'CRAB',
    dripSize: '1',
    networkType: 'Mainnet',
    dripVersion: 0,
    decimals: 18,
    queryFilterBlockLimit: 2000,
    legacyTx: false,
    actionGasLimitBuffer: false,
    eip2028: true,
    hardcodedMerkleLeafGas: (11200000).toString(),
    actionTransactionBatching: false,
  },
]

export const DEPRECATED_SPHINX_NETWORKS = [
  {
    name: 'goerli',
    chainId: BigInt(5),
    blockexplorers: {
      etherscan: {
        browserURL: 'https://goerli.etherscan.io',
        blockExplorer: 'Etherscan',
      },
      blockscout: undefined,
    },
  },
  {
    name: 'arbitrum_goerli',
    chainId: BigInt(421613),
    blockexplorers: {
      etherscan: {
        browserURL: 'https://goerli.arbiscan.io/',
        blockExplorer: 'Etherscan',
      },
      blockscout: undefined,
    },
  },
  {
    name: 'optimism_goerli',
    chainId: BigInt(420),
    blockexplorers: {
      etherscan: {
        browserURL: 'https://goerli-optimism.etherscan.io/',
        blockExplorer: 'Etherscan',
      },
      blockscout: undefined,
    },
  },
  {
    name: 'base_goerli',
    chainId: BigInt(84531),
    blockexplorers: {
      etherscan: {
        browserURL: 'https://goerli.basescan.org/',
        blockExplorer: 'Etherscan',
      },
      blockscout: undefined,
    },
  },
  {
    name: 'oktc',
    chainId: BigInt(66),
    blockexplorers: {},
  },
  {
    name: 'linea_goerli',
    chainId: BigInt(59140),
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-goerli.lineascan.build/api',
        browserURL: 'https://goerli.lineascan.build',
        envKey: 'LINEA_ETHERSCAN_API_KEY',
      },
    },
  },
  {
    name: 'polygon_zkevm_goerli',
    chainId: BigInt(1442),
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-testnet-zkevm.polygonscan.com/api',
        browserURL: 'https://testnet-zkevm.polygonscan.com',
        envKey: 'POLYGON_ZKEVM_ETHERSCAN_API_KEY',
      },
    },
  },
  {
    name: 'polygon_mumbai',
    chainId: BigInt(80001),
    blockexplorers: {
      etherscan: {
        apiURL: 'https://api-testnet.polygonscan.com/api',
        browserURL: 'https://mumbai.polygonscan.com/',
        envKey: 'POLYGON_ETHERSCAN_API_KEY',
      },
    },
  },
]
