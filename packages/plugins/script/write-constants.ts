import hre from 'hardhat'
import '@nomicfoundation/hardhat-ethers'
import { ManagedServiceArtifact } from '@sphinx-labs/contracts'
import {
  getSphinxConstants,
  remove0x,
  getManagedServiceAddress,
  getManagedServiceConstructorArgs,
  getSphinxModuleFactoryAddress,
  parseFoundryArtifact,
  getGnosisSafeProxyFactoryAddress,
} from '@sphinx-labs/core'
import { ethers } from 'ethers'

/**
 * Writes various constant values to a Solidity contract. This improves the speed of the Foundry
 * plugin by reducing the number of times we read need to read JSON files or do FFI calls.
 * This script can be called by running:
 * npx ts-node src/scripts/write-constants.ts
 *
 * The output can be written to a file by appending this CLI command with: `> fileName.json`.
 */
const writeConstants = async () => {
  const constants = {
    sphinxModuleFactoryAddress: {
      type: 'address',
      value: getSphinxModuleFactoryAddress(),
    },
    managedServiceAddressOptimism: {
      type: 'address',
      value: getManagedServiceAddress(10n),
    },
    managedServiceAddressOptimismGoerli: {
      type: 'address',
      value: getManagedServiceAddress(420n),
    },
    managedServiceAddressStandard: {
      type: 'address',
      value: getManagedServiceAddress(1n),
    },
  }

  const sphinxConstants = await getSphinxConstants(hre.ethers.provider)

  // Add the manager contract info for specifically optimism and optimism goerli
  // where the address is different from the rest of the networks.
  // We do not include these in the above getSphinxConstants function b/c that function
  // is also used by our live network deployment process so including three copies of the
  // Sphinx manager contract info would be redundant and potentially cause errors.
  sphinxConstants.push(
    ...[
      {
        artifact: parseFoundryArtifact(ManagedServiceArtifact),
        expectedAddress: getManagedServiceAddress(10n),
        constructorArgs: getManagedServiceConstructorArgs(10n),
      },
      {
        artifact: parseFoundryArtifact(ManagedServiceArtifact),
        expectedAddress: getManagedServiceAddress(420n),
        constructorArgs: getManagedServiceConstructorArgs(420n),
      },
    ]
  )

  const contractInfo = sphinxConstants.map(
    ({ artifact, constructorArgs, expectedAddress }) => {
      const { abi, bytecode } = artifact

      const iface = new ethers.Interface(abi)

      const creationCode = bytecode.concat(
        remove0x(iface.encodeDeploy(constructorArgs))
      )

      return { creationCode, expectedAddress }
    }
  )

  const solidityFile =
    `// SPDX-License-Identifier: MIT\n` +
    `pragma solidity >=0.6.2 <0.9.0;\n\n` +
    `struct SphinxContractInfo {\n` +
    `  bytes creationCode;\n` +
    `  address expectedAddress;\n` +
    `}\n\n` +
    `contract SphinxConstants {\n` +
    `${Object.entries(constants)
      .map(([name, { type, value }]) => {
        if (type === 'bytes') {
          // We must use the hex"..." format instead of 0x... for dynamic bytes because Solidity
          // prevents these types from using the latter format. Only fixed-size bytes, e.g. bytes32,
          // can the 0x... format.
          return `  ${type} public constant ${name} = hex"${remove0x(
            value.toString()
          )}";`
        } else {
          return `  ${type} public constant ${name} = ${value};`
        }
      })
      .join('\n')}\n\n` +
    `  function getSphinxContractInfo() public pure returns (SphinxContractInfo[] memory) {\n` +
    `    SphinxContractInfo[] memory contracts = new SphinxContractInfo[](${contractInfo.length});\n` +
    `${contractInfo
      .map(
        ({ creationCode, expectedAddress }, i) =>
          `    contracts[${i}] = SphinxContractInfo(hex"${remove0x(
            creationCode
          )}", ${expectedAddress});`
      )
      .join('\n')}\n` +
    `    return contracts;\n  }` +
    `\n}`

  process.stdout.write(solidityFile)
}

writeConstants()
