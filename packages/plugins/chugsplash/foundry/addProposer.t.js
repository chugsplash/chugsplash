const { ethers } = require('ethers')

const projectName = 'Add proposer test'

module.exports = {
  // Configuration options for the project:
  options: {
    organizationID: ethers.utils.keccak256(
      ethers.utils.toUtf8Bytes(projectName)
    ),
    projectName,
  },
  contracts: {
    MySimpleStorage: {
      contract: 'SimpleStorage',
      variables: {
        myStorage: '0x1111111111111111111111111111111111111111',
        myStateless: '0x1111111111111111111111111111111111111111',
      },
    },
  },
}
