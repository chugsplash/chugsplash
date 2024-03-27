// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import {Script, console} from "sphinx-forge-std/Script.sol";
import {Sphinx} from "@sphinx-labs/contracts/contracts/foundry/Sphinx.sol";
import {SphinxConstants} from "@sphinx-labs/contracts/contracts/foundry/SphinxConstants.sol";
import {Sphinx} from "@sphinx-labs/contracts/contracts/foundry/Sphinx.sol";
import {Network} from "@sphinx-labs/contracts/contracts/foundry/SphinxPluginTypes.sol";
// import {SphinxSimulator} from "@sphinx-labs/contracts/contracts/periphery/SphinxSimulator.sol";
// import {MyContract1,MyContract2, HelloSphinx} from "../contracts/test/MyContracts.sol";
import "../contracts/test/MyContracts.sol";
import {CREATE3} from "solady/utils/CREATE3.sol";

contract Sample is Sphinx, Script, SphinxConstants {
    MyContract1 myContract;

    function configureSphinx() public override {
        sphinxConfig.projectName = "test_project";
        sphinxConfig.owners = [0x4856e043a1F2CAA8aCEfd076328b4981Aca91000];
        sphinxConfig.threshold = 1;
        sphinxConfig.orgId = "clksrkg1v0001l00815670lu8";
        sphinxConfig.saltNonce = 213222412;
        sphinxConfig.mainnets = [
            'kava',
            'rootstock',
            'rari',
            'optimism_mainnet',
            'celo',
            'evmos'
        ];
        sphinxConfig.testnets = [
            // 'sepolia',
            // 'arbitrum_sepolia',
            // 'bnb_testnet',
            // 'linea_goerli',
            // 'avalanche_fuji',
            // 'base_sepolia',
            // 'moonbase_alpha',
            'kava_testnet',
            'rootstock_testnet',
            'rari_sepolia',
            'optimism_sepolia',
            // 'polygon_mumbai',
            // 'gnosis_chiado',
            // 'polygon_zkevm_goerli',
            // 'fantom_testnet',
            'celo_alfajores',
            'evmos_testnet'
            // 'scroll_sepolia',
            // 'zora_sepolia',
            // 'blast_sepolia'
        ];
    }

    function run() public sphinx {
        new MyContract1(
            -1,
            2,
            address(1),
            address(2)
        );
        new MyContract1(
            -1,
            2,
            address(1),
            address(2)
        );
        new MyContract1(
            -1,
            2,
            address(1),
            address(2)
        );
    }

    function simulator() public sphinx {
        // vm.startBroadcast(vm.envUint("PRIVATE_KEY"));
        // new SphinxSimulator{ salt: 0 }(safeFactoryAddress, safeSingletonAddress);

        bytes memory initCodeWithArgs = hex"60a06040523073ffffffffffffffffffffffffffffffffffffffff1660809073ffffffffffffffffffffffffffffffffffffffff1660601b8152503480156200004757600080fd5b5060405162001d9c38038062001d9c83398181016040528101906200006d91906200010e565b81600160006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff16021790555080600260006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff16021790555050506200019d565b600081519050620001088162000183565b92915050565b600080604083850312156200012257600080fd5b60006200013285828601620000f7565b92505060206200014585828601620000f7565b9150509250929050565b60006200015c8262000163565b9050919050565b600073ffffffffffffffffffffffffffffffffffffffff82169050919050565b6200018e816200014f565b81146200019a57600080fd5b50565b60805160601c611be0620001bc60003960006105050152611be06000f3fe608060405234801561001057600080fd5b506004361061004c5760003560e01c8063402c12ae146100515780638d3e8cdb146100815780638d6d6a16146100b1578063edcee730146100e1575b600080fd5b61006b60048036038101906100669190610f6c565b610111565b60405161007891906115b2565b60405180910390f35b61009b60048036038101906100969190610f6c565b6102c3565b6040516100a891906115b2565b60405180910390f35b6100cb60048036038101906100c6919061103b565b61031a565b6040516100d891906115cd565b60405180910390f35b6100fb60048036038101906100f69190610fe7565b6106e2565b6040516101089190611590565b60405180910390f35b6000600173ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff16141580156101db5750600073ffffffffffffffffffffffffffffffffffffffff166000803373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1614155b61021a576040517f08c379a000000000000000000000000000000000000000000000000000000000815260040161021190611671565b60405180910390fd5b610227858585855a610be2565b90508015610277573373ffffffffffffffffffffffffffffffffffffffff167f6895c13664aa4f67288b25d7a21d7aaa34916e355fb9b6fae0a139a9085becb860405160405180910390a26102bb565b3373ffffffffffffffffffffffffffffffffffffffff167facd2c8702804128fdb0db2bb49f6d127dd0181c13fd45dbfe16de0930e2bd37560405160405180910390a25b949350505050565b60007fb648d3644f584ed1c2232d53c46d87e693586486ad0d1175f8656013110b714e33868686866040516102fc95949392919061144c565b60405180910390a161031085858585610111565b9050949350505050565b606061035a6040518060400160405280600381526020017f4141410000000000000000000000000000000000000000000000000000000000815250610c88565b60008473ffffffffffffffffffffffffffffffffffffffff163b14156104a957600160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16631688f0b9600260009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1685856040518463ffffffff1660e01b81526004016103fb939291906114d6565b602060405180830381600087803b15801561041557600080fd5b505af1158015610429573d6000803e3d6000fd5b505050506040513d601f19601f8201168201806040525081019061044d91906110f7565b5060008473ffffffffffffffffffffffffffffffffffffffff163b116104a8576040517f08c379a000000000000000000000000000000000000000000000000000000000815260040161049f90611611565b60405180910390fd5b5b6104e76040518060400160405280600381526020017f4242420000000000000000000000000000000000000000000000000000000000815250610c88565b8373ffffffffffffffffffffffffffffffffffffffff1663b4faba097f000000000000000000000000000000000000000000000000000000000000000063edcee73060e01b888860405160240161053f929190611560565b604051602081830303815290604052907bffffffffffffffffffffffffffffffffffffffffffffffffffffffff19166020820180517bffffffffffffffffffffffffffffffffffffffffffffffffffffffff83818316178352505050506040518363ffffffff1660e01b81526004016105b99291906114a6565b600060405180830381600087803b1580156105d357600080fd5b505af19250505080156105e4575060015b610661573d8060008114610614576040519150601f19603f3d011682016040523d82523d6000602084013e610619565b606091505b506106586040518060400160405280600381526020017f4444440000000000000000000000000000000000000000000000000000000000815250610c88565b809150506106da565b61069f6040518060400160405280600381526020017f4343430000000000000000000000000000000000000000000000000000000000815250610c88565b6040517f08c379a00000000000000000000000000000000000000000000000000000000081526004016106d190611651565b60405180910390fd5b949350505050565b60606107226040518060400160405280600381526020017f4545450000000000000000000000000000000000000000000000000000000000815250610c88565b8173ffffffffffffffffffffffffffffffffffffffff163073ffffffffffffffffffffffffffffffffffffffff1614610790576040517f08c379a000000000000000000000000000000000000000000000000000000000815260040161078790611631565b60405180910390fd5b6107ce6040518060400160405280600381526020017f4646460000000000000000000000000000000000000000000000000000000000815250610c88565b8173ffffffffffffffffffffffffffffffffffffffff1663610b5925306040518263ffffffff1660e01b81526004016108079190611431565b600060405180830381600087803b15801561082157600080fd5b505af1158015610835573d6000803e3d6000fd5b50505050825167ffffffffffffffff81111561087a577f4e487b7100000000000000000000000000000000000000000000000000000000600052604160045260246000fd5b6040519080825280602002602001820160405280156108a85781602001602082028036833780820191505090505b5090506000806060600080600090505b8751811015610b99576108ff6040518060400160405280600381526020017f4747470000000000000000000000000000000000000000000000000000000000815250610c88565b600088828151811061093a577f4e487b7100000000000000000000000000000000000000000000000000000000600052603260045260246000fd5b602002602001015190508060000151955080602001519450806040015193508060600151925060005a90506109a36040518060400160405280600381526020017f4848480000000000000000000000000000000000000000000000000000000000815250610c88565b60008973ffffffffffffffffffffffffffffffffffffffff1663468721a7898989896040518563ffffffff1660e01b81526004016109e49493929190611514565b602060405180830381600087803b1580156109fe57600080fd5b505af1158015610a12573d6000803e3d6000fd5b505050506040513d601f19601f82011682018060405250810190610a3691906110ce565b9050610a766040518060400160405280600381526020017f4949490000000000000000000000000000000000000000000000000000000000815250610c88565b5a82610a8291906117ce565b898581518110610abb577f4e487b7100000000000000000000000000000000000000000000000000000000600052603260045260246000fd5b602002602001018181525050610b056040518060400160405280600381526020017f4a4a4a0000000000000000000000000000000000000000000000000000000000815250610c88565b80610b45576040517f08c379a0000000000000000000000000000000000000000000000000000000008152600401610b3c90611671565b60405180910390fd5b610b836040518060400160405280600381526020017f4b4b4b0000000000000000000000000000000000000000000000000000000000815250610c88565b5050508080610b919061193c565b9150506108b8565b50610bd86040518060400160405280600381526020017f4c4c4c0000000000000000000000000000000000000000000000000000000000815250610c88565b5050505092915050565b6000600180811115610c1d577f4e487b7100000000000000000000000000000000000000000000000000000000600052602160045260246000fd5b836001811115610c56577f4e487b7100000000000000000000000000000000000000000000000000000000600052602160045260246000fd5b1415610c6f576000808551602087018986f49050610c7f565b600080855160208701888a87f190505b95945050505050565b610d1e81604051602401610c9c91906115ef565b6040516020818303038152906040527f41304fac000000000000000000000000000000000000000000000000000000007bffffffffffffffffffffffffffffffffffffffffffffffffffffffff19166020820180517bffffffffffffffffffffffffffffffffffffffffffffffffffffffff8381831617835250505050610d21565b50565b60008151905060006a636f6e736f6c652e6c6f679050602083016000808483855afa5050505050565b6000610d5d610d58846116b6565b611691565b90508083825260208201905082856020860282011115610d7c57600080fd5b60005b85811015610dc657813567ffffffffffffffff811115610d9e57600080fd5b808601610dab8982610ecb565b85526020850194506020840193505050600181019050610d7f565b5050509392505050565b6000610de3610dde846116e2565b611691565b905082815260208101848484011115610dfb57600080fd5b610e068482856118c9565b509392505050565b600081359050610e1d81611b27565b92915050565b600082601f830112610e3457600080fd5b8135610e44848260208601610d4a565b91505092915050565b600081519050610e5c81611b3e565b92915050565b600082601f830112610e7357600080fd5b8135610e83848260208601610dd0565b91505092915050565b600081519050610e9b81611b55565b92915050565b600081359050610eb081611b6c565b92915050565b600081359050610ec581611b83565b92915050565b600060808284031215610edd57600080fd5b610ee76080611691565b90506000610ef784828501610e0e565b6000830152506020610f0b84828501610f57565b602083015250604082013567ffffffffffffffff811115610f2b57600080fd5b610f3784828501610e62565b6040830152506060610f4b84828501610eb6565b60608301525092915050565b600081359050610f6681611b93565b92915050565b60008060008060808587031215610f8257600080fd5b6000610f9087828801610e0e565b9450506020610fa187828801610f57565b935050604085013567ffffffffffffffff811115610fbe57600080fd5b610fca87828801610e62565b9250506060610fdb87828801610eb6565b91505092959194509250565b60008060408385031215610ffa57600080fd5b600083013567ffffffffffffffff81111561101457600080fd5b61102085828601610e23565b925050602061103185828601610ea1565b9150509250929050565b6000806000806080858703121561105157600080fd5b600085013567ffffffffffffffff81111561106b57600080fd5b61107787828801610e23565b945050602061108887828801610ea1565b935050604085013567ffffffffffffffff8111156110a557600080fd5b6110b187828801610e62565b92505060606110c287828801610f57565b91505092959194509250565b6000602082840312156110e057600080fd5b60006110ee84828501610e4d565b91505092915050565b60006020828403121561110957600080fd5b600061111784828501610e8c565b91505092915050565b600061112c83836113b0565b905092915050565b60006111408383611413565b60208301905092915050565b61115581611802565b82525050565b61116481611802565b82525050565b600061117582611733565b61117f8185611779565b93508360208202850161119185611713565b8060005b858110156111cd57848403895281516111ae8582611120565b94506111b98361175f565b925060208a01995050600181019050611195565b50829750879550505050505092915050565b60006111ea8261173e565b6111f4818561178a565b93506111ff83611723565b8060005b838110156112305781516112178882611134565b97506112228361176c565b925050600181019050611203565b5085935050505092915050565b61124681611826565b82525050565b600061125782611749565b611261818561179b565b93506112718185602086016118d8565b61127a81611a12565b840191505092915050565b600061129082611749565b61129a81856117ac565b93506112aa8185602086016118d8565b6112b381611a12565b840191505092915050565b6112c781611893565b82525050565b6112d6816118b7565b82525050565b6112e5816118b7565b82525050565b60006112f682611754565b61130081856117bd565b93506113108185602086016118d8565b61131981611a12565b840191505092915050565b6000611331600a836117bd565b915061133c82611a23565b602082019050919050565b60006113546030836117bd565b915061135f82611a4c565b604082019050919050565b6000611377602a836117bd565b915061138282611a9b565b604082019050919050565b600061139a601b836117bd565b91506113a582611aea565b602082019050919050565b60006080830160008301516113c8600086018261114c565b5060208301516113db6020860182611413565b50604083015184820360408601526113f3828261124c565b915050606083015161140860608601826112cd565b508091505092915050565b61141c81611889565b82525050565b61142b81611889565b82525050565b6000602082019050611446600083018461115b565b92915050565b600060a082019050611461600083018861115b565b61146e602083018761115b565b61147b6040830186611422565b818103606083015261148d8185611285565b905061149c60808301846112dc565b9695505050505050565b60006040820190506114bb600083018561115b565b81810360208301526114cd8184611285565b90509392505050565b60006060820190506114eb600083018661115b565b81810360208301526114fd8185611285565b905061150c6040830184611422565b949350505050565b6000608082019050611529600083018761115b565b6115366020830186611422565b81810360408301526115488185611285565b905061155760608301846112dc565b95945050505050565b6000604082019050818103600083015261157a818561116a565b905061158960208301846112be565b9392505050565b600060208201905081810360008301526115aa81846111df565b905092915050565b60006020820190506115c7600083018461123d565b92915050565b600060208201905081810360008301526115e78184611285565b905092915050565b6000602082019050818103600083015261160981846112eb565b905092915050565b6000602082019050818103600083015261162a81611324565b9050919050565b6000602082019050818103600083015261164a81611347565b9050919050565b6000602082019050818103600083015261166a8161136a565b9050919050565b6000602082019050818103600083015261168a8161138d565b9050919050565b600061169b6116ac565b90506116a7828261190b565b919050565b6000604051905090565b600067ffffffffffffffff8211156116d1576116d06119e3565b5b602082029050602081019050919050565b600067ffffffffffffffff8211156116fd576116fc6119e3565b5b61170682611a12565b9050602081019050919050565b6000819050602082019050919050565b6000819050602082019050919050565b600081519050919050565b600081519050919050565b600081519050919050565b600081519050919050565b6000602082019050919050565b6000602082019050919050565b600082825260208201905092915050565b600082825260208201905092915050565b600082825260208201905092915050565b600082825260208201905092915050565b600082825260208201905092915050565b60006117d982611889565b91506117e483611889565b9250828210156117f7576117f6611985565b5b828203905092915050565b600061180d82611869565b9050919050565b600061181f82611869565b9050919050565b60008115159050919050565b600061183d82611814565b9050919050565b600061184f82611814565b9050919050565b600081905061186482611b13565b919050565b600073ffffffffffffffffffffffffffffffffffffffff82169050919050565b6000819050919050565b600061189e826118a5565b9050919050565b60006118b082611869565b9050919050565b60006118c282611856565b9050919050565b82818337600083830152505050565b60005b838110156118f65780820151818401526020810190506118db565b83811115611905576000848401525b50505050565b61191482611a12565b810181811067ffffffffffffffff82111715611933576119326119e3565b5b80604052505050565b600061194782611889565b91507fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff82141561197a57611979611985565b5b600182019050919050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601160045260246000fd5b7f4e487b7100000000000000000000000000000000000000000000000000000000600052602160045260246000fd5b7f4e487b7100000000000000000000000000000000000000000000000000000000600052604160045260246000fd5b6000601f19601f8301169050919050565b7f544f444f28646f63732900000000000000000000000000000000000000000000600082015250565b7f544f444f28646f6373293a206d7573742062652064656c656761746563616c6c60008201527f656420627920736166652070726f787900000000000000000000000000000000602082015250565b7f537068696e7853696d756c61746f723a2073696d756c6174696f6e206e65766560008201527f7220726576657274656400000000000000000000000000000000000000000000602082015250565b7f537068696e7853696d756c61746f723a20544f444f28646f6373290000000000600082015250565b60028110611b2457611b236119b4565b5b50565b611b3081611802565b8114611b3b57600080fd5b50565b611b4781611826565b8114611b5257600080fd5b50565b611b5e81611832565b8114611b6957600080fd5b50565b611b7581611844565b8114611b8057600080fd5b50565b60028110611b9057600080fd5b50565b611b9c81611889565b8114611ba757600080fd5b5056fea2646970667358221220a15423dbdda25b103b8a4e0434cc4f5f4843113dfff8d158cb79b902463d78d564736f6c63430008040033000000000000000000000000a6b71e26c5e0845f74c812102ca7114b6a896ab2000000000000000000000000d9db270c1b5e3bd161e8c8503c55ceabee709552";
        address addr;
        assembly {
            addr := create2(0, add(initCodeWithArgs, 0x20), mload(initCodeWithArgs), 0x0)
        }
        console.log(addr);
    }
}
