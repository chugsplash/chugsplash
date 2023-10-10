import path from 'path'
import fs, { readdirSync } from 'fs'
import { spawnSync } from 'child_process'

import { astDereferencer, findAll } from 'solidity-ast/utils'
import {
  ContractDefinition,
  FunctionDefinition,
  InheritanceSpecifier,
  SourceUnit,
} from 'solidity-ast/types'
import ora from 'ora'
import { ConfigArtifacts } from '@sphinx-labs/core'

import {
  generateDeploymentFunctionFromASTDefinition,
  generateFunctionFromASTDefinition,
} from './functions'
import { getFoundryConfigOptions } from '../../foundry/options'
import { fetchUniqueTypeName } from './imports'
import { makeGetConfigArtifacts } from '../../foundry/utils'
import { fetchSourceContractNames } from './artifacts'

// Maybe:
// - handle using contract clients when the input type is a contract instead of just converting it to an address
// - handle if an external contract is used for an input/output value in a function or constructor

const CLIENT_FOLDER_NAME = 'client'

const generateFunctionsForParentContracts = async (
  baseContracts: InheritanceSpecifier[],
  remappings: Record<string, string>,
  allDeployFunctionImports: Record<string, string>,
  clientPath: string,
  artifactFolder: string,
  src: string,
  fullyQualifiedContractName: string,
  configArtifacts: ConfigArtifacts,
  functionSelectors: string[]
) => {
  const parentImports: Record<string, string> = {}
  const parentFunctionDefinitions: string[] = []

  const buildInfo = configArtifacts[fullyQualifiedContractName].buildInfo

  const deref = astDereferencer(buildInfo.output)

  for (const contract of baseContracts) {
    // Dereference source contract
    const { node, sourceUnit } = deref.withSourceUnit(
      'ContractDefinition',
      contract.baseName.referencedDeclaration
    )

    if (node.contractKind === 'interface') {
      continue
    }

    // Fetch absolute path and generate expected artifact path from source contract
    const absolutePath = sourceUnit.absolutePath
    const artifactFile = path.join(
      artifactFolder,
      path.basename(absolutePath),
      `${node.canonicalName}.json`
    )

    let pathFromSrc = absolutePath.replace(src, '')
    if (pathFromSrc.startsWith('/')) {
      pathFromSrc = pathFromSrc.replace('/', '')
    }
    const depth = pathFromSrc.split('/').length

    const contractData = await generateClientContractFromArtifact(
      artifactFile,
      absolutePath,
      depth,
      remappings,
      allDeployFunctionImports,
      clientPath,
      src,
      artifactFolder,
      configArtifacts,
      functionSelectors
    )

    if (!contractData) {
      throw new Error(
        `Could not generate client for parent contract: ${contract.baseName.name} in file: ${fullyQualifiedContractName}. This should never happen, please report this to the developers.`
      )
    }

    for (const [localName, importString] of Object.entries(
      contractData.clientImports
    )) {
      parentImports[localName] = importString
    }

    parentFunctionDefinitions.push(...contractData.clientFunctions)
  }

  return { parentImports, parentFunctionDefinitions }
}

const generateClientContractFromArtifact = async (
  artifactFile: string,
  filePath: string,
  fileDepth: number,
  remappings: Record<string, string>,
  allDeployFunctionImports: Record<string, string>,
  clientPath: string,
  src: string,
  artifactFolder: string,
  configArtifacts: ConfigArtifacts,
  functionSelectors: string[]
) => {
  const fileName = path.basename(filePath)
  const contractName = path.basename(artifactFile).replace('.json', '')
  const clientName = `${contractName}Client`

  const uniqueClientName = fetchUniqueTypeName(
    allDeployFunctionImports,
    clientName,
    clientPath,
    filePath,
    src
  )

  const artifact = JSON.parse(fs.readFileSync(artifactFile, 'utf-8'))
  const sourceUnit: SourceUnit = artifact.ast
  const astNodes = artifact.ast.nodes
  const contractDefinition: ContractDefinition = astNodes.find(
    (node) =>
      node.nodeType === 'ContractDefinition' &&
      node.canonicalName === contractName
  )

  // If the absolute path from the artifact does not match the file path,
  // then this artifact is for a file with the same name but in a different folder
  // so we should skip it
  if (sourceUnit.absolutePath !== filePath) {
    return undefined
  }

  // If there is no contract definition, then we don't need to generate a client so we skip this file
  if (!contractDefinition) {
    return undefined
  }

  // If the definition is a library or interface, then we don't need to generate a client so we skip this file
  if (contractDefinition.contractKind !== 'contract') {
    return undefined
  }

  const functionDefinitions = contractDefinition.nodes
    // filter for only function definitions
    .filter((node) => node.nodeType === 'FunctionDefinition')
    // filter out constructor
    .filter((definition: FunctionDefinition) => definition.kind === 'function')
    // filter for only external and public functions
    .filter(
      (definition: FunctionDefinition) =>
        definition.visibility === 'external' ||
        definition.visibility === 'public'
    )
    // filter out any functions that have are mutable and accept function inputs
    .filter(
      (definition: FunctionDefinition) =>
        definition.stateMutability === 'pure' ||
        definition.parameters.parameters.filter(
          (parameter) => parameter.typeName?.nodeType === 'FunctionTypeName'
        ).length === 0
    )

  const fullyQualifiedContractName = `${filePath}:${contractName}`

  const importsAndDefinitions = functionDefinitions.map(
    (definition: FunctionDefinition) =>
      generateFunctionFromASTDefinition(
        definition,
        sourceUnit,
        filePath,
        fileDepth,
        remappings,
        src,
        fullyQualifiedContractName,
        functionSelectors
      )
  )

  const clientImports: Record<string, string> = {}
  const allFunctionDefinitions: string[] = []
  importsAndDefinitions.forEach((importAndDefinition) => {
    if (!importAndDefinition) {
      return
    }

    const { imports, functionDefinition } = importAndDefinition
    allFunctionDefinitions.push(functionDefinition)

    for (const [localName, importString] of Object.entries(imports)) {
      clientImports[localName] = importString
    }
  })

  const { parentImports, parentFunctionDefinitions } =
    await generateFunctionsForParentContracts(
      contractDefinition.baseContracts,
      remappings,
      allDeployFunctionImports,
      clientPath,
      artifactFolder,
      src,
      fullyQualifiedContractName,
      configArtifacts,
      functionSelectors
    )

  for (const [localName, importString] of Object.entries(parentImports)) {
    clientImports[localName] = importString
  }

  allFunctionDefinitions.push(...parentFunctionDefinitions)

  const clientContract = `contract ${uniqueClientName} is AbstractContractClient {
  constructor(address _sphinxManager, address _sphinx, address _impl) AbstractContractClient(_sphinxManager, _sphinx, _impl) {}

  fallback() external override {
    require(msg.sender != sphinxInternalManager, "User attempted to call a non-existent function on ${uniqueClientName}");
    _delegate(sphinxInternalImpl);
  }

  ${allFunctionDefinitions.join('')}
}
`

  const constructorDefinition: any = contractDefinition.nodes.find(
    (node) =>
      node.nodeType === 'FunctionDefinition' && node.kind === 'constructor'
  )

  const artifactPath = `${fileName}:${contractName}`
  const clientArtifactPath = `${fileName.replace(
    '.sol',
    '.c.sol'
  )}:${uniqueClientName}`

  const {
    imports: deployFunctionImports,
    functionDefinitions: deployFunctions,
  } = generateDeploymentFunctionFromASTDefinition(
    constructorDefinition,
    uniqueClientName,
    artifactPath,
    clientArtifactPath,
    fullyQualifiedContractName,
    sourceUnit,
    filePath,
    remappings,
    allDeployFunctionImports,
    src
  )

  return {
    uniqueClientName,
    clientContract,
    clientImports,
    clientFunctions: allFunctionDefinitions,
    deployFunctions,
    deployFunctionImports,
  }
}

export const generateClientForFile = async (
  filePath: string,
  artifactFolder: string,
  outputPath: string,
  fileDepth: number,
  remappings: Record<string, string>,
  allDeployFunctionImports: Record<string, string>,
  src: string,
  configArtifacts: ConfigArtifacts
) => {
  const fileName = path.basename(filePath)
  if (!fileName.endsWith('.sol')) {
    return { deployFunctionImports: {}, deployFunctions: [] }
  }

  const fileArtifactPath = path.join(artifactFolder, fileName)
  let artifactFiles
  try {
    artifactFiles = fs.readdirSync(fileArtifactPath)
  } catch (e) {
    if (e.message.includes('no such file or directory')) {
      throw new Error(
        `Could not find compiler artifact for file: ${filePath}. If this problem persists please report it to the developers.`
      )
    } else {
      throw e
    }
  }

  const contracts: Array<{
    uniqueClientName: string
    clientContract: string
    clientImports: Record<string, string>
    deployFunctions: string
    deployFunctionImports: Record<string, string>
  }> = []
  for (const file of artifactFiles) {
    const contract = await generateClientContractFromArtifact(
      path.join(artifactFolder, fileName, file),
      filePath,
      fileDepth,
      remappings,
      allDeployFunctionImports,
      outputPath,
      src,
      artifactFolder,
      configArtifacts,
      []
    )

    if (contract !== undefined) {
      contracts.push(contract)
    }
  }

  const uniqueClientNames: string[] = []
  const clientContracts: string[] = []
  const consolidatedClientImports: Record<string, string> = {}
  const consolidatedDeployFunctions: string[] = []
  const consolidatedDeployFunctionImports: Record<string, string> = {}

  // Consolidate all of the imports and contracts into arrays and objects
  // Remove any duplicate imports
  contracts.forEach((contract) => {
    const {
      uniqueClientName,
      clientContract,
      clientImports,
      deployFunctions,
      deployFunctionImports,
    } = contract
    uniqueClientNames.push(uniqueClientName)
    clientContracts.push(clientContract)
    consolidatedDeployFunctions.push(deployFunctions)

    for (const [localName, importString] of Object.entries(clientImports)) {
      consolidatedClientImports[localName] = importString
    }

    for (const [localName, importString] of Object.entries(
      deployFunctionImports
    )) {
      consolidatedDeployFunctionImports[localName] = importString
    }
  })

  // Join all of the contract sources into a single string
  const contractClientSrc = clientContracts.join('\n')

  // Add in the imports for the clients themselves
  for (const clientName of uniqueClientNames) {
    consolidatedDeployFunctionImports[
      clientName
    ] = `import { ${clientName} } from "./${outputPath.replace(
      `${CLIENT_FOLDER_NAME}/`,
      ''
    )}";`
  }

  // Generate the final contract client source file
  const clientSource = `// THIS IS AN AUTOGENERATED FILE. DO NOT EDIT THIS FILE DIRECTLY
// SPDX-License-Identifier: MIT
pragma solidity >=0.7.4 <0.9.0;

import { Sphinx } from "@sphinx-labs/plugins/Sphinx.sol";
import { AbstractContractClient } from "@sphinx-labs/plugins/AbstractContractClient.sol";
${
  Object.values(consolidatedClientImports).length > 0
    ? Object.values(consolidatedClientImports).join('\n') + '\n'
    : ''
}
${contractClientSrc}
`

  const fullOutputPath = path.resolve(outputPath)
  fs.mkdirSync(path.dirname(fullOutputPath), { recursive: true })
  fs.writeFileSync(fullOutputPath, clientSource)

  return {
    deployFunctionImports: consolidatedDeployFunctionImports,
    deployFunctions: consolidatedDeployFunctions,
    clientSource,
  }
}

export const generateClientsInFolder = async (
  folder: string,
  artifactFolder: string,
  outputPath: string,
  fileDepth: number,
  remappings: Record<string, string>,
  src: string,
  allDeployFunctionImports: Record<string, string>,
  allDeployFunctions: string[],
  configArtifacts: ConfigArtifacts
) => {
  const subdirs: string[] = []
  const files: string[] = []
  readdirSync(folder, { withFileTypes: true }).map((dirent) => {
    if (dirent.isDirectory()) {
      subdirs.push(dirent.name)
    } else {
      files.push(dirent.name)
    }
  })

  for (const subdir of subdirs) {
    const subdirOutputPath = path.join(outputPath, subdir)
    const subdirPath = path.join(folder, subdir)
    await generateClientsInFolder(
      subdirPath,
      artifactFolder,
      subdirOutputPath,
      fileDepth + 1,
      remappings,
      src,
      allDeployFunctionImports,
      allDeployFunctions,
      configArtifacts
    )
  }

  for (const file of files) {
    // Skip the sphinx external contract since it's handled separately
    if (file === 'SphinxExternal.sol') {
      continue
    }

    const filePath = path.join(folder, file)
    const outputFileName = file.replace('.sol', `.c.sol`)
    const outputFilePath = path.join(outputPath, outputFileName)
    const { deployFunctionImports, deployFunctions } =
      await generateClientForFile(
        filePath,
        artifactFolder,
        outputFilePath,
        fileDepth,
        remappings,
        allDeployFunctionImports,
        src,
        configArtifacts
      )

    for (const [localName, importString] of Object.entries(
      deployFunctionImports
    )) {
      allDeployFunctionImports[localName] = importString
    }

    allDeployFunctions.push(...deployFunctions)
  }

  return {
    deployFunctionImports: allDeployFunctionImports,
    deployFunctions: allDeployFunctions,
  }
}

export const generateClientsForExternalContracts = async (
  src: string,
  artifactFolder: string,
  outputPath: string,
  fileDepth: number,
  remappings: Record<string, string>,
  configArtifacts: ConfigArtifacts
): Promise<{
  deployFunctionImports: Record<string, string>
  deployFunctions: string[]
}> => {
  const allDeployFunctionImports: Record<string, string> = {}
  const allDeployFunctions: string[] = []

  const externalImpostsArtifactPath = path.join(
    artifactFolder,
    'SphinxExternal.sol',
    'SphinxExternal.json'
  )

  if (!fs.existsSync(externalImpostsArtifactPath)) {
    return { deployFunctionImports: {}, deployFunctions: [] }
  }

  const artifact = JSON.parse(
    fs.readFileSync(externalImpostsArtifactPath, 'utf-8')
  )

  for (const importDirective of findAll('ImportDirective', artifact.ast)) {
    const fileName = path.basename(importDirective.absolutePath)
    const clientOutputPath = path
      .join(outputPath, fileName)
      .replace('.sol', '.c.sol')
    const { deployFunctionImports, deployFunctions } =
      await generateClientForFile(
        importDirective.absolutePath,
        artifactFolder,
        clientOutputPath,
        fileDepth,
        remappings,
        allDeployFunctionImports,
        src,
        configArtifacts
      )

    for (const [localName, importString] of Object.entries(
      deployFunctionImports
    )) {
      allDeployFunctionImports[localName] = importString
    }

    allDeployFunctions.push(...deployFunctions)
  }

  return {
    deployFunctionImports: allDeployFunctionImports,
    deployFunctions: allDeployFunctions,
  }
}

const generateSphinxClient = async (
  imports: Record<string, string>,
  deployFunctions: string[],
  clientFolder: string
) => {
  const source = `// THIS IS AN AUTOGENERATED FILE. DO NOT EDIT THIS FILE DIRECTLY
// SPDX-License-Identifier: MIT
pragma solidity >=0.7.4 <0.9.0;

import { Sphinx } from "@sphinx-labs/plugins/Sphinx.sol";
import { SphinxConfig, DeployOptions, DefineOptions } from "@sphinx-labs/plugins/SphinxPluginTypes.sol";
${Object.values(imports).join('\n')}

abstract contract SphinxClient is Sphinx {
  ${deployFunctions.join('\n')}
}
`

  const fullClientFolder = path.resolve(clientFolder)
  fs.mkdirSync(path.dirname(clientFolder), { recursive: true })
  fs.writeFileSync(path.join(fullClientFolder, `SphinxClient.sol`), source)
}

export const generateClient = async () => {
  const spinner = ora()
  spinner.info('Compiling sources...')

  const { status: compilationStatusSrc } = spawnSync(
    `forge`,
    ['build', '--skip', 'test', '--skip', 'script'],
    {
      stdio: 'inherit',
    }
  )
  // Exit the process if compilation fails.
  if (compilationStatusSrc !== 0) {
    process.exit(1)
  }

  spinner.succeed('Finished compiling sources')

  spinner.start('Generating Sphinx clients...')

  const { src, artifactFolder, remappings, buildInfoFolder, cachePath } =
    await getFoundryConfigOptions()

  const getConfigArtifacts = makeGetConfigArtifacts(
    artifactFolder,
    buildInfoFolder,
    cachePath
  )

  const contractNames = await fetchSourceContractNames(artifactFolder, src)
  const configArtifacts = await getConfigArtifacts(contractNames)

  const { deployFunctionImports, deployFunctions } =
    await generateClientsForExternalContracts(
      src,
      artifactFolder,
      CLIENT_FOLDER_NAME,
      1,
      remappings,
      configArtifacts
    )

  if (!fs.existsSync(src)) {
    throw new Error(
      `The src directory: '${src}' was not found. Please check that you've defined the correct src directory in your foundry.toml file.`
    )
  }

  await generateClientsInFolder(
    src,
    artifactFolder,
    CLIENT_FOLDER_NAME,
    1,
    remappings,
    src,
    deployFunctionImports,
    deployFunctions,
    configArtifacts
  )

  await generateSphinxClient(
    deployFunctionImports,
    deployFunctions,
    CLIENT_FOLDER_NAME
  )

  spinner.succeed('Generated Sphinx clients')
  spinner.info('Compiling clients and scripts...')

  const { status: compilationStatusScripts } = spawnSync(`forge`, ['build'], {
    stdio: 'inherit',
  })
  // Exit the process if compilation fails.
  if (compilationStatusScripts !== 0) {
    process.exit(1)
  }

  spinner.succeed('Finished compiling clients and scripts')
}
