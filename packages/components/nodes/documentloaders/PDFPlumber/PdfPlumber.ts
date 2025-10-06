import { omit } from 'lodash'
import { IDocument, ICommonObject, INode, INodeData, INodeParams } from '../../../src/Interface'
import { TextSplitter } from 'langchain/text_splitter'
import { PDFPlumberLoader } from '@langchain/community/document_loaders/fs/pdf'
import { getFileFromStorage, handleEscapeCharacters, INodeOutputsValue } from '../../../src'

class PdfPlumber_DocumentLoaders implements INode {
    label: string
    name: string
    version: number
    description: string
    type: string
    icon: string
    category: string
    baseClasses: string[]
    inputs: INodeParams[]
    outputs: INodeOutputsValue[]

    constructor() {
        this.label = 'Pdf File (PDFPlumber)'
        this.name = 'pdfPlumberFile'
        this.version = 1.0
        this.type = 'Document'
        this.icon = 'pdf.svg'
        this.category = 'Document Loaders'
        this.description = `Load data from PDF files using PDFPlumber for better text extraction`
        this.baseClasses = [this.type]
        this.inputs = [
            {
                label: 'Pdf File',
                name: 'pdfFile',
                type: 'file',
                fileType: '.pdf'
            },
            {
                label: 'Text Splitter',
                name: 'textSplitter',
                type: 'TextSplitter',
                optional: true
            },
            {
                label: 'Usage',
                name: 'usage',
                type: 'options',
                options: [
                    {
                        label: 'One document per page',
                        name: 'perPage'
                    },
                    {
                        label: 'One document per file',
                        name: 'perFile'
                    }
                ],
                default: 'perPage'
            },
            {
                label: 'Extract Images',
                name: 'extractImages',
                type: 'boolean',
                optional: true,
                default: false,
                additionalParams: true,
                description: 'Whether to extract images from the PDF'
            },
            {
                label: 'Language',
                name: 'language',
                type: 'string',
                optional: true,
                placeholder: 'eng, fra, etc.',
                additionalParams: true,
                description: 'Language for OCR (optional)'
            },
            {
                label: 'Additional Metadata',
                name: 'metadata',
                type: 'json',
                description: 'Additional metadata to be added to the extracted documents',
                optional: true,
                additionalParams: true
            },
            {
                label: 'Omit Metadata Keys',
                name: 'omitMetadataKeys',
                type: 'string',
                rows: 4,
                description:
                    'Each document loader comes with a default set of metadata keys that are extracted from the document. You can use this field to omit some of the default metadata keys. The value should be a list of keys, seperated by comma. Use * to omit all metadata keys execept the ones you specify in the Additional Metadata field',
                placeholder: 'key1, key2, key3.nestedKey1',
                optional: true,
                additionalParams: true
            }
        ]
        this.outputs = [
            {
                label: 'Document',
                name: 'document',
                description: 'Array of document objects containing metadata and pageContent',
                baseClasses: [...this.baseClasses, 'json']
            },
            {
                label: 'Text',
                name: 'text',
                description: 'Concatenated string from pageContent of documents',
                baseClasses: ['string', 'json']
            }
        ]
    }

    async init(nodeData: INodeData, _: string, options: ICommonObject): Promise<any> {
        const textSplitter = nodeData.inputs?.textSplitter as TextSplitter
        const pdfFileBase64 = nodeData.inputs?.pdfFile as string
        const usage = nodeData.inputs?.usage as string
        const metadata = nodeData.inputs?.metadata
        const extractImages = nodeData.inputs?.extractImages as boolean
        const language = nodeData.inputs?.language as string
        const _omitMetadataKeys = nodeData.inputs?.omitMetadataKeys as string
        const output = nodeData.outputs?.output as string

        let omitMetadataKeys: string[] = []
        if (_omitMetadataKeys) {
            omitMetadataKeys = _omitMetadataKeys.split(',').map((key) => key.trim())
        }

        let docs: IDocument[] = []
        let files: string[] = []

        //FILE-STORAGE::["CONTRIBUTING.md","LICENSE.md","README.md"]
        if (pdfFileBase64.startsWith('FILE-STORAGE::')) {
            const fileName = pdfFileBase64.replace('FILE-STORAGE::', '')
            if (fileName.startsWith('[') && fileName.endsWith(']')) {
                files = JSON.parse(fileName)
            } else {
                files = [fileName]
            }
            const orgId = options.orgId
            const chatflowid = options.chatflowid

            for (const file of files) {
                if (!file) continue
                const fileData = await getFileFromStorage(file, orgId, chatflowid)
                const bf = Buffer.from(fileData)
                await this.extractDocs(usage, bf, extractImages, language, textSplitter, docs)
            }
        } else {
            if (pdfFileBase64.startsWith('[') && pdfFileBase64.endsWith(']')) {
                files = JSON.parse(pdfFileBase64)
            } else {
                files = [pdfFileBase64]
            }

            for (const file of files) {
                if (!file) continue
                const splitDataURI = file.split(',')
                splitDataURI.pop()
                const bf = Buffer.from(splitDataURI.pop() || '', 'base64')
                await this.extractDocs(usage, bf, extractImages, language, textSplitter, docs)
            }
        }

        if (metadata) {
            const parsedMetadata = typeof metadata === 'object' ? metadata : JSON.parse(metadata)
            docs = docs.map((doc) => ({
                ...doc,
                metadata:
                    _omitMetadataKeys === '*'
                        ? {
                              ...parsedMetadata
                          }
                        : omit(
                              {
                                  ...doc.metadata,
                                  ...parsedMetadata
                              },
                              omitMetadataKeys
                          )
            }))
        } else {
            docs = docs.map((doc) => ({
                ...doc,
                metadata:
                    _omitMetadataKeys === '*'
                        ? {}
                        : omit(
                              {
                                  ...doc.metadata
                              },
                              omitMetadataKeys
                          )
            }))
        }

        if (output === 'document') {
            return docs
        } else {
            let finaltext = ''
            for (const doc of docs) {
                finaltext += `${doc.pageContent}\n`
            }
            return handleEscapeCharacters(finaltext, false)
        }
    }

    private async extractDocs(
        usage: string, 
        bf: Buffer, 
        extractImages: boolean, 
        language: string, 
        textSplitter: TextSplitter, 
        docs: IDocument[]
    ) {
        // Create temporary file for PDFPlumber
        const fs = require('fs')
        const path = require('path')
        const tempDir = '/tmp/flowise-pdfplumber'
        
        if (!fs.existsSync(tempDir)) {
            fs.mkdirSync(tempDir, { recursive: true })
        }
        
        const tempFilePath = path.join(tempDir, `temp-${Date.now()}.pdf`)
        
        try {
            fs.writeFileSync(tempFilePath, bf)

            const loaderOptions: any = {
                splitPages: usage === 'perPage'
            }

            // Add optional parameters
            if (extractImages !== undefined) {
                loaderOptions.extractImages = extractImages
            }
            
            if (language) {
                loaderOptions.lang = language
            }

            const loader = new PDFPlumberLoader(tempFilePath, loaderOptions)

            if (textSplitter) {
                let splittedDocs = await loader.load()
                splittedDocs = await textSplitter.splitDocuments(splittedDocs)
                docs.push(...splittedDocs)
            } else {
                docs.push(...(await loader.load()))
            }
        } finally {
            // Clean up temporary file
            try {
                if (fs.existsSync(tempFilePath)) {
                    fs.unlinkSync(tempFilePath)
                }
            } catch (error) {
                console.error('Error cleaning up temporary file:', error)
            }
        }
    }
}

module.exports = { nodeClass: PdfPlumber_DocumentLoaders }
