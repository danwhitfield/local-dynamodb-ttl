import {
  AttributeValue,
  DeleteItemCommand,
  DescribeTableCommand,
  DynamoDBClient,
  KeyType,
  ResourceNotFoundException,
  ScanCommand
} from '@aws-sdk/client-dynamodb'

interface KeySchema {
  hash: string
  range: string | undefined
}

const client = new DynamoDBClient({
  endpoint: getRequiredEnvironmentVariable('AWS_ENDPOINT'),
  region: process.env.AWS_REGION
})

function getRequiredEnvironmentVariable(name: string): string {
  const value = process.env[name]

  if (!value) {
    throw new Error(`You must configure the '${name}' environment variable!`)
  }

  return value
}

async function sleep(seconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, seconds * 1000)
  })
}

async function getExpiredItemKeys(keySchema: KeySchema): Promise<Record<string, AttributeValue>[] | undefined> {
  const response = await client.send(
    new ScanCommand({
      TableName: getRequiredEnvironmentVariable('TABLE_NAME'),
      ScanFilter: {
        [getRequiredEnvironmentVariable('TTL_ATTRIBUTE')]: {
          ComparisonOperator: 'LT',
          AttributeValueList: [{ N: Math.floor(Date.now() / 1000).toString() }]
        }
      }
    })
  )

  if (response.$metadata.httpStatusCode !== 200) {
    throw new Error(`Failed to scan DynamoDB items '${JSON.stringify(response)}'`)
  }

  if (!response.Items) {
    return
  }

  return response.Items.map((i) => {
    const record: Record<string, AttributeValue> = {
      [keySchema.hash]: i[keySchema.hash]
    }

    if (keySchema.range) {
      record[keySchema.range] = i[keySchema.range]
    }

    return record as Record<string, AttributeValue>
  })
}

async function getTableKeySchema(): Promise<KeySchema> {
  const response = await client.send(
    new DescribeTableCommand({
      TableName: getRequiredEnvironmentVariable('TABLE_NAME')
    })
  )

  if (response.$metadata.httpStatusCode !== 200 || !response.Table) {
    throw new Error(`Failed to describe DynamoDB table '${JSON.stringify(response)}'`)
  }

  if (!response.Table.KeySchema) {
    throw new Error(`No key schema defined on DynamoDB table '${JSON.stringify(response)}'`)
  }

  const keySchema: Partial<KeySchema> = {}

  for (const keySchemaItem of response.Table.KeySchema) {
    const attributeDefinition = response.Table.AttributeDefinitions?.find((d) => d.AttributeName === keySchemaItem.AttributeName)

    if (!attributeDefinition || !attributeDefinition.AttributeType) {
      console.warn(`Could not find attribute definition or type for key '${keySchemaItem.AttributeName}'`)
      continue
    }

    if (keySchemaItem.KeyType === KeyType.HASH) {
      keySchema.hash = keySchemaItem.AttributeName as string
    } else if (keySchemaItem.KeyType === KeyType.RANGE) {
      keySchema.range = keySchemaItem.AttributeName as string
    } else {
      console.warn(`Key '${keySchemaItem}' was not key type HASH or RANGE`)
    }
  }

  return keySchema as KeySchema
}

async function deleteItems(keys: Record<string, AttributeValue>[]) {
  for (const key of keys) {
    console.log(`Deleting DynamoDB item with key: '${JSON.stringify(key)}'`)

    const response = await client.send(
      new DeleteItemCommand({
        TableName: getRequiredEnvironmentVariable('TABLE_NAME'),
        Key: key
      })
    )

    if (response.$metadata.httpStatusCode !== 200) {
      throw new Error(`Failed to delete DynamoDB item with key '${JSON.stringify(key)}'`)
    }
  }
}

async function main() {
  const sleepSeconds = 5
  let keySchema

  do {
    try {
      keySchema = await getTableKeySchema()
    } catch (err) {
      if (!(err instanceof ResourceNotFoundException)) {
        throw err
      }

      console.warn(`Table '${getRequiredEnvironmentVariable('TABLE_NAME')}' not found, trying again in '${sleepSeconds}' seconds`)
      await sleep(sleepSeconds)
    }
  } while (!keySchema)

  while (true) {
    console.log('Scanning for new expired DynamoDB items...')

    const expiredItems = await getExpiredItemKeys(keySchema)

    if (expiredItems) {
      await deleteItems(expiredItems)
    } else {
      console.log('No expired items to delete')
    }

    console.log(`Sleeping for ${sleepSeconds} seconds...`)

    await sleep(sleepSeconds)
  }
}

main()
