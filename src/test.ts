// @ts-ignore
require('source-map-support').install()

import crypto from 'crypto'
import execa from 'execa'
import _ from 'lodash'
import Promise from 'bluebird'
import test from 'tape'
// import uuid from 'uuid/v4'
import AWS from 'aws-sdk'
import _emptyBucket from 'empty-aws-bucket'
// import _deleteBucket from 'delete-aws-bucket'

type Item = {
  Key: string
  Body: any
  ContentType: string
}

type Items = Item[]
type Pair = [any, any]
type Pairs = Pair[]
type Diff = {
  missing: Item[]
  extra: Item[]
  notEqual: { expected: Item, actual: Item }[]
}

const LOCALSTACK_ENDPOINT = 'http://localhost:4572'
const ENDPOINT = null
const GRACE_PERIOD = 1000
const log = (...args) => console.log(...args)
const waitGracePeriod = async () => {
  log(`waiting ${GRACE_PERIOD}ms`)
  await Promise.delay(GRACE_PERIOD)
}

const s3 = new AWS.S3({
  endpoint: ENDPOINT,
  s3ForcePathStyle: false,
})

const prettify = obj => JSON.stringify(obj, null, 2)
const genBucketName = (suffix: string) => `mvayngrib-s3-pit-test-bucket-12345-${suffix}`
const SOURCE = genBucketName('source')
const DEST = genBucketName('dest')

const createBucketIfNotExists = async (Bucket: string) => {
  try {
    await s3.headBucket({ Bucket }).promise()
  } catch (err) {
    if (err.code === 'NoSuchBucket') {
      await s3.createBucket({ Bucket }).promise()
    }
  }
}

const enableVersioning = async (Bucket: string) => {
  await s3.putBucketVersioning({
    Bucket,
    VersioningConfiguration: {
      Status: 'Enabled'
    }
  }).promise()
}

const createVersionedBucketIfNotExists = async (Bucket: string) => {
  await createBucketIfNotExists(Bucket)
  await enableVersioning(Bucket)
}

// const deleteBucket = async (bucket: string) => _deleteBucket({ s3, bucket })
const emptyBucket = (bucket: string) => _emptyBucket({ s3, bucket })
const deleteAllCurrentVersions = async (bucket: string) => {
  // const params: AWS.S3.ListObjectsV2Request
  await mapBucket({
    bucket,
    map: async ({ Key }) => {
      if (typeof Key === 'string') {
        return s3.deleteObject({ Bucket: bucket, Key }).promise()
      }
    }
  })
}

const ensureEmptyBucket = async (bucket: string) => {
  await createVersionedBucketIfNotExists(bucket)
  await emptyBucket(bucket)
}

const genObjects = (() => {
  let invocationCount = -1
  return ({ count, offset=0, itemsPerFolder = 10 }: {
    count: number
    offset?: number
    itemsPerFolder?: number
  }): Items => {
    invocationCount++
    let folder = 0
    return new Array(count).fill(null).map((ignore, i) => {
      i += offset
      if (i && i % itemsPerFolder === 0) {
        folder++
      }

      const filename = i % itemsPerFolder
      return {
        Key: `${folder}/${filename}.json`,
        Body: { i, iteration: invocationCount },
        ContentType: 'application/json'
      }
    })
  }
})()

const restore = async ({ source, dest, start, end, endpoint }: {
  source: string
  dest: string
  start: string
  end: string
  endpoint?: string
}) => {
  const env = {}
  const args = ['-b', source, '-d', `s3://${dest}`, '-f', start, '-t', end]//, '--restore-deleted']
  if (endpoint) {
    args.push('--endpoint', ENDPOINT)
  }

  console.log('executing:', `./s3-pit-restore ${args.join(' ')}`)
  await execa('./s3-pit-restore', args, { env })
}

const verify = async ({ bucket, expectedItems }: {
  bucket: string
  expectedItems: Item[]
}) => {
  const actual = await dumpBucket(DEST)
  const diff = compare(expectedItems, actual)
  verifyDiff(diff)
  printDiff(diff)
}

const isoDateNow = () => new Date().toISOString()
const mapBucket = async <T>({ bucket, map }: {
  bucket: string
  map: (obj: AWS.S3.Object) => Promise<T>
}):Promise<T[]> => {
  const Bucket = bucket
  const params:AWS.S3.ListObjectsV2Request = { Bucket }

  let batch:AWS.S3.ListObjectsV2Output
  const batchPromises:Promise<T[]>[] = []
  do {
    batch = await s3.listObjectsV2({ Bucket }).promise()
    let { Contents=[] } = batch
    batchPromises.push(Promise.all(Contents.map(map)))
    params.ContinuationToken = batch.ContinuationToken
  } while (batch.ContinuationToken)

  const results = await Promise.all(batchPromises)
  return results.reduce((all, some) => all.concat(some), [])
}

const dumpBucket = async (bucket: string) => {
  const items = await mapBucket({
    bucket,
    map: async ({ Key }) => {
      if (typeof Key === 'string') {
        const result = await s3.getObject({ Bucket: bucket, Key }).promise()
        return { ...result, Key }
      }
    }
  })

  return items.filter(_.identity)
}

const printDiff = ({ missing, extra, notEqual }: Diff) => {
  missing.forEach(item => console.warn(`missing: ${item.Key}`))
  extra.forEach(item => console.warn(`extra: ${item.Key}`))
  notEqual.forEach(({ expected, actual }) => {
      console.warn(`item mismatched:
expected: ${prettify(expected)}
actual: ${prettify(actual)}`)

  })
}

const compare = (expected: Item[], actual: Item[]):Diff => {
  const expectedMap = new Map(expected.map(item => [item.Key, item]) as Pairs)
  const actualMap = new Map(actual.map(item => [item.Key, item]) as Pairs)
  const missing = expected.filter(({ Key }) => !actualMap.has(Key))
  const extra = actual.filter(({ Key }) => !expectedMap.has(Key))
  const notEqual = actual.filter(item => {
    if (extra.includes(item)) return

    const expectedItem = expectedMap.get(item.Key)
    if (_.isMatch(item, expectedItem)) {
      return true
    }
  })
  .map(item => ({
    expected: expectedMap.get(item.Key),
    actual: item,
  }))

  return {
    missing,
    extra,
    notEqual,
  }
}

const putItems = (Bucket: string, tree:Items) => Promise.map(
  tree,
  async item => {
    const params: AWS.S3.PutObjectRequest = {
      ...item,
      Bucket,
      Body: JSON.stringify(item.Body)
    }

    return s3.putObject(params).promise()
  },
  { concurrency: 100 }
)

const verifyDiff = (result: Diff) => {
  // t.equal(result.missing.length, 0, 'none missing')
  // t.equal(result.extra.length, 0, 'no extras')
  // t.equal(result.notEqual.length, 0, 'values match')
  if (result.missing.length || result.extra.length || result.notEqual.length) {
    throw new Error(`failed: ${prettify(result)}`)
  }
}

test('pit restore', async t => {
  const batchSize = 10
  const source = SOURCE
  const dest = DEST
  log('emptying source and dest buckets')
  await Promise.all([ensureEmptyBucket(source), ensureEmptyBucket(dest)])

  await waitGracePeriod()
  const timeStart = isoDateNow()
  await waitGracePeriod()

  // put1: first batch
  log(`putting 1st batch: > ${timeStart}`)
  const put1 = genObjects({ count: batchSize, offset: 0 })
  await putItems(source, put1)
  await waitGracePeriod()
  const timeAfterPut1 = isoDateNow()
  const timeBeforePut2 = timeAfterPut1

  await waitGracePeriod()

  // put2: half new, half overwrites
  const put2 = genObjects({ count: batchSize, offset: Math.floor(batchSize / 2) })
  log(`putting 2nd batch: > ${timeAfterPut1}`)
  await putItems(source, put2)
  await waitGracePeriod()
  const timeAfterPut2 = isoDateNow()
  const timeBeforeDelete = timeAfterPut2

  await waitGracePeriod()

  // empty: erase all items
  log(`deleting all current versions from source bucket: > ${timeBeforeDelete}`)
  await deleteAllCurrentVersions(source)
  await waitGracePeriod()
  const timeAfterEmptying = isoDateNow()
  const timeBeforePut3 = timeAfterEmptying
  await waitGracePeriod()

  // put3: new items, overlapping range with put1/put2
  log(`putting 3rd batch: > ${timeBeforePut3}`)
  const put3 = genObjects({ count: batchSize, offset: Math.floor(batchSize / 4) })
  await putItems(source, put3)
  await waitGracePeriod()
  const timeAfterPut3 = isoDateNow()
  await waitGracePeriod()

  log(`beginning verifications: ${timeAfterPut3}`)

  // verify 1
  await emptyBucket(dest)
  await waitGracePeriod()
  await restore({ source, dest, start: timeStart, end: timeAfterPut1 })
  await verify({
    bucket: dest,
    expectedItems: put1
  })

  // verify 2
  await emptyBucket(dest)
  await waitGracePeriod()
  await restore({ source, dest, start: timeStart, end: timeAfterPut2 })
  await verify({
    bucket: dest,
    expectedItems: _.uniqBy([...put2, ...put1], 'Key')
  })

  // verify 3
  await emptyBucket(dest)
  await waitGracePeriod()
  await restore({ source, dest, start: timeStart, end: timeAfterEmptying })
  await verify({
    bucket: dest,
    expectedItems: []
  })

  // verify 4
  await emptyBucket(dest)
  await waitGracePeriod()
  await restore({ source, dest, start: timeStart, end: timeAfterPut3 })
  await verify({
    bucket: dest,
    expectedItems: put3
  })

  await Promise.all([emptyBucket(source), emptyBucket(dest)])
  t.end()
})
