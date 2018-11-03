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
  notEqual: { source: Item, dest: Item }[]
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
const createVersionedBucketIfNotExists = async (Bucket: string) => {
  try {
    await s3.headBucket({ Bucket }).promise()
  } catch (err) {
    if (err.code === 'NoSuchBucket') {
      await s3.createBucket({ Bucket }).promise()
    }
  }

  await s3.putBucketVersioning({
    Bucket,
    VersioningConfiguration: {
      Status: 'Enabled'
    }
  }).promise()

  return Bucket
}

// const deleteBucket = async (bucket: string) => _deleteBucket({ s3, bucket })
const emptyBucket = (bucket: string) => _emptyBucket({ s3, bucket })

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
  const args = ['-b', source, '-d', `s3://${dest}`, '-f', start, '-t', end, '--restore-deleted']
  if (endpoint) {
    args.push('--endpoint', ENDPOINT)
  }

  console.log('executing:', `./s3-pit-restore ${args.join(' ')}`)
  await execa('./s3-pit-restore', args, { env })
}

const isoDateNow = () => new Date().toISOString()
const dumpBucket = async (Bucket: string) => {
  const params:AWS.S3.ListObjectsV2Request = {
    Bucket
  }

  const gets:Promise<AWS.S3.GetObjectOutput[]> = []
  const get = async (Key: string):Promise<Item> => {
    const result = await s3.getObject({ Bucket, Key }).promise()
    return { ...result, Key } as Item
  }

  let result:AWS.S3.ListObjectsV2Output
  do {
    result = await s3.listObjectsV2({ Bucket }).promise()
    let { Contents=[] } = result
    let keys = Contents.filter(i => i.Key && i.Key.length).map(i => i.Key) as string[]
    gets.push(Promise.all(keys.map(get)))
    params.ContinuationToken = result.ContinuationToken
  } while (result.ContinuationToken)

  const bodies = await Promise.all(gets)
  // flatten
  return bodies.reduce((all:Item[], some: Item[]) => all.concat(some), []) as Item[]
}

const printDiff = ({ missing, extra, notEqual }: Diff) => {
  missing.forEach(item => console.warn(`missing: ${item.Key}`))
  extra.forEach(item => console.warn(`extra: ${item.Key}`))
  notEqual.forEach(({ source, dest }) => {
      console.warn(`item mismatched:
Source: ${prettify(source)}
Dest: ${prettify(dest)}`)

  })
}

const compare = (source: Item[], dest: Item[]):Diff => {
  const sourceMap = new Map(source.map(item => [item.Key, item]) as Pairs)
  const destMap = new Map(dest.map(item => [item.Key, item]) as Pairs)
  const missing = source.filter(({ Key }) => !destMap.has(Key))
  const extra = dest.filter(({ Key }) => !sourceMap.has(Key))
  const notEqual = dest.filter(item => {
    if (extra.includes(item)) return

    const sourceItem = sourceMap.get(item.Key)
    if (_.isMatch(item, sourceItem)) {
      return true
    }
  })
  .map(item => ({
    source: sourceMap.get(item.Key),
    dest: item,
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

const verifyDiff = (t, result: Diff) => {
  t.equal(result.missing.length, 0, 'none missing')
  t.equal(result.extra.length, 0, 'no extras')
  t.equal(result.notEqual.length, 0, 'values match')
  if (result.missing.length || result.extra.length || result.notEqual.length) {
    throw new Error('failed')
  }
}

test('pit restore', async t => {
  const verify = async (expected) => {
    const actual = await dumpBucket(dest)
    debugger
    const diff = compare(expected, actual)
    verifyDiff(t, diff)
    printDiff(diff)
  }

  const source = genBucketName('source')
  const dest = genBucketName('dest')
  log('emptying source and dest buckets')
  await Promise.all([ensureEmptyBucket(source), ensureEmptyBucket(dest)])

  await waitGracePeriod()
  const timeStart = isoDateNow()
  await waitGracePeriod()

  // put1: first batch
  log(`putting 1st batch: > ${timeStart}`)
  const put1 = genObjects({ count: 100, offset: 0 })
  await putItems(source, put1)
  await waitGracePeriod()
  const timeAfterPut1 = isoDateNow()
  const timeBeforePut2 = timeAfterPut1

  await waitGracePeriod()

  // put2: half new, half overwrites
  log(`putting 2nd batch: > ${timeAfterPut1}`)
  const put2 = genObjects({ count: 100, offset: 50 })
  await putItems(source, put2)
  await waitGracePeriod()
  const timeAfterPut2 = isoDateNow()
  const timeBeforeEmptying = timeAfterPut2

  await waitGracePeriod()

  // empty: erase all items
  log('emptying source bucket')
  await emptyBucket(source)
  await waitGracePeriod()
  const timeAfterEmptying = isoDateNow()
  const timeBeforePut3 = timeAfterEmptying
  await waitGracePeriod()

  // put3: new items, overlapping range with put1/put2
  log('putting 3rd batch')
  const put3 = genObjects({ count: 100, offset: 75 })
  await putItems(source, put3)
  await waitGracePeriod()
  const timeAfterPut3 = isoDateNow()
  await waitGracePeriod()

  log('beginning verifications')

  // verify 1
  await emptyBucket(dest)
  await restore({ source, dest, start: timeStart, end: timeAfterPut1 })
  await verify(put1)

  // verify 2
  await emptyBucket(dest)
  await restore({ source, dest, start: timeStart, end: timeAfterPut2 })
  await verify(_.uniqBy(put2.concat(put1), 'Key'))

  // verify 3
  await emptyBucket(dest)
  await restore({ source, dest, start: timeStart, end: timeAfterEmptying })
  await verify([])

  // verify 4
  await emptyBucket(dest)
  await restore({ source, dest, start: timeStart, end: timeAfterPut3 })
  await verify(put3)

  await Promise.all([emptyBucket(source), emptyBucket(dest)])
  t.end()
})
