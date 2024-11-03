import * as fs from 'node:fs/promises'

import { Storage } from '@google-cloud/storage'
import { Pinecone } from '@pinecone-database/pinecone'
import pMap from 'p-map'

import * as types from '@/server/types'
import '@/server/config'
import { getThumbnailsForVideo } from '@/server/thumbnails'

function chunkArray<T>(array: T[], chunkSize: number): T[][] {
  const chunks = []
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize))
  }
  return chunks
}

async function main() {
  const pinecone = new Pinecone({
    apiKey: process.env.PINECONE_API_KEY
  })

  const index = pinecone.index(process.env.PINECONE_INDEX_NAME)

  const playlistId = process.env.YOUTUBE_PLAYLIST_ID
  const playlistDetailsWithTranscripts: types.PlaylistDetailsWithTranscripts =
    JSON.parse(await fs.readFile(`out/${playlistId}.json`, 'utf-8'))

  const storage = new Storage()
  const bucket = process.env.GOOGLE_STORAGE_BUCKET

  await pMap(
    playlistDetailsWithTranscripts.playlistItems,
    async (playlistItem) => {
      const videoId = playlistItem.contentDetails.videoId
      if (!videoId) return

      try {
        const ids = Array.from(Array(1000).keys()).map(
          (_, i) => `${videoId}-${i}`
        )

        const idChunks = chunkArray(ids, 50) // Try smaller chunk size if needed

        const allVectors: types.PineconeVector[] = []

        for (const idChunk of idChunks) {
          const fetchResponse = await index.fetch(idChunk)

          // Debug log to see the fetch response
          console.log(
            `Fetch response for chunk of video ${videoId}:`,
            fetchResponse
          )

          const vectors = Object.values(fetchResponse) as types.PineconeVector[]
          allVectors.push(...vectors)
        }

        const docs = allVectors.filter(
          (doc) => doc.id && !doc.metadata?.thumbnail
        )

        docs.sort((a, b) => a.id!.localeCompare(b.id!))
        const timestamps = docs.map((doc) => doc.metadata?.start)

        if (!timestamps.length) {
          console.warn('video', videoId, 'no embeddings found')
          return
        }

        console.log(
          '\nProcessing video',
          videoId,
          'with',
          timestamps.length,
          'timestamps\n'
        )

        const thumbnailMap = await getThumbnailsForVideo({
          videoId,
          timestamps,
          storage,
          bucket
        })

        for (const doc of docs) {
          const thumbnailData = thumbnailMap[doc.metadata?.start]
          if (thumbnailData) {
            doc.metadata = {
              ...doc.metadata,
              thumbnail: thumbnailData.thumbnail,
              preview: thumbnailData.preview
            }
          }
        }

        await index.upsert(
          docs.map((doc) => ({
            id: doc.id!,
            values: doc.values,
            metadata: doc.metadata
          })) as {
            id: string
            values: number[]
            metadata: types.PineconeCaptionMetadata
          }[]
        )
      } catch (err) {
        console.error('Error processing video', videoId, err)
      }
    },
    {
      concurrency: 4
    }
  )
}

main().catch((err) => {
  console.error('Error', err)
  process.exit(1)
})
