import * as fs from 'node:fs/promises'
import * as path from 'node:path'

import { Pinecone } from '@pinecone-database/pinecone'
import pMap from 'p-map'

import * as types from './types'
import { getEmbeddingsForVideoTranscript } from './openai'

const CHECKPOINT_DIR = 'checkpoints'
const getCheckpointPath = (playlistId: string) =>
  path.join(CHECKPOINT_DIR, `${playlistId}.json`)

async function saveCheckpoint(playlistId: string, processedIds: string[]) {
  await fs.writeFile(
    getCheckpointPath(playlistId),
    JSON.stringify(processedIds, null, 2),
    'utf-8'
  )
}

// Helper function to chunk array into smaller pieces
function chunkArray<T>(array: T[], chunkSize: number): T[][] {
  const chunks = []
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize))
  }
  return chunks
}

export async function upsertVideoTranscriptsForPlaylist(
  playlist: types.PlaylistDetailsWithTranscripts,
  {
    openai,
    pinecone,
    processedVideoIds = [],
    concurrency = 1
  }: {
    openai: types.OpenAIApi
    pinecone: Pinecone
    processedVideoIds?: string[]
    concurrency?: number
  }
) {
  const index = pinecone.index(process.env.PINECONE_INDEX_NAME)

  const videos = playlist.playlistItems
    .map((playlistItem) => {
      const id = playlistItem.contentDetails.videoId
      if (!id) return

      // Skip already processed videos
      if (processedVideoIds.includes(id)) {
        console.log(`Skipping already processed video: ${id}`)
        return
      }

      const title = playlistItem.snippet.title
      if (!title) return

      const transcript = playlist.transcripts[id]
      if (!transcript) return

      return {
        id,
        transcript,
        title
      }
    })
    .filter(Boolean)

  return (
    await pMap(
      videos,
      async (video) => {
        try {
          console.log('Processing video:', video.id, video.title)
          const videoEmbeddings = await getEmbeddingsForVideoTranscript({
            transcript: video.transcript,
            title: video.title,
            openai
          })

          const vectors = videoEmbeddings.map((embeddingObj, i) => {
            if (
              !Array.isArray(embeddingObj.values) ||
              !embeddingObj.values.every(Number.isFinite)
            ) {
              console.error(
                `Invalid embedding format for video ${video.id} at index ${i}:`,
                embeddingObj
              )
              throw new Error(`Invalid embedding format for video ${video.id}`)
            }

            return {
              id: `${video.id}-${i}`,
              values: embeddingObj.values
            }
          })

          const CHUNK_SIZE = 100 // Adjust based on your average vector size

          console.log(
            `Upserting ${vectors.length} vectors for video ${video.id} in chunks`
          )

          const vectorChunks = chunkArray(vectors, CHUNK_SIZE)

          for (const chunk of vectorChunks) {
            try {
              await index.upsert(chunk)
              console.log(
                `Successfully upserted chunk of ${chunk.length} vectors`
              )
            } catch (err) {
              console.error(`Error upserting chunk:`, err)
              // Consider if you want to throw here or continue with next chunk
            }
          }

          // Save progress after each successful video processing
          processedVideoIds.push(video.id)
          await saveCheckpoint(playlist.playlistId, processedVideoIds)

          return video
        } catch (err) {
          console.warn(
            'Error upserting transcripts for video',
            video.id,
            video.title,
            err
          )
        }
      },
      {
        concurrency
      }
    )
  ).filter(Boolean)
}
