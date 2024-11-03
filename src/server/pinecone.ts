import * as fs from 'node:fs/promises'
import * as path from 'node:path'

import { Pinecone } from '@pinecone-database/pinecone'
import pMap from 'p-map'

import * as types from './types'
import { getEmbeddingsForVideoTranscript } from './openai'

const CHECKPOINT_DIR = 'checkpoints'
const MAX_PAYLOAD_SIZE = 2 * 1024 * 1024 // 2 MB limit
const getCheckpointPath = (playlistId: string) =>
  path.join(CHECKPOINT_DIR, `${playlistId}.json`)

async function saveCheckpoint(playlistId: string, processedIds: string[]) {
  await fs.writeFile(
    getCheckpointPath(playlistId),
    JSON.stringify(processedIds, null, 2),
    'utf-8'
  )
}

// Function to estimate the size of each vector object in bytes
function estimateVectorSize(vector: any): number {
  const valuesSize = vector.values.length * 8 // Assuming float64 values
  const metadataSize = JSON.stringify(vector.metadata).length
  return valuesSize + metadataSize + vector.id.length
}

// Function to split the transcript while keeping required fields (start, dur, text, and videoId)
function splitTranscript(
  transcript: types.Transcript,
  chunkSize = 1000
): types.Transcript[] {
  const transcriptChunks: types.Transcript[] = []
  let currentChunk: types.TranscriptPart[] = []
  let currentTextLength = 0

  transcript.parts.forEach((part) => {
    if (currentTextLength + part.text.length > chunkSize) {
      // Push the current chunk as a `Transcript` type with `videoId` and required fields
      transcriptChunks.push({
        videoId: transcript.videoId,
        parts: currentChunk
      })
      currentChunk = [] // Start a new chunk
      currentTextLength = 0
    }
    // Add the current part to the chunk
    currentChunk.push(part)
    currentTextLength += part.text.length
  })

  // Push the last chunk if there are any remaining parts
  if (currentChunk.length > 0) {
    transcriptChunks.push({ videoId: transcript.videoId, parts: currentChunk })
  }

  return transcriptChunks
}

// Helper function to create smaller chunks based on max payload size
function createPayloadChunks(vectors: any[], maxPayloadSize: number): any[][] {
  const chunks = []
  let currentChunk = []
  let currentChunkSize = 0

  for (const vector of vectors) {
    const vectorSize = estimateVectorSize(vector)

    if (currentChunkSize + vectorSize > maxPayloadSize) {
      chunks.push(currentChunk)
      currentChunk = []
      currentChunkSize = 0
    }

    currentChunk.push(vector)
    currentChunkSize += vectorSize
  }

  if (currentChunk.length > 0) {
    chunks.push(currentChunk)
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

          // Split the transcript into manageable parts, maintaining the expected `Transcript` type
          const transcriptParts = splitTranscript(video.transcript, 1000) // Adjust chunk size as needed

          // Generate embeddings for each part of the transcript
          const videoEmbeddings = await Promise.all(
            transcriptParts.map((part) =>
              getEmbeddingsForVideoTranscript({
                transcript: part,
                title: video.title,
                openai
              })
            )
          )

          // Flatten the embeddings array and add metadata for each chunk
          const vectors = videoEmbeddings.flatMap((embeddingArray, partIndex) =>
            embeddingArray.map((embeddingObj, i) => {
              if (
                !Array.isArray(embeddingObj.values) ||
                !embeddingObj.values.every(Number.isFinite)
              ) {
                console.error(
                  `Invalid embedding format for video ${video.id} at index ${i}:`,
                  embeddingObj
                )
                throw new Error(
                  `Invalid embedding format for video ${video.id}`
                )
              }

              return {
                id: `${video.id}-${partIndex}-${i}`, // Unique ID for each part
                values: embeddingObj.values,
                metadata: {
                  videoId: video.id,
                  title: video.title,
                  partIndex: partIndex,
                  transcriptPart: transcriptParts[partIndex].parts
                    .map((p) => p.text)
                    .join(' ') // Store part of transcript
                }
              }
            })
          )

          console.log(
            `Upserting ${vectors.length} vectors for video ${video.id} with payload limit`
          )

          // Create chunks based on max payload size
          const vectorChunks = createPayloadChunks(vectors, MAX_PAYLOAD_SIZE)

          for (const chunk of vectorChunks) {
            try {
              await index.upsert(chunk)
              console.log(
                `Successfully upserted chunk of ${chunk.length} vectors`
              )
            } catch (err) {
              console.error(`Error upserting chunk:`, err)
              // Decide if you want to throw here or continue with next chunk
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
