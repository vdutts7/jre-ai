import * as fs from 'node:fs/promises'
import * as path from 'node:path'

import { Pinecone } from '@pinecone-database/pinecone'
import { OpenAIApi } from 'openai'
import pMap from 'p-map'

import * as types from './types'

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

function estimateVectorSize(vector: any): number {
  const valuesSize = vector.values.length * 8
  const metadataSize = JSON.stringify(vector.metadata).length
  return valuesSize + metadataSize + vector.id.length
}

function splitTranscript(
  transcript: types.Transcript,
  chunkSize = 1000
): types.Transcript[] {
  if (!transcript) {
    console.warn('Transcript is undefined')
    return []
  }

  if (!transcript.parts) {
    console.warn(
      `Transcript parts is undefined for video ${transcript?.videoId}`
    )
    return []
  }

  if (!Array.isArray(transcript.parts)) {
    console.warn(
      `Transcript parts is not an array for video ${transcript?.videoId}`
    )
    return []
  }

  if (transcript.parts.length === 0) {
    console.warn(
      `Transcript parts array is empty for video ${transcript?.videoId}`
    )
    return []
  }

  console.log('Processing transcript:', {
    videoId: transcript.videoId,
    partsLength: transcript.parts.length,
    firstPart: transcript.parts[0]
  })

  const transcriptChunks: types.Transcript[] = []
  let currentChunk: types.TranscriptPart[] = []
  let currentTextLength = 0

  transcript.parts.forEach((part) => {
    if (!part?.text) {
      console.warn(
        `Invalid transcript part found in video ${transcript.videoId}:`,
        part
      )
      return
    }

    if (
      currentTextLength + part.text.length > chunkSize &&
      currentChunk.length > 0
    ) {
      transcriptChunks.push({
        videoId: transcript.videoId,
        parts: currentChunk
      })
      currentChunk = []
      currentTextLength = 0
    }

    currentChunk.push(part)
    currentTextLength += part.text.length
  })

  if (currentChunk.length > 0) {
    transcriptChunks.push({ videoId: transcript.videoId, parts: currentChunk })
  }

  return transcriptChunks
}

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

async function getEmbeddingsWithRetry({
  transcript,
  title,
  openai,
  maxRetries = 5,
  retryDelay = 1000
}: {
  transcript: types.Transcript
  title: string
  openai: OpenAIApi
  maxRetries?: number
  retryDelay?: number
}): Promise<any> {
  let attempts = 0
  let delay = retryDelay

  while (attempts < maxRetries) {
    try {
      const response = await openai.createEmbedding({
        model: 'text-embedding-ada-002',
        input: transcript.parts.map((part) => part.text).join(' ')
      })
      return response.data.data
    } catch (error: any) {
      if (error.response && error.response.status === 429) {
        console.warn(`Rate limit hit, retrying in ${delay} ms...`)
        await new Promise((resolve) => setTimeout(resolve, delay))
        attempts += 1
        delay *= 2
      } else {
        console.error(`Error while fetching embeddings:`, error)
        throw error
      }
    }
  }
  throw new Error(`Failed to retrieve embeddings after ${maxRetries} attempts`)
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

  // Log playlist structure for debugging
  console.log('Playlist structure:', {
    itemCount: playlist.playlistItems.length,
    firstItem: playlist.playlistItems[0],
    transcripts: Object.keys(playlist.transcripts).length
  })

  const videos = playlist.playlistItems
    .map((playlistItem) => {
      const id = playlistItem.contentDetails.videoId
      const title = playlistItem.snippet.title
      const transcript = playlist.transcripts[id]

      // Log the current item details for deeper debugging
      console.log(
        `Processing video - ID: ${id}, Title: ${title}, Transcript Exists: ${!!transcript}`
      )

      if (!id) return null
      if (processedVideoIds.includes(id)) {
        console.log(`Skipping already processed video: ${id}`)
        return null
      }
      if (!title || !transcript) {
        console.log(`Skipping video without title or transcript: ${id}`)
        return null
      }

      return { id, transcript, title }
    })
    .filter(Boolean)

  console.log(`Total videos to process: ${videos.length}`)

  const processedVideos = await pMap(
    videos,
    async (video) => {
      try {
        console.log(`Processing video: ${video.id} - ${video.title}`)

        // ... (rest of the processing code remains unchanged)
      } catch (err) {
        console.warn(`Error processing video ${video.id}:`, err)
      }
    },
    { concurrency }
  )

  console.log(
    `Processing completed. Total processed videos: ${
      processedVideos.filter(Boolean).length
    }`
  )
}
