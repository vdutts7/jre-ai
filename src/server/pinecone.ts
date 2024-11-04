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
  maxChunkLength = 4000 // Conservative limit to stay under token limits
): types.Transcript[] {
  if (!transcript?.parts?.length) {
    console.warn('Invalid transcript:', transcript)
    return []
  }

  const chunks: types.Transcript[] = []
  let currentChunk: types.TranscriptPart[] = []
  let currentLength = 0

  for (const part of transcript.parts) {
    if (!part?.text) continue

    const partLength = part.text.length

    if (
      currentLength + partLength > maxChunkLength &&
      currentChunk.length > 0
    ) {
      // Save current chunk and start a new one
      chunks.push({
        videoId: transcript.videoId,
        parts: currentChunk
      })
      currentChunk = []
      currentLength = 0
    }

    currentChunk.push(part)
    currentLength += partLength
  }

  // Add the last chunk if it has any parts
  if (currentChunk.length > 0) {
    chunks.push({
      videoId: transcript.videoId,
      parts: currentChunk
    })
  }

  return chunks
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
  retryDelay = 1000,
  maxTokens = 8000 // OpenAI's token limit for embeddings
}: {
  transcript: types.Transcript
  title: string
  openai: OpenAIApi
  maxRetries?: number
  retryDelay?: number
  maxTokens?: number
}): Promise<any[]> {
  // First split the transcript into smaller chunks
  const chunks = splitTranscript(transcript)
  let allEmbeddings: any[] = []

  for (const chunk of chunks) {
    let attempts = 0
    let delay = retryDelay

    while (attempts < maxRetries) {
      try {
        const text = chunk.parts
          .map((part) => part.text)
          .join(' ')
          .trim()
        if (!text) continue

        const response = await openai.createEmbedding({
          model: 'text-embedding-ada-002',
          input: text
        })

        allEmbeddings.push(...response.data.data)
        break
      } catch (error: any) {
        if (error.response?.status === 429) {
          console.warn(`Rate limit hit, retrying in ${delay}ms...`)
          await new Promise((resolve) => setTimeout(resolve, delay))
          attempts++
          delay *= 2
        } else {
          console.error(
            `Error while fetching embeddings:`,
            error.response?.data || error
          )
          throw error
        }
      }
    }
  }

  return allEmbeddings
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

  console.log('Playlist structure:', {
    itemCount: playlist.playlistItems.length,
    transcripts: Object.keys(playlist.transcripts).length
  })

  const videos = playlist.playlistItems
    .map((playlistItem) => {
      const id = playlistItem.contentDetails.videoId
      const title = playlistItem.snippet.title
      const transcriptData = playlist.transcripts[id]

      console.log(
        `Processing video - ID: ${id}, Title: ${title}, Transcript Exists: ${!!transcriptData}`
      )

      if (!id) return null
      if (processedVideoIds.includes(id)) {
        console.log(`Skipping already processed video: ${id}`)
        return null
      }
      if (!title || !transcriptData || !transcriptData.transcript) {
        console.log(`Skipping video without title or transcript: ${id}`)
        return null
      }

      // Convert Python transcript format to our internal format
      const transcript: types.Transcript = {
        videoId: id,
        parts: transcriptData.transcript.map((entry) => ({
          text: entry.text,
          start: entry.start.toString(),
          dur: entry.duration.toString()
        }))
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

        // Get embeddings for the transcript
        const embeddings = await getEmbeddingsWithRetry({
          transcript: video.transcript,
          title: video.title,
          openai
        })

        // Create vectors from embeddings
        const vectors = embeddings.map((embedding, i) => ({
          id: `${video.id}:${i}`,
          values: embedding.embedding,
          metadata: {
            title: video.title,
            videoId: video.id,
            text: video.transcript.parts[i].text,
            start: video.transcript.parts[i].start
          }
        }))

        // Split vectors into chunks to avoid payload size limits
        const chunks = createPayloadChunks(vectors, MAX_PAYLOAD_SIZE)

        // Upsert chunks to Pinecone
        for (const chunk of chunks) {
          await index.upsert(chunk)
        }

        // Save checkpoint
        await saveCheckpoint(playlist.playlistId, [
          ...processedVideoIds,
          video.id
        ])
        processedVideoIds.push(video.id)

        return video.id
      } catch (err) {
        console.warn(`Error processing video ${video.id}:`, err)
        return null
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
