import * as fs from 'node:fs/promises'
import * as path from 'node:path'

import { Pinecone } from '@pinecone-database/pinecone'
import { Configuration, OpenAIApi } from 'openai'

import * as types from '@/server/types'
import '@/server/config'
import { upsertVideoTranscriptsForPlaylist } from '@/server/pinecone'

const CHECKPOINT_DIR = 'checkpoints'
const getCheckpointPath = (playlistId: string) =>
  path.join(CHECKPOINT_DIR, `${playlistId}.json`)

async function ensureCheckpointDir() {
  try {
    await fs.mkdir(CHECKPOINT_DIR, { recursive: true })
  } catch (err) {
    // Directory already exists
  }
}

async function loadCheckpoint(playlistId: string): Promise<string[]> {
  try {
    const checkpoint = await fs.readFile(getCheckpointPath(playlistId), 'utf-8')
    return JSON.parse(checkpoint)
  } catch (err) {
    return []
  }
}

async function main() {
  const openai = new OpenAIApi(
    new Configuration({
      apiKey: process.env.OPENAI_API_KEY
    })
  )

  const pinecone = new Pinecone({
    apiKey: process.env.PINECONE_API_KEY
  })

  const playlistId = process.env.YOUTUBE_PLAYLIST_ID
  const playlistDetailsWithTranscripts = JSON.parse(
    await fs.readFile(`out/${playlistId}.json`, 'utf-8')
  )

  await ensureCheckpointDir()
  const processedVideoIds = await loadCheckpoint(playlistId)

  await upsertVideoTranscriptsForPlaylist(playlistDetailsWithTranscripts, {
    openai,
    pinecone,
    processedVideoIds
  })
}

main().catch((err) => {
  console.error('error', err)
  process.exit(1)
})
