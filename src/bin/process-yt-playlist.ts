import * as fs from 'node:fs/promises'

import { Pinecone } from '@pinecone-database/pinecone'
import { Configuration, OpenAIApi } from 'openai'

import * as types from '@/server/types'
import '@/server/config'
import { upsertVideoTranscriptsForPlaylist } from '@/server/pinecone'

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

  await upsertVideoTranscriptsForPlaylist(playlistDetailsWithTranscripts, {
    openai,
    pinecone
  })
}

main().catch((err) => {
  console.error('error', err)
  process.exit(1)
})
