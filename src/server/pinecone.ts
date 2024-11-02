import { Pinecone } from '@pinecone-database/pinecone'
import pMap from 'p-map'

import * as types from './types'
import { getEmbeddingsForVideoTranscript } from './openai'

export async function upsertVideoTranscriptsForPlaylist(
  playlist: types.PlaylistDetailsWithTranscripts,
  {
    openai,
    pinecone,
    concurrency = 1
  }: {
    openai: types.OpenAIApi
    pinecone: Pinecone
    concurrency?: number
  }
) {
  // Initialize or connect to the Pinecone index
  const index = pinecone.index(process.env.PINECONE_INDEX_NAME)

  const videos = playlist.playlistItems
    .map((playlistItem) => {
      const id = playlistItem.contentDetails.videoId
      if (!id) return

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

          // Debugging the structure of videoEmbeddings
          console.log(
            `Embedding structure for video ${video.id}:`,
            videoEmbeddings
          )

          // Extract only `values` arrays from each embedding object
          const vectors = videoEmbeddings.map((embeddingObj, i) => {
            // Ensure the object has `values` array
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
              id: `${video.id}-${i}`, // Unique ID for each vector
              values: embeddingObj.values // Only the values array
            }
          })

          console.log(
            `Upserting ${vectors.length} vectors for video ${video.id}`
          )

          // Upsert vectors into the index
          await index.upsert(vectors)

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
