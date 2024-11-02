import * as React from 'react'
import { InferGetStaticPropsType } from 'next'

import * as config from '@/lib/config'
import { Layout } from '@/components/Layout/Layout'
import { Markdown } from '@/components/Markdown/Markdown'
import { markdownToHtml } from '@/server/markdown-to-html'

import styles from './styles.module.css'

const markdownContent = `
## Intro

I love the [JRE Podcast](https://www.youtube.com/channel/UCESLZhusAkFfsNsApnjF_Cg). But search and discovery with podcasts can be really challenging.

I built this project to solve this problem... and I also wanted to play around with cool AI stuff. 😂

This project uses the latest models from [OpenAI](https://openai.com/) to build a semantic search index across every episode of the Pod. It allows you to find your favorite moments with Google-level accuracy and rewatch the exact clips you're interested in.

You can use it to power advanced search across _any YouTube channel or playlist_. The demo uses the [JRE Podcast](https://www.youtube.com/channel/UCzQUP1qoWDoEbmsQxvdjxgQ) because it's my favorite 💕, but it's designed to work with any playlist.

## Example Queries

- [dmt visions](/?query=dmt+visions)
- [best advice for fitness](/?query=best+advice+for+fitness)
- [alpha brain](/?query=alpha+brain)
- [elk meat](/?query=elk+meat)
- [tony hinchcliffe](/?query=tony+hinchcliffe)
- [intellectual honesty](/?query=intellectual+honesty)
- [moon landing](/?query=moon+landing)
- [ufo aliens](/?query=ufo+aliens)

## How It Works

This project is [open source](${config.githubRepoUrl})! 😄

Under the hood, it uses:

- [OpenAI](https://openai.com) - We're using the brand new [text-embedding-ada-002](https://openai.com/blog/new-and-improved-embedding-model/) embedding model, which captures deeper information about text in a latent space with 1536 dimensions
  - This allows us to go beyond keyword search and search by higher-level topics.
- [Pinecone](https://www.pinecone.io) - Hosted vector search which enables us to efficiently perform [k-NN searches](https://en.wikipedia.org/wiki/K-nearest_neighbors_algorithm) across these embeddings
- [Vercel](https://vercel.com) - Hosting and API functions
- [Next.js](https://nextjs.org) - React web framework

We use Node.js and the [YouTube API v3](https://developers.google.com/youtube/v3/getting-started) to fetch the videos of our target playlist. In this case, we're focused on the [JRE Podcast Episodes Playlist](https://www.youtube.com/watch?v=fRyyTAs1XY8&list=PLk1Sqn_f33KuWf3tW9BBe_4TP7x8l0m3Tl), which contains 2210 videos at the time of writing.

\`\`\`bash
npx tsx src/bin/resolve-yt-playlist.ts
\`\`\`

We download the English transcripts for each episode using a hacky HTML scraping solution, since the YouTube API doesn't allow non-OAuth access to captions. Note that a few episodes don't have automated English transcriptions available, so we're just skipping them at the moment. A better solution would be to use [Whisper](https://openai.com/blog/whisper/) to transcribe each episode's audio.

Once we have all of the transcripts and metadata downloaded locally, we pre-process each video's transcripts, breaking them up into reasonably sized chunks of ~100 tokens and fetch it's [text-embedding-ada-002](https://openai.com/blog/new-and-improved-embedding-model/) embedding from OpenAI. This results in ~200 embeddings per episode.

All of these embeddings are then upserted into a [Pinecone](https://www.pinecone.io) index with a dimensionality of 1536. There are ~17,575 embeddings in total across ~2210 episodes of teh JRE Podcast.

\`\`\`bash
npx tsx src/bin/process-yt-playlist.ts
\`\`\`

Once our Pinecone search index is set up, we can start querying it either via the webapp or via the example CLI:

\`\`\`bash
npx tsx src/bin/query.ts
\`\`\`

If you want to generate thumbnails (optional), run:

\`\`\`bash
npx tsx src/bin/generate-thumbnails.ts
\`\`\`

Note that thumbnail generation takes ~2 hours and requires a pretty stable internet connection.

The frontend is a [Next.js](https://nextjs.org) webapp deployed to [Vercel](https://vercel.com) that uses our Pinecone index as a primary data store.

## Feedback

Have an idea on how this webapp could be improved? Find a particularly fun search query?

Feel free to send me feedback, either on [GitHub](https://github.com/vdutts7/jre-ai/issues/new) or [Twitter](https://twitter.com/vdutts7). 💯

## License

This project is [open source](${config.githubRepoUrl}). MIT © [${config.author}](${config.twitterUrl})

Support my open source work by [sponsoring me](${config.githubSponsorsUrl}) or <a href="${config.twitterUrl}">following me on twitter</a>. The API and server costs add up over time, so if you can spare it, [sponsoring me on Github](${config.githubSponsorsUrl}) is greatly appreciated. 💕

**This project is not affiliated with the JRE Podcast**. It just pulls data from the [YouTube channel](https://www.youtube.com/channel/UCESLZhusAkFfsNsApnjF_Cg.
`

export default function AboutPage({
  content
}: InferGetStaticPropsType<typeof getStaticProps>) {
  return (
    <Layout>
      <div className={styles.aboutPage}>
        <div className={styles.meta}>
          <h1 className={styles.title}>{config.title}</h1>
          <p className={styles.detail}>
            <a
              className='link'
              href={config.twitterUrl}
              title={`Twitter ${config.twitter}`}
              target='_blank'
              rel='noopener noreferrer'
            >
              By vdutts7
            </a>
          </p>
        </div>

        <Markdown content={content} />
      </div>
    </Layout>
  )
}

export const getStaticProps = async () => {
  const content = await markdownToHtml(markdownContent)

  return {
    props: {
      content
    }
  }
}
