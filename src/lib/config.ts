export const environment = process.env.NODE_ENV || 'development'
export const isDev = environment === 'development'
export const isServer = typeof window === 'undefined'
export const isSafari =
  !isServer && /^((?!chrome|android).)*safari/i.test(navigator.userAgent)

export const title = 'JRE AI'
export const description =
  'Search across the JRE Podcast using an advanced semantic search index powered by AI.'
export const domain = 'jre--ai.vercel.app'

export const author = 'vdutts7'
export const twitter = 'vdutts7'
export const twitterUrl = `https://twitter.com/${twitter}`
export const githubRepoUrl =
  'https://github.com/vdutts7/jre-ai'
export const githubSponsorsUrl =
  'https://github.com/sponsors/vdutts7'
export const copyright = `Copyright 2024 ${author}`
export const madeWithLove = 'Made with ❤️'

export const port = process.env.PORT || '3000'
export const prodUrl = `https://${domain}`
export const url = isDev ? `http://localhost:${port}` : prodUrl

export const apiBaseUrl =
  isDev || !process.env.VERCEL_URL ? url : `https://${process.env.VERCEL_URL}`

// these must all be absolute urls
export const socialImageUrl = `${url}/social.jpg`

// ---

export const openaiEmbeddingModel = 'text-embedding-ada-002'
