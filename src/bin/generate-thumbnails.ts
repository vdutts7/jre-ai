async function fetchVectorsWithRetry(
  index,
  idChunk,
  maxRetries = 3,
  retryDelay = 1000
) {
  let attempts = 0
  let delay = retryDelay

  while (attempts < maxRetries) {
    try {
      const response = await index.fetch({
        ids: idChunk,
        includeMetadata: true,
        includeValues: true
      })

      // Attempt to validate and parse the response
      if (!response || !response.vectors) {
        throw new Error('Incomplete JSON response: response or vectors missing')
      }

      // Optionally log the response to confirm its structure
      console.log('Response received:', JSON.stringify(response))

      return response
    } catch (error) {
      attempts++
      console.warn(
        `Fetch attempt ${attempts} failed: ${error.message || error}`
      )
      if (attempts === maxRetries) {
        console.error('Max retries reached. Error:', error)
        throw error
      }

      await new Promise((resolve) => setTimeout(resolve, delay))
      delay *= 2 // Exponential backoff for retries
    }
  }
}
