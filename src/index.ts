import { AwsClient } from "aws4fetch"
import { Hono } from "hono"
import { logger } from "hono/logger"

type Bindings = {
  RANGE_RETRY_ATTEMPTS: number
  END_POINT: string
  ACCESS_KEY: string
  SECRET_KEY: string
  BUCKET_NAME: string
}

const app = new Hono<{ Bindings: Bindings }>()

app.use(logger())

app.all("/:filename{.*}", async (c) => {
  const filename = c.req.param("filename")
  const method = c.req.method

  const aws = new AwsClient({
    accessKeyId: c.env.ACCESS_KEY,
    secretAccessKey: c.env.SECRET_KEY,
    service: "s3",
  })

  const endpoint = `${c.env.END_POINT}/${c.env.BUCKET_NAME}/${filename}`

  let req

  switch (method) {
    case "GET":
      req = await aws.sign(endpoint, {
        method: "GET",
        headers: c.res.headers,
      })
      if (req.headers.has("range")) {
        let attempts = c.env.RANGE_RETRY_ATTEMPTS
        let response: Response
        do {
          const controller = new AbortController()
          response = await fetch(req.url, {
            method: req.method,
            headers: req.headers,
            signal: controller.signal,
          })

          if (response.headers.has("content-range")) {
            if (attempts < c.env.RANGE_RETRY_ATTEMPTS) {
              console.log(
                `Retry for ${req.url} succeeded - response has content-range header`,
              )
            }
            break
          }

          if (response.ok) {
            attempts -= 1
            console.error(
              `Range header in request for ${req.url} but no content-range header in response. Will retry ${attempts} more times`,
            )
            if (attempts > 0) {
              controller.abort()
            }
          } else {
            break
          }
        } while (attempts > 0)
        if (attempts <= 0) {
          console.error(
            `Tried range request for ${req.url} ${c.env.RANGE_RETRY_ATTEMPTS} times, but no content-range in response.`,
          )
        }
        return response
      }
      return fetch(req)

    case "PUT":
      const putBody = await c.req.arrayBuffer()
      req = await aws.sign(endpoint, {
        method: "PUT",
        headers: c.res.headers,
        body: putBody,
      })
      return fetch(req)

    case "DELETE":
      req = await aws.sign(endpoint, {
        method: "DELETE",
        headers: c.res.headers,
      })
      return fetch(req)

    case "POST":
      const postBody = await c.req.arrayBuffer()
      req = await aws.sign(endpoint, {
        method: "POST",
        headers: c.res.headers,
        body: postBody,
      })
      return fetch(req)

    case "PATCH":
      const patchBody = await c.req.arrayBuffer()
      req = await aws.sign(endpoint, {
        method: "PATCH",
        headers: c.res.headers,
        body: patchBody,
      })
      return fetch(req)

    case "HEAD":
      req = await aws.sign(endpoint, {
        method: "HEAD",
        headers: c.res.headers,
      })
      return fetch(req)

    default:
      return c.notFound()
  }
})

export default app
