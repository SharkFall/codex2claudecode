import { responseHeaders } from "../../core/http"
import { kiroFirstTokenMaxRetries, kiroFirstTokenTimeoutMs, kiroResponseHeadersTimeoutMs } from "./constants"

export class FirstTokenTimeoutError extends Error {
  readonly attempts: number

  constructor(attempts: number) {
    super(`Kiro stream did not emit a first token after ${attempts} attempt${attempts === 1 ? "" : "s"}`)
    this.name = "FirstTokenTimeoutError"
    this.attempts = attempts
  }
}

export class ResponseHeadersTimeoutError extends Error {
  readonly timeoutMs: number

  constructor(timeoutMs: number) {
    super(`Kiro response headers timed out after ${timeoutMs} ms`)
    this.name = "ResponseHeadersTimeoutError"
    this.timeoutMs = timeoutMs
  }
}

export async function streamWithFirstTokenRetry(
  makeResponse: (signal?: AbortSignal) => Promise<Response>,
  options: { signal?: AbortSignal; timeoutMs?: number; headersTimeoutMs?: number; maxRetries?: number } = {},
) {
  const timeoutMs = options.timeoutMs ?? kiroFirstTokenTimeoutMs()
  const headersTimeoutMs = options.headersTimeoutMs ?? kiroResponseHeadersTimeoutMs()
  const maxRetries = options.maxRetries ?? kiroFirstTokenMaxRetries()
  const maxAttempts = maxRetries + 1

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    throwIfAborted(options.signal)
    const attemptController = new AbortController()
    const signal = combineSignals(options.signal, attemptController.signal)
    // Header waits include authentication and HTTP retries and are not automatically replayed here.
    const response = await waitForResponse(makeResponse, signal, attemptController, headersTimeoutMs)
    const reader = response.body?.getReader()
    if (!reader) return response

    try {
      const first = await readFirstChunk(reader, timeoutMs, signal)
      if (first.done) return rebuildResponse(response, reader)
      return rebuildResponse(response, reader, first.value)
    } catch (error) {
      attemptController.abort(error)
      void reader.cancel(error).catch(() => undefined)
      reader.releaseLock()
      if (!(error instanceof FirstTokenAttemptTimeoutError)) throw error
      if (attempt >= maxAttempts) throw new FirstTokenTimeoutError(attempt)
    }
  }

  throw new FirstTokenTimeoutError(maxAttempts)
}

async function waitForResponse(
  makeResponse: (signal?: AbortSignal) => Promise<Response>,
  signal: AbortSignal | undefined,
  controller: AbortController,
  timeoutMs: number,
) {
  throwIfAborted(signal)
  let timeout: ReturnType<typeof setTimeout> | undefined
  let onAbort: (() => void) | undefined
  let abandoned = false
  let received: Response | undefined
  const pending = makeResponse(signal)
  void pending.then((response) => {
    received = response
    if (abandoned) void response.body?.cancel(controller.signal.reason).catch(() => undefined)
  }, () => undefined)
  try {
    return await Promise.race([
      pending,
      new Promise<never>((_, reject) => {
        timeout = setTimeout(() => reject(new ResponseHeadersTimeoutError(timeoutMs)), timeoutMs)
        onAbort = () => reject(signal?.reason ?? new DOMException("The operation was aborted", "AbortError"))
        signal?.addEventListener("abort", onAbort, { once: true })
        if (signal?.aborted) onAbort()
      }),
    ])
  } catch (error) {
    abandoned = true
    controller.abort(error)
    if (received) void received.body?.cancel(error).catch(() => undefined)
    throw error
  } finally {
    if (timeout) clearTimeout(timeout)
    if (onAbort) signal?.removeEventListener("abort", onAbort)
  }
}

function rebuildResponse(response: Response, reader: ReadableStreamDefaultReader<Uint8Array>, firstChunk?: Uint8Array) {
  let sentFirstChunk = firstChunk === undefined
  let finished = false
  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (!sentFirstChunk && firstChunk) {
        sentFirstChunk = true
        controller.enqueue(firstChunk)
        return
      }
      try {
        const chunk = await reader.read()
        if (finished) return
        if (chunk.done) {
          finished = true
          reader.releaseLock()
          controller.close()
        } else {
          controller.enqueue(chunk.value)
        }
      } catch (error) {
        if (finished) return
        finished = true
        reader.releaseLock()
        controller.error(error)
      }
    },
    cancel(reason) {
      if (finished) return
      finished = true
      void reader.cancel(reason).catch(() => undefined)
      reader.releaseLock()
    },
  }, { highWaterMark: 0 })
  return new Response(body, { status: response.status, statusText: response.statusText, headers: responseHeaders(response.headers) })
}

async function readFirstChunk(reader: ReadableStreamDefaultReader<Uint8Array>, timeoutMs: number, signal?: AbortSignal) {
  let timeout: ReturnType<typeof setTimeout> | undefined
  let abortListener: (() => void) | undefined
  throwIfAborted(signal)
  try {
    return await Promise.race([
      reader.read(),
      new Promise<never>((_, reject) => {
        timeout = setTimeout(() => reject(new FirstTokenAttemptTimeoutError()), timeoutMs)
        if (signal) {
          abortListener = () => reject(signal.reason ?? new DOMException("The operation was aborted", "AbortError"))
          signal.addEventListener("abort", abortListener, { once: true })
          if (signal.aborted) abortListener()
        }
      }),
    ])
  } finally {
    if (timeout) clearTimeout(timeout)
    if (signal && abortListener) signal.removeEventListener("abort", abortListener)
  }
}

function combineSignals(...signals: Array<AbortSignal | undefined>) {
  const active = signals.filter((signal): signal is AbortSignal => Boolean(signal))
  if (!active.length) return undefined
  if (active.length === 1) return active[0]
  return AbortSignal.any(active)
}

function throwIfAborted(signal?: AbortSignal) {
  if (!signal?.aborted) return
  throw signal.reason ?? new DOMException("The operation was aborted", "AbortError")
}

class FirstTokenAttemptTimeoutError extends Error {
  constructor() {
    super("Kiro stream first-token timeout")
    this.name = "FirstTokenAttemptTimeoutError"
  }
}
