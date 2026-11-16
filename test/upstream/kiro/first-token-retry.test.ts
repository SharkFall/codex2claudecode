import { describe, expect, test } from "bun:test"

import { FirstTokenTimeoutError, streamWithFirstTokenRetry } from "../../../src/upstream/kiro/stream-retry"

const encoder = new TextEncoder()

function immediateResponse(...chunks: string[]) {
  return new Response(new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk))
      controller.close()
    },
  }))
}

function stalledResponse(onCancel?: () => void) {
  return new Response(new ReadableStream<Uint8Array>({
    cancel() {
      onCancel?.()
    },
  }))
}

async function responseText(response: Response) {
  const reader = response.body!.getReader()
  const decoder = new TextDecoder()
  let text = ""
  for (;;) {
    const chunk = await reader.read()
    if (chunk.done) break
    text += decoder.decode(chunk.value, { stream: true })
  }
  return text + decoder.decode()
}

describe("Kiro first-token retry", () => {
  test("returns immediate streams without retrying and preserves the first chunk", async () => {
    let calls = 0
    const response = await streamWithFirstTokenRetry(() => {
      calls += 1
      return Promise.resolve(immediateResponse("first", "second"))
    }, { timeoutMs: 5, maxRetries: 1 })

    expect(calls).toBe(1)
    expect(await responseText(response)).toBe("firstsecond")
  })

  test("retries a stalled first attempt before returning the successful stream", async () => {
    let calls = 0
    let cancelled = 0
    const response = await streamWithFirstTokenRetry(() => {
      calls += 1
      if (calls === 1) return Promise.resolve(stalledResponse(() => { cancelled += 1 }))
      return Promise.resolve(immediateResponse("ok"))
    }, { timeoutMs: 1, maxRetries: 1 })

    expect(calls).toBe(2)
    expect(cancelled).toBe(1)
    expect(await responseText(response)).toBe("ok")
  })

  test("does not wait for timed-out body cancellation before retrying", async () => {
    let calls = 0
    let cancelStarted = false
    const response = await streamWithFirstTokenRetry(() => {
      calls += 1
      if (calls === 1) {
        return Promise.resolve(new Response(new ReadableStream<Uint8Array>({
          cancel() {
            cancelStarted = true
            return new Promise(() => {})
          },
        })))
      }
      return Promise.resolve(immediateResponse("recovered"))
    }, { timeoutMs: 1, maxRetries: 1 })

    expect(calls).toBe(2)
    expect(cancelStarted).toBe(true)
    expect(await responseText(response)).toBe("recovered")
  })

  test("throws after all first-token attempts stall", async () => {
    let calls = 0
    try {
      await streamWithFirstTokenRetry(() => {
        calls += 1
        return Promise.resolve(stalledResponse())
      }, { timeoutMs: 1, maxRetries: 2 })
      throw new Error("expected first-token timeout")
    } catch (error) {
      expect(error).toBeInstanceOf(FirstTokenTimeoutError)
      expect((error as FirstTokenTimeoutError).attempts).toBe(3)
      expect(calls).toBe(3)
    }
  })

  test("propagates caller aborts without retrying indefinitely", async () => {
    const controller = new AbortController()
    let calls = 0
    const promise = streamWithFirstTokenRetry(() => {
      calls += 1
      return Promise.resolve(stalledResponse())
    }, { signal: controller.signal, timeoutMs: 50, maxRetries: 3 })

    controller.abort(new DOMException("caller aborted", "AbortError"))

    try {
      await promise
      throw new Error("expected abort")
    } catch (error) {
      expect(error).toBeInstanceOf(DOMException)
      expect((error as DOMException).name).toBe("AbortError")
      expect(calls).toBe(1)
    }
  })
})

describe("Kiro response wait boundaries", () => {
  test("bounds waiting for response headers without replaying the request", async () => {
    let calls = 0
    let signal: AbortSignal | undefined
    let resolveResponse: ((response: Response) => void) | undefined
    const pending = streamWithFirstTokenRetry((currentSignal) => {
      calls += 1
      signal = currentSignal
      return new Promise<Response>((resolve) => { resolveResponse = resolve })
    }, { headersTimeoutMs: 5, timeoutMs: 5, maxRetries: 2 })
    await expect(pending).rejects.toThrow("Kiro response headers timed out")
    expect(calls).toBe(1)
    expect(signal?.aborted).toBe(true)

    let cancelled = false
    resolveResponse!(stalledResponse(() => { cancelled = true }))
    await Promise.resolve()
    expect(cancelled).toBe(true)
  })

  test("caller abort stops a header wait even when makeResponse ignores cancellation", async () => {
    const caller = new AbortController()
    let calls = 0
    const pending = streamWithFirstTokenRetry(() => {
      calls += 1
      return new Promise<Response>(() => {})
    }, { signal: caller.signal, headersTimeoutMs: 10_000 })
    caller.abort(new DOMException("caller stopped", "AbortError"))
    await expect(pending).rejects.toThrow("caller stopped")
    expect(calls).toBe(1)
  })

  test("does not replay a stream after its first metadata chunk", async () => {
    let calls = 0
    let sourceController: ReadableStreamDefaultController<Uint8Array> | undefined
    const source = new ReadableStream<Uint8Array>({
      start(controller) {
        sourceController = controller
        controller.enqueue(encoder.encode('{"usage":1}'))
      },
    })
    const response = await streamWithFirstTokenRetry(() => {
      calls += 1
      return Promise.resolve(new Response(source))
    }, { timeoutMs: 5, maxRetries: 2 })
    const reader = response.body!.getReader()
    expect(new TextDecoder().decode((await reader.read()).value)).toBe('{"usage":1}')
    sourceController!.error(new Error("upstream disconnected"))
    await expect(reader.read()).rejects.toThrow("upstream disconnected")
    reader.releaseLock()
    expect(calls).toBe(1)
    expect(source.locked).toBe(false)
  })

  test("releases the source reader when the rebuilt response is cancelled", async () => {
    let cancelled = false
    const source = new ReadableStream<Uint8Array>({
      start(controller) { controller.enqueue(encoder.encode("first")) },
      cancel() { cancelled = true },
    })
    const response = await streamWithFirstTokenRetry(() => Promise.resolve(new Response(source)))
    await response.body!.cancel("client disconnected")
    expect(cancelled).toBe(true)
    expect(source.locked).toBe(false)
  })
})
