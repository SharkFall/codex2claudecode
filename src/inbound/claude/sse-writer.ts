import type { JsonObject } from "../types"

/**
 * Claude SSE block writer that encapsulates content block index allocation,
 * open block tracking, and mid-stream error recovery.
 *
 * Provides typed helpers for all Claude SSE event types and ensures
 * valid SSE framing even when upstream exceptions occur after content
 * blocks have started.
 */
export class ClaudeSseWriter {
  private readonly encoder = new TextEncoder()
  private readonly controller: ReadableStreamDefaultController<Uint8Array>
  private closed = false
  private contentIndex = 0
  private textOpen = false
  private thinkingOpen = false
  private readonly toolBlocks = new Map<string, { index: number; name: string; arguments: string }>()

  constructor(controller: ReadableStreamDefaultController<Uint8Array>) {
    this.controller = controller
  }

  /** Whether the writer has been closed (no more events can be sent). */
  get isClosed(): boolean {
    return this.closed
  }

  /** Current content block index. */
  get currentIndex(): number {
    return this.contentIndex
  }

  /** Whether a text block is currently open. */
  get isTextOpen(): boolean {
    return this.textOpen
  }

  /** Whether a thinking block is currently open. */
  get isThinkingOpen(): boolean {
    return this.thinkingOpen
  }

  /** Tool blocks stay open until their complete arguments arrive. */
  get hasOpenToolBlocks(): boolean {
    return this.toolBlocks.size > 0
  }

  /** Next content index that won't collide with already-emitted blocks. */
  nextContentIndex(): number {
    return this.contentIndex
  }

  /** Send a raw SSE event. */
  send(event: string, data: JsonObject): void {
    if (this.closed) return
    try {
      this.controller.enqueue(this.encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`))
    } catch {
      this.closed = true
    }
  }

  /** Emit message_start event. */
  messageStart(message: JsonObject): void {
    this.send("message_start", { type: "message_start", message })
  }

  /** Open a text content block if not already open. */
  startTextBlock(): void {
    if (this.textOpen) return
    this.textOpen = true
    this.send("content_block_start", {
      type: "content_block_start",
      index: this.contentIndex,
      content_block: { type: "text", text: "" },
    })
  }

  /** Close the current text block if open. */
  stopTextBlock(): void {
    if (!this.textOpen) return
    this.send("content_block_stop", { type: "content_block_stop", index: this.contentIndex })
    this.textOpen = false
    this.contentIndex += 1
  }

  /** Open a thinking content block. Closes text block first if open. */
  startThinkingBlock(signature: string): void {
    if (this.thinkingOpen) return
    this.stopTextBlock()
    this.thinkingOpen = true
    this.send("content_block_start", {
      type: "content_block_start",
      index: this.contentIndex,
      content_block: { type: "thinking", thinking: "", signature },
    })
  }

  /** Close the current thinking block if open, emitting signature delta first. */
  stopThinkingBlock(signature?: string): void {
    if (!this.thinkingOpen) return
    if (signature) {
      this.send("content_block_delta", {
        type: "content_block_delta",
        index: this.contentIndex,
        delta: { type: "signature_delta", signature },
      })
    }
    this.send("content_block_stop", { type: "content_block_stop", index: this.contentIndex })
    this.thinkingOpen = false
    this.contentIndex += 1
  }

  /** Emit a text delta within the current text block. */
  textDelta(text: string): void {
    this.send("content_block_delta", {
      type: "content_block_delta",
      index: this.contentIndex,
      delta: { type: "text_delta", text },
    })
  }

  /**
   * Emit one citation delta within the current text block.
   *
   * One event per citation, each appending to the `citations` list of the text block at the
   * current index — the shape the Messages API documents for `citations_delta`.
   */
  citationDelta(citation: JsonObject): void {
    this.send("content_block_delta", {
      type: "content_block_delta",
      index: this.contentIndex,
      delta: { type: "citations_delta", citation },
    })
  }

  /** Emit a thinking delta within the current thinking block. */
  thinkingDelta(thinking: string): void {
    this.send("content_block_delta", {
      type: "content_block_delta",
      index: this.contentIndex,
      delta: { type: "thinking_delta", thinking },
    })
  }

  /** Open a tool block once and forward argument fragments without waiting for completion. */
  toolUseDelta(callId: string, name: string, argumentsDelta: string): void {
    const state = this.ensureToolUseBlock(callId, name)
    if (!argumentsDelta) return
    this.send("content_block_delta", {
      type: "content_block_delta",
      index: state.index,
      delta: { type: "input_json_delta", partial_json: argumentsDelta },
    })
    state.arguments += argumentsDelta
  }

  /** Complete a tool block, emitting only the argument suffix not already streamed. */
  toolUseBlock(callId: string, name: string, args: string): void {
    const state = this.ensureToolUseBlock(callId, name)
    if (!args.startsWith(state.arguments)) throw new Error("Completed tool arguments do not match streamed arguments")
    this.toolUseDelta(callId, name, args.slice(state.arguments.length))
    this.send("content_block_stop", { type: "content_block_stop", index: state.index })
    this.toolBlocks.delete(callId)
  }

  private ensureToolUseBlock(callId: string, name: string) {
    const existing = this.toolBlocks.get(callId)
    if (existing) {
      if (existing.name !== name) throw new Error("Tool name changed during argument streaming")
      return existing
    }
    this.stopThinkingBlock()
    this.stopTextBlock()
    const state = { index: this.contentIndex++, name, arguments: "" }
    this.toolBlocks.set(callId, state)
    this.send("content_block_start", {
      type: "content_block_start",
      index: state.index,
      content_block: { type: "tool_use", id: callId, name, input: {} },
    })
    this.send("content_block_delta", {
      type: "content_block_delta",
      index: state.index,
      delta: { type: "input_json_delta", partial_json: "" },
    })
    return state
  }

  /** Emit server tool blocks (server_tool_use, web_search_tool_result, mcp_tool_use, etc.). */
  serverToolBlocks(blocks: JsonObject[]): void {
    for (const block of blocks) {
      this.stopTextBlock()
      this.stopThinkingBlock()
      const isServerToolUse = block.type === "server_tool_use" || block.type === "mcp_tool_use"
      this.send("content_block_start", {
        type: "content_block_start",
        index: this.contentIndex,
        content_block: isServerToolUse
          ? {
              type: block.type,
              id: block.id,
              name: block.name,
              ...(block.type === "mcp_tool_use" && { server_name: block.server_name }),
              input: {},
            }
          : block,
      })
      if (isServerToolUse && block.input) {
        this.send("content_block_delta", {
          type: "content_block_delta",
          index: this.contentIndex,
          delta: { type: "input_json_delta", partial_json: "" },
        })
        this.send("content_block_delta", {
          type: "content_block_delta",
          index: this.contentIndex,
          delta: { type: "input_json_delta", partial_json: JSON.stringify(block.input) },
        })
      }
      this.send("content_block_stop", { type: "content_block_stop", index: this.contentIndex })
      this.contentIndex += 1
    }
  }

  /** Emit message_delta with final usage and stop reason. */
  messageDelta(stopReason: string, usage: JsonObject): void {
    this.send("message_delta", {
      type: "message_delta",
      delta: { stop_reason: stopReason, stop_sequence: null },
      usage,
    })
  }

  /** Emit message_stop event. */
  messageStop(): void {
    this.send("message_stop", { type: "message_stop" })
  }

  /** Emit a ping event. */
  ping(): void {
    this.send("ping", { type: "ping" })
  }

  /** Emit an error event. */
  error(errorBody: JsonObject): void {
    this.send("error", errorBody)
  }

  /**
   * Close text and thinking blocks before ending a stream.
   * Unfinished tool blocks stay incomplete when an error terminates the response.
   */
  closeOpenBlocks(thinkingSignature?: string): void {
    this.stopThinkingBlock(thinkingSignature)
    this.stopTextBlock()
  }

  /** Close the underlying stream controller. */
  close(): void {
    if (this.closed) return
    this.closed = true
    this.toolBlocks.clear()
    try {
      this.controller.close()
    } catch {
      // Controller already closed or errored — safe to ignore
    }
  }
}
