# Design Notes

Internal notes on how the shared-memory layout and threads fit together, plus
a small measurement of retransmission cost under simulated loss.

> **Known bug:** transfers that require sequence numbers to wrap past
> `MAX_SEQ_NUM` become unreliable (stall or silently corrupt data). See
> [Known issues in the README](../README.md#known-issues) for details and
> reproduction steps.

## Retransmissions vs. drop probability

Average number of transmissions needed to get a message through, measured
while sweeping the simulated drop probability `p` (see `dropMessage()` in
[msocket.c](../msocket.c)):

| Drop probability (p) | Avg. transmissions |
|---|---|
| 0.05 | 1.76 |
| 0.10 | 2.12 |
| 0.15 | 2.54 |
| 0.20 | 2.90 |
| 0.25 | 3.20 |
| 0.30 | 3.51 |
| 0.35 | 3.84 |
| 0.40 | 4.02 |
| 0.45 | 4.65 |
| 0.50 | 4.90 |

As expected for Go-Back-N, cost grows roughly with `1 / (1 - p)` — every lost
packet forces the whole in-flight window to be resent, not just the dropped
packet.

## `msocket.h`

- **`SOCKINFO`** — used by the user process and the daemon to create and bind
  a socket.
- **`swnd`** (send window) — `send_window_size`, `unack_time`, per-slot
  message `length`/`validmssg`, `rem_buff_space`, and the circular-buffer
  indices `start_index` / `last_sent_index` / `end_index`, each paired with
  its ack number.
- **`rwnd`** (receive window) — `receive_window_size`, `last_inorder_msg`,
  `last_inorder_msg_seq_num`, the `recv_msg`/`msg_size` arrays, `start_index`
  / `start_seq_num`, and `nospace` (set once the window fills up).
- **`shared_memory`** — one slot per active socket: `free`, `pid`, `sockfd`,
  destination `sockaddr_in`, `send_buffer`/`recv_buffer`, and the `swnd`/`rwnd`
  structs above.

## `msocket.c` (user-facing API)

1. **`m_socket`** — allocates an entry via the daemon and returns a socket fd.
2. **`m_bind`** — binds source/destination addresses and initializes the
   shared-memory entry.
3. **`m_sendto`** — validates input and writes into the send buffer; the
   sender thread picks it up from there.
4. **`m_recvfrom`** — validates input and copies the next in-order message out
   of the receive buffer.
5. **`m_close`** — marks the entry free once nothing is left to send (returns
   `EBUSY` otherwise); the garbage collector reclaims it later.
6. **`dropMessage(probability)`** — simulates loss: returns 1 (drop) with the
   given probability, 0 otherwise.

## `initmsocket.c` (daemon)

- **`update_Receive_Window_Size`** / **`update_Receiver_Last_Inorder_Msg`** —
  recompute the receive window and the last in-order sequence number as
  messages arrive out of order or fill gaps.
- **`R` (receiver thread)** — blocks on `select()`; on timeout it resends
  unacked messages, on data it updates the receive buffer/window and forwards
  in-order messages to the user, applying `dropMessage` to simulate loss.
- **`S` (sender thread)** — resends unacked messages on timeout and flushes
  newly queued messages from the send buffer (Go-Back-N).
- **`G` (garbage collector thread)** — periodically frees shared-memory
  entries whose owning process has finished.
