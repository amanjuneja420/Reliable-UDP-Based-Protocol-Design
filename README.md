# Reliable UDP-Based Protocol (RUDP) Design

A robust, message-oriented transport protocol built on top of unreliable UDP sockets. This project guarantees end-to-end data integrity, ordered message delivery, and custom error control without the connection overhead of TCP.

## Features
- **Go-Back-N ARQ**: Implements a sliding window protocol to detect dropped packets and automatically retransmit unacknowledged data.
- **Multithreaded Daemon Architecture**: A background daemon (`init`) runs independent sender, receiver, and garbage-collection threads to handle concurrent data transfer.
- **Shared Memory IPC**: Communication between user applications and the background daemon happens at lightning speed using system shared memory.
- **Custom Application API**: Provides a seamless interface mimicking standard socket programming (`m_socket`, `m_bind`, `m_sendto`, `m_recvfrom`, `m_close`).
- **Simulated Network Degradation**: Includes a configurable message drop function to stress-test the protocol under variable network loss conditions.

## Architecture

The system utilizes a split architecture between user applications and a central daemon:

### 1. The Daemon (`initmsocket.c`)
The daemon process manages all background networking tasks so the user applications don't have to block on network I/O. It spawns three main threads:
* **R (Receiver Thread)**: Listens for incoming UDP packets. If it receives an in-order data packet, it places it in the receive buffer and updates the advertised receive window (`rwnd`). If it receives an ACK, it slides the sender window forward.
* **S (Sender Thread)**: Periodically checks the send buffer for unacknowledged messages. If a message timeout expires, it retransmits the entire sliding window (Go-Back-N). Otherwise, it sends newly queued messages.
* **G (Garbage Collector)**: Wakes up periodically to clean up shared memory segments of processes that have terminated.

### 2. Shared Memory (`msocket.h`)
The daemon and user applications communicate through shared memory structs:
* **`swnd`**: Sender window tracking `send_window_size`, `unack_time`, message lengths, and circular buffer indices (`start_index`, `last_sent_index`, `end_index`).
* **`rwnd`**: Receiver window tracking `receive_window_size`, `last_inorder_msg`, sequence numbers, and buffer indices.

## Application API (`msocket.c`)
User applications link against `libmsocket.a` and use the following API:

* `m_socket(domain, type, protocol)`: Requests a new RUDP socket. The daemon allocates a shared memory segment for it.
* `m_bind(sockfd, src_addr, src_addrlen, dest_addr, dest_addrlen)`: Binds the socket to a source and destination IP/Port.
* `m_sendto(sockfd, message, length, flags, dest_addr, dest_addrlen)`: Places a message into the shared memory send buffer. The daemon handles the actual transmission.
* `m_recvfrom(sockfd, buffer, length, flags, address, address_len)`: Pulls the next in-order message from the receive buffer.
* `m_close(sockfd)`: Marks the socket as inactive so the garbage collector can clean it up.

## Build Instructions

This project uses modular Makefiles. 

1. First, compile the shared static library (`libmsocket.a`):
```bash
make -f libmsocket.mk
```
2. Then, compile the daemon (`init`) and test applications (`user1` and `user2`):
```bash
make -f run.mk
```

## Running the Protocol

1. Start the daemon process in a terminal:
```bash
./init
```
2. Start the receiver application (`user2`) in a separate terminal:
```bash
./user2
```
3. Start the sender application (`user1`) in a third terminal. It will begin transferring data from `send.txt`:
```bash
./user1
```
