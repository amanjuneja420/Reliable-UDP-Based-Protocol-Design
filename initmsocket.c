/**
 * @file initmsocket.c
 * @brief Standalone daemon for the Message Transmission Protocol (MTP).
 *
 * This program runs as a background daemon and must be started before any user
 * programs (user1, user2). It owns the shared memory and semaphores, and performs
 * all network I/O operations asynchronously.
 *
 * Architecture:
 * - main(): Initializes shared memory, semaphores, spawns R/S/G threads, and
 *           handles m_socket() / m_bind() requests in a continuous loop.
 * - Thread R (Receiver): Uses select() on all UDP sockets, parses packets,
 *                        places data in receive buffers, and sends ACKs.
 * - Thread S (Sender): Periodically checks send buffers, sends new messages,
 *                      and handles Go-Back-N retransmissions for timeouts.
 * - Thread G (Garbage Collector): Detects dead user processes and frees their
 *                                 shared memory slots.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <msocket.h>        // angle brackets because we compile with -I.
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/select.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/socket.h>
#include <sys/types.h>

// Theory: The Receiver Thread (R) — most complex part
// The R thread must watch multiple UDP sockets simultaneously (one per active MTP connection). It uses select() for this.
// select() lets you watch a set of file descriptors and tells you which ones have data ready to read — without blocking on any single one.

// 1. Receive raw UDP packet
// 2. Check dropMessage(p) → maybe simulate packet loss
// 3. Parse the header (type, IP, port, seq_num, size)
// 4. If TYPE=1 (ACK):
//      → advance send window (slide start_index forward)
//      → update send_window_size with advertised rwnd
// 5. If TYPE=0 (DATA):
//      → find which SM slot this belongs to (match IP+port)
//      → check if seq_num is in the expected range
//      → place in recv_buffer at correct position
//      → send ACK back with last_inorder_msg_seq_num + rwnd size


// Theory: The Sender Thread (S)
// Runs every T/3 seconds. Does two jobs:
// Job 1 — Retransmit: Check if any sent-but-unACKed message has been waiting longer than T seconds. If yes → Go-Back-N resend everything from start_index to last_sent_index.
// Job 2 — Send new: Check if last_sent_index < end_index AND send window has room. If yes → send next message

// Theory: Helper functions (update_Receive_Window_Size and update_Receiver_Last_Inorder_Msg)
// update_Receiver_Last_Inorder_Msg():
//   Scans recv_msg[] forward from last_inorder_msg
//   Keeps advancing while consecutive slots are filled
//   Stops at the first gap → that's the new last_inorder_msg

// update_Receive_Window_Size():
//   Counts empty slots from last_inorder_msg+1 back to start_index
//   That count = how many more messages we can accept


// ── Globals ────────────────────────────────────────────────────────────────
// These are shared between the threads (they all run in the same process)
int total_transmissions = 0;   // counts total packets sent (for statistics)
struct shared_memory *SM;      // pointer to the shared memory array (attached in main)

// Scratch buffers for building outgoing packets
// (not in shared memory — private to the daemon process)
char send_buffer[1500], recv_buffer[1500];

/**
 * @brief Recalculates the receiver's advertised window size.
 *
 * Counts empty slots from (last_inorder_msg + 1) backwards to start_index.
 *
 * @param i  Index of the MTP socket in the shared memory array.
 * @param SM Pointer to the shared memory array.
 */
void update_Receive_Window_Size(int i, struct shared_memory *SM) {
    int win_size = 0;
    int j = (SM[i].rwnd.last_inorder_msg + 1) % RECV_BUFFER_SIZE;

    // Count contiguous empty slots starting from last_inorder_msg + 1
    while (win_size < RECV_BUFFER_SIZE) {
        if (SM[i].rwnd.recv_msg[j] == 0) {
            win_size++;
            j = (j + 1) % RECV_BUFFER_SIZE;
        } else {
            break; // Stop at the first filled slot
        }
    }
    SM[i].rwnd.receive_window_size = win_size;
}

/**
 * @brief Advances the last in-order message pointer.
 *
 * After placing a new message in the receive buffer, advances the pointer
 * as far forward as consecutive slots are filled.
 *
 * @param i  Index of the MTP socket in the shared memory array.
 * @param SM Pointer to the shared memory array.
 */
void update_Receiver_Last_Inorder_Msg(int i, struct shared_memory *SM) {
    int j = SM[i].rwnd.last_inorder_msg;
    int advanced = 0;
    int next = (j + 1) % RECV_BUFFER_SIZE;

    // Keep advancing while the next expected slot is filled
    while (SM[i].rwnd.recv_msg[next] == 1 && advanced < RECV_BUFFER_SIZE) {
        j = next;
        next = (j + 1) % RECV_BUFFER_SIZE;
        advanced++;
    }

    SM[i].rwnd.last_inorder_msg = j;
}


// ACK packet format sent back to the sender:
//   "1$<DEST_IP>$<DEST_PORT>$<LAST_INORDER_SEQ>$<RWND_STR_LEN>$<RWND_VALUE>"
//    ─┬─  ──────────────────  ────────────────   ─────────────   ──────────
//     │   e.g. 127.0.0.1:8181    e.g. 7            e.g. 2          e.g. 10
//     1=ACK
// Example: "1$127.0.0.1$8181$7$2$10"
//   → "I have received all packets up to seq 7, and I have room for 10 more bytes (rwnd_str_len=2)"
// Note: the 5th field is the *string length* of the rwnd number, not the rwnd itself.

/**
 * @brief Receiver thread (R).
 *
 * Runs continuously. Uses select() to watch all active UDP sockets simultaneously.
 * Handles incoming data packets (TYPE 0) and acknowledgment packets (TYPE 1).
 *
 * @param params Unused thread parameters.
 * @return NULL
 */
void *R(void *params) {
    // Attach to shared memory (R runs as a thread but needs its own shmat)
    key_t key = KEY;
    int shmid = shmget(key, N * sizeof(struct shared_memory), 0777);
    struct shared_memory *SM = (struct shared_memory *)shmat(shmid, 0, 0);

    // Get the shared memory mutex semaphore
    key_t sem_sm_key = SEM_SM_KEY;
    int sem_sm = semget(sem_sm_key, 1, 0777);

    // pop/vop needed for P() and V() macros
    struct sembuf pop, vop;
    pop.sem_num = vop.sem_num = 0;
    pop.sem_flg = vop.sem_flg = 0;
    pop.sem_op = -1;
    vop.sem_op = 1;

    struct timeval timeout;
    timeout.tv_sec = T;
    timeout.tv_usec = 0;

    while (1) {
        // Small sleep so Sender/Garbage threads also get CPU time
        // (prevents this thread from starving others when holding the lock)
        usleep(1000);
        P(sem_sm);   // acquire shared memory lock
        printf("Receiver\n");

        // Build the fd_set from all active sockets
        fd_set readfd;
        FD_ZERO(&readfd);
        int max_fd = -1;
        for (int i = 0; i < N; i++) {
            if (SM[i].free == 0) {
                FD_SET(SM[i].sockfd, &readfd);
                printf("Socket setting here: %d\n", SM[i].sockfd);
                if (SM[i].sockfd > max_fd) {
                    max_fd = SM[i].sockfd;
                }
            }
        }

        // Reset timeout each iteration (select() modifies it on Linux)
        timeout.tv_sec = T;
        timeout.tv_usec = 0;

        int ret = select(max_fd + 1, &readfd, NULL, NULL, &timeout);

        if (ret < 0) {
            // select() itself failed (e.g. interrupted by signal)
            perror("Error in select");
            V(sem_sm);
            continue;

        } else if (ret == 0) {
            // Timeout: no data arrived within T seconds
            printf("Timedout\n");

            // If receiver buffer was full (nospace=1) but now has room,
            // send a probing ACK to unblock the sender
            for (int i = 0; i < N; i++) {
                if (SM[i].rwnd.nospace == 1 && SM[i].rwnd.receive_window_size != 0) {
                    SM[i].rwnd.nospace = 0;

                    // Build and send a probe ACK
                    char *ack = (char *)malloc(1500);
                    char *rwnd_size = (char *)malloc(1000);
                    sprintf(rwnd_size, "%d", SM[i].rwnd.receive_window_size);
                    int size_len = strlen(rwnd_size);
                    sprintf(ack, "1$%s$%d$%d$%d$",
                            inet_ntoa(SM[i].addr.sin_addr),
                            ntohs(SM[i].addr.sin_port),
                            SM[i].rwnd.last_inorder_msg_seq_num,
                            size_len);
                    int len = strlen(ack);
                    for (int k = 0; k < size_len; k++) {
                        ack[len + k] = rwnd_size[k];
                    }
                    if (sendto(SM[i].sockfd, ack, len + size_len, 0,
                               (struct sockaddr *)&SM[i].addr,
                               sizeof(struct sockaddr_in)) < 0) {
                        perror("Error sending probe ACK");
                    }
                    free(ack);
                    free(rwnd_size);
                }
            }

        } else {
            // Data is ready on one or more sockets
            for (int i = 0; i < N; i++) {
                if (SM[i].free == 0 && FD_ISSET(SM[i].sockfd, &readfd)) {

                    struct sockaddr_in sender_addr;
                    socklen_t sender_len = sizeof(sender_addr);

                    // Receive the raw UDP packet into recv_buffer (daemon's private buffer)
                    int n = recvfrom(SM[i].sockfd, recv_buffer, 1500, 0,
                                     (struct sockaddr *)&sender_addr, &sender_len);
                    if (n < 0) {
                        perror("recvfrom error");
                        continue;
                    }
                    recv_buffer[n] = '\0';  // null-terminate for string ops

                    // Simulate packet loss
                    // dropMessage(p) returns 1 with probability p → discard
                    if (dropMessage(p)) {
                        printf("Dropping message\n");
                        continue;
                    }

                    // Parse the header using strtok
                    // Header format: "TYPE$DESTIP$PORT$SEQNUM$SIZE$"
                    // then payload bytes follow immediately
                    char *message = (char *)malloc(1500);
                    memcpy(message, recv_buffer, n);
                    message[n] = '\0';

                    char *token = strtok(message, "$");
                    int type = atoi(token);           // 0=data, 1=ACK

                    token = strtok(NULL, "$");
                    char *dest_ip = token;            // destination IP string

                    token = strtok(NULL, "$");
                    int dest_port = atoi(token);      // destination port

                    token = strtok(NULL, "$");
                    int seq_num = atoi(token);        // sequence number

                    token = strtok(NULL, "$");
                    int size = atoi(token);           // payload size in bytes

                    // Payload starts right after the last "$" in the header
                    // strtok has been moving a pointer through message[];
                    // the payload is the next `size` bytes after the last token
                    int header_len = (token + strlen(token) + 1) - message;

                    printf("Type: %d, Dest IP: %s, Dest Port: %d, Seq: %d, Size: %d\n",
                           type, dest_ip, dest_port, seq_num, size);

                    // TYPE = 1: This is an ACK
                    if (type == 1) {
                        // seq_num here = last in-order seq num the receiver ACKed
                        // size field  = receiver's advertised window size (rwnd)
                        int rwnd_size = 0;
                        // The "payload" in an ACK is the rwnd size as a string
                        for (int k = 0; k < size; k++) {
                            rwnd_size = rwnd_size * 10 + (recv_buffer[header_len + k] - '0');
                        }
                        printf("ACK received: seq=%d, rwnd=%d\n", seq_num, rwnd_size);

                        // Slide the send window forward:
                        // Everything up to seq_num has been received by the other side
                        // Find how many new messages are now ACKed
                        if (seq_num != SM[i].swnd.start_index_ack_no) {
                            // New ACK (not a duplicate)
                            // Calculate the new start_index in the circular send buffer
                            int steps;
                            if (seq_num >= SM[i].swnd.start_index_ack_no) {
                                steps = seq_num - SM[i].swnd.start_index_ack_no;
                            } else {
                                // Wrapped around MAX_SEQ_NUM
                                steps = (MAX_SEQ_NUM - SM[i].swnd.start_index_ack_no) + seq_num;
                            }

                            int new_start_index = (SM[i].swnd.start_index + steps) % SEND_BUFFER_SIZE;
                            SM[i].swnd.start_index_ack_no = seq_num;

                            // Mark ACKed slots as free (validmssg = 0)
                            int j = SM[i].swnd.start_index;
                            while (j != new_start_index) {
                                SM[i].swnd.validmssg[j] = 0;
                                j = (j + 1) % SEND_BUFFER_SIZE;
                            }
                            SM[i].swnd.start_index = new_start_index;
                            printf("New start index: %d\n", new_start_index);

                            // Update send window size from receiver's advertised rwnd
                            SM[i].swnd.send_window_size = rwnd_size;
                            printf("New window size: %d\n", rwnd_size);

                            // Update receiver state (used when we ARE the receiver side)
                            update_Receiver_Last_Inorder_Msg(i, SM);
                            printf("Here I am\n");
                            update_Receive_Window_Size(i, SM);

                        } else {
                            // Duplicate ACK: peer didn't advance → just update window
                            SM[i].swnd.send_window_size = rwnd_size;
                        }
                    }

                    // TYPE = 0: This is a DATA packet
                    else {
                        // The packet was received on SM[i].sockfd, so it belongs to slot i.
                        int slot = i;

                        // Check if seq_num falls within our expected receive window
                        // Expected range: [start_seq_num ... start_seq_num + recv_window - 1]
                        int start_seq = SM[slot].rwnd.start_seq_num;
                        int win = SM[slot].rwnd.receive_window_size;

                        // Check if in window (accounting for wrap-around)
                        int in_window = 0;
                        for (int w = 0; w < win; w++) {
                            if (((start_seq + w - 1) % MAX_SEQ_NUM + 1) == seq_num) {
                                in_window = 1;
                                break;
                            }
                        }

                        if (!in_window) {
                            // Out of window: send a duplicate ACK for last in-order msg
                            printf("Out of window: seq=%d, start=%d, win=%d\n",
                                   seq_num, start_seq, win);
                        } else {
                            // Calculate buffer index from seq_num offset
                            int offset;
                            if (seq_num >= start_seq) {
                                offset = seq_num - start_seq;
                            } else {
                                offset = (MAX_SEQ_NUM - start_seq) + seq_num;
                            }
                            int buf_index = (SM[slot].rwnd.start_index + offset) % RECV_BUFFER_SIZE;

                            // Only store if slot is empty (avoid overwriting on duplicate)
                            if (SM[slot].rwnd.recv_msg[buf_index] == 0) {
                                // Copy payload into recv_buffer
                                for (int k = 0; k < size; k++) {
                                    SM[slot].recv_buffer[buf_index][k] = recv_buffer[header_len + k];
                                }
                                SM[slot].rwnd.recv_msg[buf_index] = 1;  // mark slot filled
                                SM[slot].rwnd.msg_size[buf_index] = size;

                                // Advance last_inorder_msg as far as we can
                                update_Receiver_Last_Inorder_Msg(slot, SM);
                                SM[slot].rwnd.last_inorder_msg_seq_num =
                                    ((start_seq + (SM[slot].rwnd.last_inorder_msg
                                      - SM[slot].rwnd.start_index + RECV_BUFFER_SIZE)
                                      % RECV_BUFFER_SIZE) - 1) % MAX_SEQ_NUM + 1;
                            }
                        }

                        // Send ACK back to sender
                        // ACK format: "1$DESTIP$PORT$LAST_INORDER_SEQ$SIZE$RWND_VALUE"
                        update_Receive_Window_Size(slot, SM);

                        char *ack = (char *)malloc(1500);
                        char *rwnd_str = (char *)malloc(100);
                        sprintf(rwnd_str, "%d", SM[slot].rwnd.receive_window_size);
                        int rsize = strlen(rwnd_str);
                        sprintf(ack, "1$%s$%d$%d$%d$",
                                inet_ntoa(SM[slot].addr.sin_addr),
                                ntohs(SM[slot].addr.sin_port),
                                SM[slot].rwnd.last_inorder_msg_seq_num,
                                rsize);
                        int ack_len = strlen(ack);
                        for (int k = 0; k < rsize; k++) {
                            ack[ack_len + k] = rwnd_str[k];
                        }
                        printf("Sending ACK: %s\n", ack);
                        if (sendto(SM[slot].sockfd, ack, ack_len + rsize, 0,
                                   (struct sockaddr *)&SM[slot].addr,
                                   sizeof(struct sockaddr_in)) < 0) {
                            perror("Error sending ACK");
                        }
                        free(ack);
                        free(rwnd_str);

                        // If buffer is now full, set the nospace flag
                        update_Receive_Window_Size(slot, SM);
                        if (SM[slot].rwnd.receive_window_size == 0) {
                            SM[slot].rwnd.nospace = 1;
                        }
                    }

                    free(message);
                }
            }
        }
        printf("Receiver Leaving\n");
        V(sem_sm);   // release shared memory lock
    }
}


// Key design points in Thread R:
//
//   P(sem_sm) / V(sem_sm)
//     R holds the shared-memory mutex for the entire duration of packet processing.
//     Threads S and G must wait. This prevents race conditions on the SM array.
//
//   dropMessage(p)
//     Simulates 5% packet loss. If a packet is dropped, R simply skips it.
//     The sender's T-second retransmission timer ensures the packet is resent.
//
//   strtok()
//     Modifies the string in-place by replacing '$' delimiters with '\0'.
//     That is why the raw recv_buffer bytes are first copied into `message`
//     before strtok is called — the original buffer must stay intact for
//     payload extraction via the header_len offset.
//
//   Probe ACK on select() timeout
//     If nospace==1 (the receive buffer was full when the sender last heard from us)
//     and the window has since reopened, R sends an unsolicited ACK to unblock the
//     sender, which is stuck waiting for a non-zero rwnd.

/**
 * @brief Sender thread (S).
 *
 * Wakes every T/3 seconds to perform two jobs:
 * 1. Retransmit: If the oldest unacknowledged message has timed out, resend all
 *    messages in the current window (Go-Back-N).
 * 2. Send new: Send newly queued messages if the send window allows.
 *
 * @param params Unused thread parameters.
 * @return NULL
 */
void *S(void *params) {

    key_t key = KEY;
    int shmid = shmget(key, N * sizeof(struct shared_memory), 0777);
    struct shared_memory *SM = (struct shared_memory *)shmat(shmid, 0, 0);

    struct sembuf pop, vop;
    pop.sem_num = vop.sem_num = 0;
    pop.sem_flg = vop.sem_flg = 0;
    pop.sem_op = -1;
    vop.sem_op = 1;

    key_t sem_sm_key = SEM_SM_KEY;
    int sem_sm = semget(sem_sm_key, 1, 0777);

    while (1) {
        sleep(T / 3);    // wake up every T/3 seconds
        P(sem_sm);
        printf("Sender\n");
        fflush(stdout);

        // PASS 1: Retransmission — check if the oldest unACKed msg has timed out
        for (int i = 0; i < N; i++) {
            if (SM[i].free == 0) {

                // j = start_index (oldest unACKed slot); handle -1 sentinel
                int j = (SM[i].swnd.start_index > -1) ? SM[i].swnd.start_index : 0;

                // Check if timer has expired for the oldest message
                // unack_time[j] == -1 means no message sent yet at this slot
                if (SM[i].swnd.unack_time[j] != -1 &&
                    (SM[i].swnd.unack_time[j] + T < time(NULL))) {

                    // Go-Back-N: resend ALL messages from start_index to last_sent_index
                    do {

                        // Build header: "0$DESTIP$PORT$SEQNUM$SIZE$"
                        // Sequence number for slot j:
                        //   start_index_ack_no is the seq of start_index
                        //   offset = circular distance from start_index to j
                        int offset = (j - SM[i].swnd.start_index + SEND_BUFFER_SIZE) % SEND_BUFFER_SIZE;
                        int seq = (SM[i].swnd.start_index_ack_no + offset - 1) % MAX_SEQ_NUM + 1;
                        if (SM[i].swnd.start_index_ack_no == 0) {
                            SM[i].swnd.start_index_ack_no = 1;
                        }

                        int msglen = SM[i].swnd.length[j];
                        sprintf(send_buffer, "0$%s$%d$%d$%d$",
                                inet_ntoa(SM[i].addr.sin_addr),
                                ntohs(SM[i].addr.sin_port),
                                seq,
                                msglen);

                        int len = strlen(send_buffer);
                        printf("\nMessage from S thread(retransmit): %s", send_buffer);

                        // Append payload after the header
                        for (int k = 0; k < msglen; k++) {
                            send_buffer[len + k] = SM[i].send_buffer[j][k];
                        }

                        if (sendto(SM[i].sockfd, send_buffer, len + msglen, 0,
                                   (struct sockaddr *)&SM[i].addr,
                                   sizeof(struct sockaddr_in)) < 0) {
                            perror("Error retransmitting");
                            break;
                        }

                        total_transmissions++;
                        printf("Sender: Retransmitting via sockfd=%d to %s:%d\n",
                               SM[i].sockfd,
                               inet_ntoa(SM[i].addr.sin_addr),
                               ntohs(SM[i].addr.sin_port));

                        // Reset the timer for this slot
                        SM[i].swnd.unack_time[j] = time(NULL);
                        j = (j + 1) % SEND_BUFFER_SIZE;
                    } while (j != (SM[i].swnd.last_sent_index + 1) % SEND_BUFFER_SIZE);
                }
            }
        }
        // PASS 2: Send new messages — advance last_sent_index if window allows
        for (int i = 0; i < N; i++) {
            if (SM[i].free == 0) {

                printf("Sockid: %d\n", SM[i].sockfd);
                printf("SM[i].swnd.last_sent_index %d\n", SM[i].swnd.last_sent_index);
                printf("SM[i].swnd.start_index %d\n", SM[i].swnd.start_index);
                printf("SM[i].swnd.end_index %d\n", SM[i].swnd.end_index);

                // Try to send up to send_window_size new messages
                for (int j = 1; j <= SM[i].swnd.send_window_size; j++) {

                    // Condition: last_sent is behind end (there are unsent messages)
                    // AND the slot at (last_sent+1) is valid
                    // circular distance: last_sent_index + 1 - start_index <= end_index - start_index
                    int sent_dist = (SM[i].swnd.last_sent_index + 1 - SM[i].swnd.start_index + SEND_BUFFER_SIZE) % SEND_BUFFER_SIZE;
                    int end_dist  = (SM[i].swnd.end_index      - SM[i].swnd.start_index + SEND_BUFFER_SIZE) % SEND_BUFFER_SIZE;
                    int next_slot = (SM[i].swnd.last_sent_index + 1) % SEND_BUFFER_SIZE;

                    if (sent_dist <= end_dist && SM[i].swnd.validmssg[next_slot]) {

                        // Advance last_sent_index to the next slot
                        SM[i].swnd.last_sent_index = next_slot;
                        // Advance sequence number (wraps 1..MAX_SEQ_NUM)
                        SM[i].swnd.last_sent_ack_no = (SM[i].swnd.last_sent_ack_no) % MAX_SEQ_NUM + 1;

                        int msglen = SM[i].swnd.length[SM[i].swnd.last_sent_index];
                        printf("Inet: %s\n", inet_ntoa(SM[i].addr.sin_addr));
                        printf("Port: %d\n", ntohs(SM[i].addr.sin_port));
                        printf("Msglen: %d\n", msglen);

                        sprintf(send_buffer, "0$%s$%d$%d$%d$",
                                inet_ntoa(SM[i].addr.sin_addr),
                                ntohs(SM[i].addr.sin_port),
                                SM[i].swnd.last_sent_ack_no,
                                msglen);

                        printf("\nMessage from S thread(new send): %s\n", send_buffer);
                        int len = strlen(send_buffer);

                        // Append payload
                        for (int k = 0; k < msglen; k++) {
                            send_buffer[len + k] = SM[i].send_buffer[SM[i].swnd.last_sent_index][k];
                        }

                        if (sendto(SM[i].sockfd, send_buffer, len + msglen, 0,
                                   (struct sockaddr *)&SM[i].addr,
                                   sizeof(struct sockaddr_in)) < 0) {
                            // Rollback on send failure
                            SM[i].swnd.last_sent_index = (SM[i].swnd.last_sent_index - 1 + SEND_BUFFER_SIZE) % SEND_BUFFER_SIZE;
                            SM[i].swnd.last_sent_ack_no = (SM[i].swnd.last_sent_ack_no - 2 + MAX_SEQ_NUM) % MAX_SEQ_NUM + 1;
                            perror("Error sending new message");
                            break;
                        }

                        total_transmissions++;
                        SM[i].swnd.send_window_size--;   // consume one window slot
                        // Record when this message was sent (for timeout check)
                        SM[i].swnd.unack_time[SM[i].swnd.last_sent_index] = time(NULL);

                    } else {
                        break;  // No more messages fit in the window
                    }
                }
            }
        }

        printf("Sender Leaving\n");
        printf("total_transmissions: %d\n", total_transmissions);
        V(sem_sm);
    }
}

/**
 * @brief Garbage Collector thread (G).
 *
 * Wakes every T/2 seconds. Checks if the process owning each active socket
 * is still alive. If dead, and no unacknowledged messages remain, frees the slot.
 *
 * @param params Unused thread parameters.
 * @return NULL
 */
void *G(void *params) {

    key_t key = KEY;
    int shmid = shmget(key, N * sizeof(struct shared_memory), 0777);
    struct shared_memory *SM = (struct shared_memory *)shmat(shmid, 0, 0);

    key_t sem_sm_key = SEM_SM_KEY;
    int sm_sem = semget(sem_sm_key, 1, 0777);

    struct sembuf pop, vop;
    pop.sem_num = vop.sem_num = 0;
    pop.sem_flg = vop.sem_flg = 0;
    pop.sem_op = -1;
    vop.sem_op = 1;

    while (1) {
        sleep(T / 2);
        P(sm_sem);
        printf("Garbage\n");

        for (int i = 0; i < N; i++) {
            if (SM[i].free == 0) {
                int pid = SM[i].pid;

                // kill(pid, 0) = "are you alive?" — doesn't actually send a signal
                // returns -1 if process doesn't exist
                int status = kill(pid, 0);
                if (status < 0) {
                    // Process is dead — check if safe to free
                    int unacked = 0;
                    for (int j = 0; j < SEND_BUFFER_SIZE; j++) {
                        if (SM[i].swnd.validmssg[j] == 1) {
                            unacked = 1;
                            break;
                        }
                    }
                    if (unacked) {
                        continue;   // Still has unACKed data — don't free yet
                    }
                    // Safe to free: reset the slot to initial state
                    SM[i].free = 1;
                    SM[i].pid = -1;
                    SM[i].swnd.send_window_size = 0;
                    SM[i].swnd.start_index = 0;
                    SM[i].swnd.last_sent_index = 0;
                    SM[i].swnd.end_index = 0;
                    SM[i].swnd.start_index_ack_no = 0;
                    SM[i].swnd.last_sent_ack_no = 0;
                    SM[i].swnd.rem_buff_space = SEND_BUFFER_SIZE;
                    for (int j = 0; j < SEND_BUFFER_SIZE; j++) {
                        SM[i].swnd.unack_time[j] = 0;
                    }
                }
            }
        }

        printf("Garbage Leaving\n");
        V(sm_sem);
    }
}

/**
 * @brief Main daemon entry point.
 *
 * Initializes IPC resources, spawns worker threads, and enters an infinite
 * loop to handle socket creation and binding requests from user applications.
 *
 * @return 0 on normal termination (though daemon typically runs forever).
 */
int main() {

    // Create and initialize all semaphores 
    key_t sem_sm_key = SEM_SM_KEY;
    int sm_sem = semget(sem_sm_key, 1, 0777 | IPC_CREAT);
    semctl(sm_sem, 0, SETVAL, 1);    // initialized to 1 (mutex: unlocked)

    struct sembuf pop, vop;
    pop.sem_num = vop.sem_num = 0;
    pop.sem_flg = vop.sem_flg = 0;
    pop.sem_op = -1;
    vop.sem_op = 1;

    key_t sem1_key = SEM1_KEY;
    key_t sem2_key = SEM2_KEY;
    int sem1 = semget(sem1_key, 1, 0777 | IPC_CREAT);
    int sem2 = semget(sem2_key, 1, 0777 | IPC_CREAT);
    semctl(sem1, 0, SETVAL, 0);    // sem1 starts at 0: blocks until user signals
    semctl(sem2, 0, SETVAL, 0);    // sem2 starts at 0: blocks until daemon signals

    // Create SOCKINFO shared memory (the tiny mailbox)
    key_t sock_info_key = SOCK_INFO_KEY;
    int sock_info = shmget(sock_info_key, sizeof(struct SOCKINFO), 0777 | IPC_CREAT);
    struct SOCKINFO *sockinfo = (struct SOCKINFO *)shmat(sock_info, 0, 0);
    sockinfo->sock_id = 0;
    sockinfo->error_no = 0;

    // Create main shared memory: N slots, all free initially
    key_t key = KEY;
    int shmid = shmget(key, N * sizeof(struct shared_memory), 0777 | IPC_CREAT);
    struct shared_memory *SM = (struct shared_memory *)shmat(shmid, 0, 0);
    for (int i = 0; i < N; i++) {
        SM[i].free = 1;   // 1 = available
    }

    fflush(stdout);

    // Spawn R, S, G threads
    // pthread_detach: we don't need to join them — they run forever
    pthread_t rid, sid, gid;
    pthread_attr_t r_attr, s_attr, g_attr;
    pthread_attr_init(&r_attr);
    pthread_attr_init(&s_attr);
    pthread_attr_init(&g_attr);

    pthread_create(&rid, &r_attr, R, NULL);
    pthread_detach(rid);

    int res = pthread_create(&sid, &s_attr, S, NULL);
    if (res != 0) {
        perror("Error creating S thread");
    }
    pthread_detach(sid);

    pthread_create(&gid, &g_attr, G, NULL);
    pthread_detach(gid);

    // Main loop: handle m_socket() and m_bind() requests
    while (1) {
        P(sem1);   // block here until m_socket() or m_bind() signals us
        printf("Main\n");

        if (sockinfo->sock_id == 0) {
            // Case 1: sock_id is 0 → m_socket() called → create a UDP socket
            sockinfo->sock_id = socket(AF_INET, SOCK_DGRAM, 0);
            if (sockinfo->sock_id < 0) {
                perror("socket() failed");
                sockinfo->error_no = errno;
                sockinfo->sock_id = -1;   // signal error back to caller
            }

        } else {
            // Case 2: sock_id is non-zero → m_bind() called → bind the socket
            printf("Address: %s\n", inet_ntoa(sockinfo->addr.sin_addr));
            printf("Port: %d\n", ntohs(sockinfo->addr.sin_port));
            fflush(stdout);

            struct sockaddr_in serv;
            socklen_t len = sizeof(serv);

            if (bind(sockinfo->sock_id,
                     (struct sockaddr *)&(sockinfo->addr), len) < 0) {
                perror("bind() failed");
                sockinfo->error_no = errno;
                sockinfo->sock_id = -1;   // signal error back to caller
            }
        }

        V(sem2);   // unblock the waiting user process (m_socket or m_bind)
    }
}

