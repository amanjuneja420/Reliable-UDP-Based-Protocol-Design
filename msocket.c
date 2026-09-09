/**
 * @file msocket.c
 * @brief Message Transmission Protocol (MTP) API implementation.
 *
 * This library is linked by user programs to provide the m_socket, m_bind,
 * m_sendto, m_recvfrom, and m_close interface. It interacts with the background
 * initmsocket daemon via shared memory and semaphores to provide reliable UDP.
 *
 * User programs only read/write shared memory through these API functions;
 * the actual network socket operations are handled asynchronously by the daemon.
 */

// note <> brackets when header is system header, and "" when local header
#include <arpa/inet.h>
#include <errno.h>
#include "msocket.h"
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/shm.h>
#include <sys/socket.h>
#include <time.h>

/* 
 * Data path overview:
 * USER side calls m_sendto() -> writes to send_buffer -> S thread reads -> UDP sendto()
 * UDP recvfrom() -> R thread reads -> writes to recv_buffer -> USER calls m_recvfrom()
 */



/**
 * @brief Creates an MTP socket.
 *
 * Requests the daemon to create an underlying UDP socket via the SOCKINFO mailbox.
 * Upon success, it reserves a slot in the shared memory array.
 *
 * @param domain   Communication domain (e.g., AF_INET).
 * @param type     Socket type (must be SOCK_MTP).
 * @param protocol Protocol to be used.
 * @return Index (0 to N-1) into the shared memory array acting as the MTP socket descriptor,
 *         or -1 on error (with errno set appropriately).
 */
int m_socket(int domain, int type, int protocol) {

    // Step 1: Validate the socket type — must be SOCK_MTP (= SOCK_DGRAM)
    if (type != SOCK_MTP) {
        errno = EINVAL;   // "Invalid argument"
        return -1;
    }

    // Step 2: Attach to the main shared memory (the array of N slots)
    key_t key = KEY;
    int shmid = shmget(key, N * sizeof(struct shared_memory), 0777);
    struct shared_memory *SM = (struct shared_memory *)shmat(shmid, NULL, 0);
    if (SM == NULL) {
        errno = ENOMEM;
        printf("Init process not called \n");
        return -1;
    }
    int free_available = 0;
    int m_sockfd;

    // Step 3: Attach to SOCKINFO mailbox
    key_t sock_info_key = SOCK_INFO_KEY;
    int sock_info = shmget(sock_info_key, sizeof(struct SOCKINFO), 0777);
    struct SOCKINFO *sockinfo = (struct SOCKINFO *)shmat(sock_info, 0, 0);

    // Step 4: Get both semaphores
    // sem1: user signals daemon ("please do socket()")
    // sem2: daemon signals user ("I'm done")
    key_t sem1_key = SEM1_KEY;
    key_t sem2_key = SEM2_KEY;
    int sem1 = semget(sem1_key, 1, 0777);
    int sem2 = semget(sem2_key, 1, 0777);

    // These two structs define the P and V operations for semaphores
    // The P/V macros in msocket.h use &pop and &vop
    struct sembuf pop, vop;
    pop.sem_num = vop.sem_num = 0;   // which semaphore in the set (index 0)
    pop.sem_flg = vop.sem_flg = 0;   // flags (0 = default blocking)
    pop.sem_op = -1;                  // P: decrement (acquire/lock)
    vop.sem_op = 1;                   // V: increment (release/unlock)

    // Step 5: Find a free slot and do the handshake with daemon
    for (int i = 0; i < N; i++) {
        if (SM[i].free == 1) {
            // Signal daemon: "create a socket"
            // SOCKINFO is empty (sock_id = 0) → daemon knows to call socket()
            V(sem1);       // wake up the daemon
            P(sem2);       // wait for daemon to reply

            // Check if daemon succeeded
            if (sockinfo->sock_id == -1) {
                errno = sockinfo->error_no;
                return -1;
            }

            free_available = 1;
            SM[i].free = 0;                  // mark slot as in-use
            SM[i].sockfd = sockinfo->sock_id; // store the real UDP fd
            SM[i].pid = getpid();            // remember which process owns this
            m_sockfd = i;                    // our "MTP socket descriptor" = index
            break;
        }
    }

    // Reset mailbox for next use
    sockinfo->sock_id = 0;
    sockinfo->error_no = 0;

    if (free_available == 0) {
        errno = ENOBUFS;   // No free slots in the N-slot array
        return -1;
    }

    return m_sockfd;   // return the INDEX (0..N-1), not a real fd!
}

/**
 * @brief Binds the MTP socket to a local address and registers the peer address.
 *
 * Passes the source address to the daemon to bind the underlying UDP socket,
 * and records the destination address in shared memory for the Sender thread.
 * Initializes the send and receive window states.
 *
 * @param m_sockfd     MTP socket descriptor (index into shared memory).
 * @param src_addr     Local address to bind to.
 * @param src_addrlen  Size of the local address structure.
 * @param dest_addr    Address of the remote peer to communicate with.
 * @param dest_addrlen Size of the destination address structure.
 * @return 0 on success, or -1 on error (with errno set appropriately).
 */

int m_bind(int m_sockfd, const struct sockaddr *src_addr, socklen_t src_addrlen,
           const struct sockaddr *dest_addr, socklen_t dest_addrlen) {

    struct sembuf pop, vop;
    pop.sem_num = vop.sem_num = 0;
    pop.sem_flg = vop.sem_flg = 0;
    pop.sem_op = -1;
    vop.sem_op = 1;

    // Attach to main shared memory
    key_t key = KEY;
    int shmid = shmget(key, N * sizeof(struct shared_memory), 0777);
    struct shared_memory *SM = (struct shared_memory *)shmat(shmid, NULL, 0);

    // Store the DESTINATION address in SM[m_sockfd].addr
    // (The sender thread needs to know where to send packets)
    SM[m_sockfd].addr = *((struct sockaddr_in *)dest_addr);

    // Initialize the SEND window
    SM[m_sockfd].swnd.send_window_size = MAX_WINDOW_SIZE;
    // SM[m_sockfd].swnd.last_ack=0;
    SM[m_sockfd].swnd.rem_buff_space = SEND_BUFFER_SIZE;
    SM[m_sockfd].swnd.start_index = -1;       // -1 means "buffer is empty"
    SM[m_sockfd].swnd.last_sent_index = -1;
    SM[m_sockfd].swnd.end_index = -1;
    SM[m_sockfd].swnd.start_index_ack_no = 0;
    SM[m_sockfd].swnd.last_sent_ack_no = 0;

    memset(SM[m_sockfd].swnd.unack_time, -1, sizeof(SM[m_sockfd].swnd.unack_time));
    memset(SM[m_sockfd].swnd.length, -1, sizeof(SM[m_sockfd].swnd.length));
    memset(SM[m_sockfd].swnd.validmssg, 0, sizeof(SM[m_sockfd].swnd.validmssg));

    // Initialize the RECEIVE window
    SM[m_sockfd].rwnd.receive_window_size = MAX_WINDOW_SIZE;
    SM[m_sockfd].rwnd.last_inorder_msg = -1;  // -1 = nothing received yet
    SM[m_sockfd].rwnd.last_inorder_msg_seq_num = 0;
    SM[m_sockfd].rwnd.start_index = 0;
    // CRITICAL FIX: The sender starts sequence numbers at 1 (0 % 16 + 1 = 1).
    // If start_seq_num is 0, the receiver puts seq=1 into buffer index 1,
    // leaving buffer index 0 empty forever. This causes ENOMSG.
    SM[m_sockfd].rwnd.start_seq_num = 1;
    SM[m_sockfd].rwnd.nospace = 0;

    memset(SM[m_sockfd].rwnd.recv_msg, 0, sizeof(SM[m_sockfd].rwnd.recv_msg));
    memset(SM[m_sockfd].rwnd.msg_size, 0, sizeof(SM[m_sockfd].rwnd.msg_size));

    // Tell daemon to call bind() on the real socket
    key_t sock_info_key = SOCK_INFO_KEY;
    int sock_info = shmget(sock_info_key, sizeof(struct SOCKINFO), 0777);
    struct SOCKINFO *sockinfo = (struct SOCKINFO *)shmat(sock_info, 0, 0);
    if (sockinfo == NULL) {
        errno = ENOMEM;
        printf("Init process not called \n");
        return -1;
    }

    key_t sem1_key = SEM1_KEY;
    key_t sem2_key = SEM2_KEY;
    int sem1 = semget(sem1_key, 1, 0777);
    int sem2 = semget(sem2_key, 1, 0777);

    // Fill the mailbox: sock_id is already set → daemon knows to call bind()
    sockinfo->sock_id = SM[m_sockfd].sockfd;
    sockinfo->addr = *((struct sockaddr_in *)src_addr);  // OUR address
    sockinfo->error_no = 0;

    printf("Sockinfo: %d, %s, %d\n", sockinfo->sock_id,
           inet_ntoa(sockinfo->addr.sin_addr), ntohs(sockinfo->addr.sin_port));

    V(sem1);   // signal daemon: "please bind"
    P(sem2);   // wait for daemon to finish binding
    printf("Semaphore complete\n");

    // Check result
    if (sockinfo->sock_id < 0) {
        errno = sockinfo->error_no;
        return -1;
    }

    // Clean up the mailbox
    memset(sockinfo, 0, sizeof(struct SOCKINFO));

    shmdt(sockinfo);
    shmdt(SM);

    return 0;
}

/**
 * @brief Queues a message in the shared memory send buffer.
 *
 * Does not send directly on the network. The daemon's Sender thread asynchronously
 * picks up messages from the circular buffer and sends them.
 *
 * @param m_sockfd     MTP socket descriptor.
 * @param message      Pointer to the message payload to send.
 * @param length       Length of the message payload.
 * @param flags        Flags (unused in this implementation).
 * @param dest_addr    Destination address (must match the bound peer address).
 * @param dest_addrlen Size of the destination address structure.
 * @return 0 on success (message queued), or -1 on error (with errno set).
 *         If the buffer is full, errno is set to ENOBUFS and the caller should retry.
 */
ssize_t m_sendto(int m_sockfd, const void *message, size_t length, int flags,
                 const struct sockaddr *dest_addr, socklen_t dest_addrlen) {

    key_t key = KEY;
    int shmid = shmget(key, N * sizeof(struct shared_memory), 0777);
    struct shared_memory *SM = (struct shared_memory *)shmat(shmid, NULL, 0);

    // Lock the shared memory — prevent daemon threads from interfering
    key_t sem_key = SEM_SM_KEY;
    int sem = semget(sem_key, 1, 0777);
    struct sembuf pop, vop;
    pop.sem_num = vop.sem_num = 0;
    pop.sem_flg = vop.sem_flg = 0;
    pop.sem_op = -1;
    vop.sem_op = 1;

    P(sem);   // acquire the shared memory mutex

    // Error checks
    if (SM == NULL) {
        printf("Init process not called \n");
        V(sem);
        errno = ENOMEM;
        return -1;
    }
    if (m_sockfd < 0 || m_sockfd >= N) {
        V(sem);
        errno = EBADF;    // Bad file descriptor: index out of range
        return -1;
    }
    if (SM[m_sockfd].free == 1) {
        V(sem);
        errno = EBADF;    // Socket is not open
        return -1;
    }
    if (SM[m_sockfd].pid != getpid()) {
        perror("PID not same\n");
        V(sem);
        errno = EBADF;    // Caller is not the owner of this socket
        return -1;
    }
    // Destination must match what was registered in m_bind()
    // (MTP is connection-oriented in the sense that peer is fixed at bind time)
    if ((SM[m_sockfd].addr.sin_port != ((struct sockaddr_in *)dest_addr)->sin_port) ||
        (SM[m_sockfd].addr.sin_addr.s_addr != ((struct sockaddr_in *)dest_addr)->sin_addr.s_addr)) {
        printf("Port and IP are not same\n");
        V(sem);
        errno = ENOTCONN;
        return -1;
    }

    // Check if send buffer is full
    if (SM[m_sockfd].swnd.start_index ==
        (SM[m_sockfd].swnd.end_index + 1) % SEND_BUFFER_SIZE) {
        perror("Buffer is full");
        V(sem);
        errno = ENOBUFS;    // No buffer space: caller should sleep(T) and retry
        return -1;
    }

    // First message ever? Initialize start_index from sentinel -1 to 0 
    if (SM[m_sockfd].swnd.start_index == -1) {
        SM[m_sockfd].swnd.start_index = 0;
    }

    // Advance end_index and write message into circular buffer
    SM[m_sockfd].swnd.end_index =
        (SM[m_sockfd].swnd.end_index + 1) % SEND_BUFFER_SIZE;

    printf("Length: %zu\n", length);
    printf("End index: %d\n", SM[m_sockfd].swnd.end_index);
    printf("Storing message in send buffer(m_sendto): \n");

    for (int i = 0; i < length; i++) {
        SM[m_sockfd].send_buffer[SM[m_sockfd].swnd.end_index][i] =
            *((char *)(message + i));
    }

    // Mark this slot as containing a valid message
    SM[m_sockfd].swnd.validmssg[SM[m_sockfd].swnd.end_index] = 1;
    SM[m_sockfd].swnd.length[SM[m_sockfd].swnd.end_index] = length;

    V(sem);       // release lock — daemon's S thread can now see and send this
    shmdt(SM);
    return 0;     // 0 = success (message accepted into buffer)
}

/**
 * @brief Retrieves a message from the shared memory receive buffer.
 *
 * Reads payload data that the daemon's Receiver thread has already fetched
 * from the network and placed into the circular receive buffer.
 *
 * @param m_sockfd    MTP socket descriptor.
 * @param buffer      Pointer to the user buffer to store the payload.
 * @param length      Maximum length to read.
 * @param flags       Flags (unused in this implementation).
 * @param address     Optional pointer to store sender address (unused here).
 * @param address_len Optional pointer to store address length (unused here).
 * @return Number of bytes copied on success, or -1 on error (with errno set).
 *         If no message is ready, errno is set to ENOMSG and the caller should retry.
 */

ssize_t m_recvfrom(int m_sockfd, void *restrict buffer, size_t length, int flags,
                   struct sockaddr *restrict address, socklen_t *restrict address_len) {

    key_t key = KEY;
    int shmid = shmget(key, N * sizeof(struct shared_memory), 0777);
    struct shared_memory *SM = (struct shared_memory *)shmat(shmid, NULL, 0);

    key_t sem_key = SEM_SM_KEY;
    int sem = semget(sem_key, 1, 0777);
    struct sembuf pop, vop;
    pop.sem_num = vop.sem_num = 0;
    pop.sem_flg = vop.sem_flg = 0;
    pop.sem_op = -1;
    vop.sem_op = 1;

    // Small sleep so the daemon has time to fill buffer before we check
    // (avoids busy-waiting spin that starves the daemon thread)
    usleep(10000);
    P(sem);

    if (SM == NULL) {
        printf("Init process not called \n");
        V(sem);
        errno = ENOMEM;
        return -1;
    }
    if (SM[m_sockfd].free == 1) {
        V(sem);
        errno = EBADF;
        return -1;
    }

    // start_index = next slot user reads from (circular)
    int i = SM[m_sockfd].rwnd.start_index;
    printf("Start index: %d\n", i);

    if (SM[m_sockfd].rwnd.recv_msg[i] == 0) {
        // Nothing at start_index yet — daemon hasn't delivered a message
        V(sem);
        errno = ENOMSG;   // "No message of desired type"
        return -1;
    }

    printf("Msg size: %d\n", SM[m_sockfd].rwnd.msg_size[i]);

    // Copy payload from recv_buffer to user's buffer
    for (int j = 0; j < SM[m_sockfd].rwnd.msg_size[i]; j++) {
        *((char *)(buffer + j)) = SM[m_sockfd].recv_buffer[i][j];
    }
    int res = SM[m_sockfd].rwnd.msg_size[i];

    // Free this slot so the daemon can reuse it 
    SM[m_sockfd].rwnd.recv_msg[i] = 0;
    SM[m_sockfd].rwnd.msg_size[i] = 0;

    // Advance circular read pointer
    SM[m_sockfd].rwnd.start_index =
        (SM[m_sockfd].rwnd.start_index + 1) % RECV_BUFFER_SIZE;
    // Advance sequence number (wraps 1..MAX_SEQ_NUM)
    SM[m_sockfd].rwnd.start_seq_num =
        (SM[m_sockfd].rwnd.start_seq_num) % MAX_SEQ_NUM + 1;

    V(sem);
    return res;   // number of bytes delivered to user
}

/**
 * @brief Closes the MTP socket and releases the shared memory slot.
 *
 * Refuses to close (returns -1 and sets errno to EBUSY) if there are unacknowledged
 * messages still in the send buffer, preventing silent data loss. The caller must
 * retry closing until all messages are confirmed delivered.
 *
 * @param m_sockfd MTP socket descriptor.
 * @return 0 on success, or -1 on error (with errno set).
 */
int m_close(int m_sockfd) {
    int res = 0;   // default: success
    key_t key = KEY;
    int shmid = shmget(key, N * sizeof(struct shared_memory), 0777);
    struct shared_memory *SM = (struct shared_memory *)shmat(shmid, NULL, 0);

    if (SM[m_sockfd].free == 0) {
        // Check if any send buffer slot still holds a valid (unACKed) message
        int unacked = 0;
        for (int j = 0; j < SEND_BUFFER_SIZE; j++) {
            if (SM[m_sockfd].swnd.validmssg[j] == 1) {
                unacked = 1;
                break;
            }
        }
        if (unacked == 1) {
            errno = EBUSY;   // "Device or resource busy" — data still in flight
            shmdt(SM);
            return -1;
        }
        SM[m_sockfd].free = 1;   // Release this slot; daemon's G thread or next m_socket() can reuse it
    } else {
        errno = EBADF;   // Socket was already closed / was never opened
        res = -1;
    }
    shmdt(SM);
    return res;
}

/**
 * @brief Simulates random network packet loss.
 *
 * Used by the Receiver thread to test protocol reliability.
 *
 * @param probability Drop probability in the range [0.0, 1.0].
 * @return 1 if the packet should be dropped, 0 otherwise.
 */
int dropMessage(float probability) {
    srand((unsigned int)time(NULL));
    float r = (float)rand() / (float)(RAND_MAX);
    return (r < probability ? 1 : 0);
}
