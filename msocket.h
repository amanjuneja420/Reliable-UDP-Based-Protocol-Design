/**
 * @file msocket.h
 * @brief Reliable UDP (RUDP/MTP) Protocol Definitions
 *
 * Defines constants, IPC keys, shared memory structures, and the public API
 * for the Message Transmission Protocol (MTP), which provides reliable data
 * transfer over UDP.
 */

#ifndef MSOCKET_H
#define MSOCKET_H

#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

#define T 5           // Timeout in seconds for retransmission
#define p 0.05        // Probability of dropping a message (5%)
#define N 25          // Max number of MTP sockets supported
#define SOCK_MTP SOCK_DGRAM  // MTP is built on UDP (DGRAM = datagram)

/* IPC keys for shared memory and semaphore objects (using /tmp and distinct chars) */
#define KEY          ftok("/tmp", 'a')  // Key for main shared memory
#define SEM1_KEY     ftok("/tmp", 'b')  // Key for semaphore 1
#define SEM2_KEY     ftok("/tmp", 'c')  // Key for semaphore 2
#define SOCK_INFO_KEY ftok("/tmp", 'd') // Key for socket-info shared memory
#define SEM_SM_KEY   ftok("/tmp", 'e')  // Key for shared-memory mutex

/* Semaphore P (lock) and V (unlock) macros relying on local 'pop' and 'vop' sembuf structs */
#define P(s) semop(s, &pop, 1)
#define V(s) semop(s, &vop, 1)

#define SEND_BUFFER_SIZE 10   // Circular send buffer can hold 10 messages
#define RECV_BUFFER_SIZE 5    // Circular receive buffer can hold 5 messages
#define MAX_WINDOW_SIZE  10   // Sender can send at most 10 unACKed messages
#define MAX_SEQ_NUM      15   // Sequence numbers go from 1 to 15, then wrap

/**
 * @brief Handshake mailbox for IPC between user processes and the daemon.
 * Used during m_socket() and m_bind() to request real UDP socket operations.
 */
struct SOCKINFO {
    int sock_id;           // The real UDP socket file descriptor
    struct sockaddr_in addr; // The address to bind to
    int error_no;          // Error code if something went wrong
};

struct swnd {
    int send_window_size;           // Current number of slots we can send
    time_t unack_time[SEND_BUFFER_SIZE]; // When each message was last sent
    int length[SEND_BUFFER_SIZE];   // Size of each message in the buffer
    int validmssg[SEND_BUFFER_SIZE];// 1=valid message here, 0=empty slot
    int rem_buff_space;  // Remaining free slots in the send buffer (tracked but not used as a send guard;
                         // send-full check is done by comparing start_index / end_index in m_sendto)
    int start_index;                // Oldest unACKed message position
    int last_sent_index;            // Last message we've sent (but maybe unACKed)
    int end_index;                  // Where the next new message will go
    int start_index_ack_no;         // Sequence number at start_index
    int last_sent_ack_no;           // Sequence number of last sent message
};

struct rwnd {
    int receive_window_size;         // How many more msgs we can accept
    int last_inorder_msg;            // Buffer index of last in-order message
    int last_inorder_msg_seq_num;    // Sequence num of last in-order message
    int recv_msg[RECV_BUFFER_SIZE];  // 1=this slot has a message, 0=empty
    int msg_size[RECV_BUFFER_SIZE];  // Size of each received message
    int start_index;                 // Next slot user will read from
    int start_seq_num;               // Sequence number at start_index
    int nospace;                     // Flag: buffer is full, need to pause
};

/**
 * @brief Primary shared memory structure holding state for an active MTP socket.
 * The daemon maintains an array of N such structures.
 */
struct shared_memory {
    int free;              // 1=this slot is available, 0=in use
    int pid;               // PID of the process using this socket
    int sockfd;            // The underlying UDP socket fd
    struct sockaddr_in addr; // Destination address (peer)
    char send_buffer[SEND_BUFFER_SIZE][1000]; // The actual message payloads
    char recv_buffer[RECV_BUFFER_SIZE][1000]; // Received message payloads
    struct swnd swnd;      // Send window state
    struct rwnd rwnd;      // Receive window state
};

/* Public API Interface */
int m_socket(int domain, int type, int protocol);
int m_bind(int m_sockfd, const struct sockaddr *src_addr, socklen_t src_addrlen, const struct sockaddr *dest_addr, socklen_t dest_addrlen);
ssize_t m_sendto(int m_sockfd, const void *message, size_t length, int flags, const struct sockaddr *dest_addr, socklen_t dest_addrlen);
ssize_t m_recvfrom(int m_sockfd, void *restrict buffer, size_t length, int flags, struct sockaddr *restrict address, socklen_t *restrict address_len);
int m_close(int m_sockfd);

/**
 * @brief Helper to simulate random network packet loss.
 * @param probability Drop probability in range [0.0, 1.0].
 * @return 1 if packet should be dropped, 0 otherwise.
 */
int dropMessage(float probability);

#endif



