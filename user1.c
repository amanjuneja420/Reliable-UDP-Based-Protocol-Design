/**
 * @file user1.c
 * @brief Sender application demonstrating the MTP (Reliable UDP) library.
 *
 * This application creates an MTP socket, binds it to a local port (8181),
 * sets the peer port (10000), reads data from 'send.txt', and sends it
 * chunk by chunk using the MTP API.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include "msocket.h"
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

int main() {
    /* Create an MTP socket (returns shared memory index, not a real fd) */
    int sockfd = m_socket(AF_INET, SOCK_MTP, 0);
    if (sockfd == -1) {
        printf("Error in creating socket\n");
        exit(1);
    }
    printf("Socket created successfully\n");

    /* Set up local source address (Port: 8181) */
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(8181);
    servaddr.sin_addr.s_addr = inet_addr("127.0.0.1");

    printf("Address: %s\n", inet_ntoa(servaddr.sin_addr));
    printf("Port: %d\n", ntohs(servaddr.sin_port));

    /* Set up peer destination address (Port: 10000) */
    struct sockaddr_in destaddr;
    memset(&destaddr, 0, sizeof(destaddr));
    destaddr.sin_family = AF_INET;
    destaddr.sin_port = htons(10000);
    destaddr.sin_addr.s_addr = inet_addr("127.0.0.1");

    /*
     * Bind socket: registers local port with the daemon and stores the peer
     * destination address for the Sender thread to use.
     */
    if (m_bind(sockfd,
               (struct sockaddr *)&servaddr, sizeof(servaddr),
               (struct sockaddr *)&destaddr, sizeof(destaddr)) == -1) {
        perror("Error in binding");
        exit(1);
    }

    /* Open the file to transmit */
    int fd = open("send.txt", O_RDONLY);
    if (fd == -1) {
        printf("Error in opening file\n");
        exit(1);
    }

    char buffer[1000];
    int n;
    int message_count = 0;

    /* Read and send loop: process up to 1000 bytes at a time */
    while ((n = read(fd, buffer, 1000)) > 0) {

        int res;
        // Retry loop: if buffer full, wait and retry the SAME chunk
        while ((res = m_sendto(sockfd, buffer, n, 0,
                               (const struct sockaddr *)&destaddr,
                               sizeof(destaddr))) == -1) {
            if (errno == ENOBUFS) {
                // Send buffer is full — daemon hasn't drained it yet
                // Sleep T seconds (one retransmission period) then try again
                perror("Buffer is full. Resending the message");
                sleep(T);
                continue;
            }
            // Any other error is fatal
            perror("Error in sending");
            printf("Errno is: %d\n", errno);
            exit(1);
        }
        message_count++;
    }

    printf("Sent %d messages\n", message_count);

    /*
     * Close the socket. If unacknowledged messages remain, m_close returns EBUSY.
     * The daemon's Garbage Collector will eventually clean up the slot.
     */
    if (m_close(sockfd) == -1) {
        if (errno == EBUSY) {
            printf("Closing socket (unACKed messages remain — daemon will handle)\n");
            exit(1);
        }
        perror("Error in closing socket\n");
        exit(1);
    }

    return 0;
}
