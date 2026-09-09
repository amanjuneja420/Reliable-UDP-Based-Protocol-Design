/**
 * @file user2.c
 * @brief Receiver application demonstrating the MTP (Reliable UDP) library.
 *
 * This application creates an MTP socket, binds it to a local port (10000),
 * sets the peer port (8181), receives data via the MTP API, and writes it
 * to 'read.txt'.
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
    /* Create an MTP socket */
    int sockfd = m_socket(AF_INET, SOCK_MTP, 0);
    if (sockfd == -1) {
        printf("Error in creating socket\n");
        exit(1);
    }
    printf("Socket created successfully\n");

    /* Set up local source address (Port: 10000) */
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(10000);
    servaddr.sin_addr.s_addr = inet_addr("127.0.0.1");

    /* Set up peer destination address (Port: 8181) */
    struct sockaddr_in destaddr;
    memset(&destaddr, 0, sizeof(destaddr));
    destaddr.sin_family = AF_INET;
    destaddr.sin_port = htons(8181);
    destaddr.sin_addr.s_addr = inet_addr("127.0.0.1");

    /* Bind socket */
    if (m_bind(sockfd,
               (struct sockaddr *)&servaddr, sizeof(servaddr),
               (struct sockaddr *)&destaddr, sizeof(destaddr)) == -1) {
        printf("Error in binding\n");
        exit(1);
    }
    printf("Bind done\n");
    fflush(stdout);

    /* Open output file (create if it does not exist) */
    int fd = open("read.txt", O_RDWR | O_CREAT, 0777);
    if (fd == -1) {
        printf("Error in opening file\n");
        exit(1);
    }

    char buffer[1000];
    int n;

    /* Receive loop */
    while (1) {
        n = m_recvfrom(sockfd, buffer, 1000, 0, NULL, NULL);

        if (n == -1) {
            // errno == ENOMSG: no message ready yet in recv_buffer
            // Just keep retrying — daemon's R thread will deliver data soon
            perror("Error in receiving");
            continue;
        }
        if (n == 0) {
            // Sender closed the connection — we're done
            break;
        }

        // Write received bytes to the output file
        int val = write(fd, buffer, n);
        if (val == -1) {
            printf("Error in writing to file\n");
            exit(1);
        }
        printf("Writing %d bytes\n", val);
    }

    /* Close the socket */
    if (m_close(sockfd) == -1) {
        if (errno == EBUSY) {
            printf("Closing socket\n");
            exit(1);
        }
        perror("Error in closing socket\n");
        exit(1);
    }

    return 0;
}
