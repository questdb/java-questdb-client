/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

#include <jni.h>
#include <sys/socket.h>
#include <sys/fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/errno.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <poll.h>
#include <time.h>
#include "glibc_compat.h"
#include "net.h"
#include <netdb.h>
#include "sysutil.h"
#ifndef __APPLE__
#include <sys/un.h>
#endif

jint handleEintrInConnect(jint fd, int result);

int set_int_sockopt(int fd, int level, int opt, int value) {
    return setsockopt(fd, level, opt, &value, sizeof(value));
}

JNIEXPORT jint JNICALL Java_io_questdb_client_network_Net_setKeepAlive0
        (JNIEnv *e, jclass cl, jint fd, jint idle_sec) {
    if (set_int_sockopt(fd, SOL_SOCKET, SO_KEEPALIVE, 1) < 0) {
        return -1;
    }
#if defined(__linux__) || defined(__FreeBSD__)
    if (set_int_sockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, idle_sec) < 0) {
        return -1;
    }
    if (set_int_sockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, idle_sec) < 0) {
        return -1;
    }
#endif
#ifdef __APPLE__
    if (set_int_sockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE, idle_sec) < 0) {
        return -1;
    }
#endif
    return fd;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_network_Net_socketTcp
        (JNIEnv *e, jclass cl, jboolean blocking) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd > 0 && !blocking) {
        if (fcntl(fd, F_SETFL, O_NONBLOCK) < 0) {
            close(fd);
            return -1;
        }

        int oni = 1;
        if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *) &oni, sizeof(oni)) < 0) {
            close(fd);
            return -1;
        }
    }
    return fd;
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_network_Net_sockaddr0
        (JNIEnv *e, jclass cl, jint address, jint port) {
    struct sockaddr_in *addr = calloc(1, sizeof(struct sockaddr_in));
    addr->sin_family = AF_INET;
    addr->sin_addr.s_addr = htonl((uint32_t) address);
    addr->sin_port = htons((uint16_t) port);
    return (jlong) addr;
}

JNIEXPORT void JNICALL Java_io_questdb_client_network_Net_freeSockAddr0
        (JNIEnv *e, jclass cl, jlong address) {
    if (address != 0) {
        free((void *) address);
    }
}

JNIEXPORT jboolean JNICALL Java_io_questdb_client_network_Net_join
        (JNIEnv *e, jclass cl, jint fd, jint bindAddress, jint groupAddress) {
    struct ip_mreq mreq;
    mreq.imr_interface.s_addr = htonl((uint32_t) bindAddress);
    mreq.imr_multiaddr.s_addr = htonl((uint32_t) groupAddress);
    return (jboolean) (setsockopt((int) fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0 ? JNI_FALSE
                                                                                                    : JNI_TRUE);
}

JNIEXPORT jint JNICALL Java_io_questdb_client_network_Net_send
        (JNIEnv *e, jclass cl, jint fd, jlong ptr, jint len) {
    ssize_t n;
    RESTARTABLE(send((int) fd, (const void *) ptr, (size_t) len, 0), n);
    if (n > -1) {
        return n;
    }

    if (errno == EWOULDBLOCK) {
        return com_questdb_network_Net_ERETRY;
    }

    return com_questdb_network_Net_EOTHERDISCONNECT;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_network_Net_recv
        (JNIEnv *e, jclass cl, jint fd, jlong ptr, jint len) {
    ssize_t n;
    RESTARTABLE(recv((int) fd, (void *) ptr, (size_t) len, 0), n);
    if (n > 0) {
        return n;
    }

    if (n == 0) {
        return com_questdb_network_Net_EOTHERDISCONNECT;
    }

    if (errno == EWOULDBLOCK) {
        return com_questdb_network_Net_ERETRY;
    }

    return com_questdb_network_Net_EOTHERDISCONNECT;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_network_Net_peek
        (JNIEnv *e, jclass cl, jint fd, jlong ptr, jint len) {
    ssize_t n;
    RESTARTABLE(recv((int) fd, (void *) ptr, (size_t) len, MSG_PEEK), n);
    if (n > 0) {
        return n;
    }

    if (n == 0) {
        return com_questdb_network_Net_EOTHERDISCONNECT;
    }

    if (errno == EWOULDBLOCK) {
        return com_questdb_network_Net_ERETRY;
    }

    return com_questdb_network_Net_EOTHERDISCONNECT;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_network_Net_configureNonBlocking
        (JNIEnv *e, jclass cl, jint fd) {
    int flags;

    if ((flags = fcntl((int) fd, F_GETFL, 0)) < 0) {
        return flags;
    }

    if ((flags = fcntl((int) fd, F_SETFL, flags | O_NONBLOCK)) < 0) {
        return flags;
    }

    return 0;
}

JNIEXPORT jint handleEintrInConnect(jint fd, int result) {
    if (result == -1 && errno == EINTR) {
        // Connection was interrupted but continues in background
        // Wait for it to complete using select()
        fd_set writefds, exceptfds;
        struct timeval timeout;

        FD_ZERO(&writefds);
        FD_ZERO(&exceptfds);
        FD_SET(fd, &writefds);
        FD_SET(fd, &exceptfds);

        // Set a reasonable timeout (e.g., 30 seconds)
        timeout.tv_sec = 30;
        timeout.tv_usec = 0;

        int select_result = select(fd + 1, NULL, &writefds, &exceptfds, &timeout);

        if (select_result > 0) {
            if (FD_ISSET(fd, &exceptfds)) {
                // Exception occurred
                int error = 0;
                socklen_t len = sizeof(error);
                if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) == 0 && error != 0) {
                    errno = error;
                }
                return -1;
            } else if (FD_ISSET(fd, &writefds)) {
                // Socket is writable, check for connection error
                int error = 0;
                socklen_t len = sizeof(error);
                if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) == 0) {
                    if (error == 0) {
                        return 0; // Success
                    } else {
                        errno = error;
                        return -1;
                    }
                }
                return -1;
            }
        } else if (select_result == 0) {
            // Timeout
            errno = ETIMEDOUT;
            return -1;
        } else {
            // select() failed
            return -1;
        }
    }

    return result;
}

jint JNICALL Java_io_questdb_client_network_Net_connect
        (JNIEnv *e, jclass cl, jint fd, jlong sockAddr) {
    int result;

    struct sockaddr *addr = (struct sockaddr *) sockAddr;
    socklen_t addrlen;

    switch (addr->sa_family) {
        case AF_INET:
            addrlen = sizeof(struct sockaddr_in);
            break;
        case AF_INET6:
            addrlen = sizeof(struct sockaddr_in6);
            break;
#ifndef __APPLE__
            case AF_UNIX:
                addrlen = sizeof(struct sockaddr_un);
                break;
#endif
        default:
            return -2;
    }

    result = connect((int) fd, (const struct sockaddr *) sockAddr, addrlen);
    return handleEintrInConnect(fd, result);
}

JNIEXPORT jint JNICALL Java_io_questdb_client_network_Net_setSndBuf
        (JNIEnv *e, jclass cl, jint fd, jint size) {
    return set_int_sockopt((int) fd, SOL_SOCKET, SO_SNDBUF, size);
}

int get_int_sockopt(int fd, int level, int opt) {
    int value = 0;
    socklen_t len = sizeof(value);
    int result = getsockopt(fd, level, opt, &value, &len);
    if (result == 0) {
        return value;
    }
    return -1;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_network_Net_getSndBuf
        (JNIEnv *e, jclass cl, jint fd) {
    return get_int_sockopt((int) fd, SOL_SOCKET, SO_SNDBUF);
}

JNIEXPORT jint JNICALL Java_io_questdb_client_network_Net_setTcpNoDelay
        (JNIEnv *e, jclass cl, jint fd, jboolean noDelay) {
    return set_int_sockopt((int) fd, IPPROTO_TCP, TCP_NODELAY, noDelay);
}

JNIEXPORT jint JNICALL Java_io_questdb_client_network_Net_getEwouldblock
        (JNIEnv *e, jclass cl) {
    return EWOULDBLOCK;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_network_Net_connectAddrInfo
        (JNIEnv *e, jclass cl, jint fd, jlong lpAddrInfo) {
    struct addrinfo *addr = (struct addrinfo *) lpAddrInfo;
    int result;

    result = connect((int) fd, addr->ai_addr, (int) addr->ai_addrlen);
    return handleEintrInConnect(fd, result);
}

// Waits up to timeout_millis for an in-progress non-blocking connect on fd to
// finish. Returns 0 on success, -1 on connection failure (errno set so the
// caller can read it via Os.errno()), or com_questdb_network_Net_ECONNTIMEOUT
// on timeout.
static jint awaitConnectComplete(int fd, jint timeout_millis) {
    // Fix a single absolute deadline up front. Recomputing the remaining budget
    // against a moving baseline on each EINTR (reset start = now, then subtract
    // whole milliseconds) lets a high-frequency signal storm extend the timeout:
    // under sub-millisecond interrupts every interval truncates to 0 ms, the
    // budget never decrements, and poll is re-armed with the full budget each
    // time. A fixed deadline is immune to interrupt frequency -- the remaining
    // time can only ever decrease.
    struct timespec deadline;
    clock_gettime(CLOCK_MONOTONIC, &deadline);
    long budget_millis = timeout_millis > 0 ? timeout_millis : 0;
    deadline.tv_sec += budget_millis / 1000L;
    deadline.tv_nsec += (budget_millis % 1000L) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec += 1;
        deadline.tv_nsec -= 1000000000L;
    }

    for (;;) {
        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        // Remaining time until the deadline, truncated to whole milliseconds for
        // poll(). Truncation only ever under-shoots by < 1 ms (it never extends
        // the wait), which keeps the timeout a strict upper bound.
        long remaining_millis = (deadline.tv_sec - now.tv_sec) * 1000L
                                + (deadline.tv_nsec - now.tv_nsec) / 1000000L;
        if (remaining_millis <= 0) {
            errno = ETIMEDOUT;
            return com_questdb_network_Net_ECONNTIMEOUT;
        }

        struct pollfd pfd;
        pfd.fd = fd;
        pfd.events = POLLOUT;
        pfd.revents = 0;

        int rc = poll(&pfd, 1, (int) remaining_millis);
        if (rc > 0) {
            // The connect attempt has finished one way or another; the only
            // authoritative result is SO_ERROR (POLLOUT alone does not mean
            // success -- a refused connection is also reported as writable).
            int so_error = 0;
            socklen_t len = sizeof(so_error);
            if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &len) < 0) {
                return -1;
            }
            if (so_error != 0) {
                errno = so_error;
                return -1;
            }
            return 0;
        }
        if (rc == 0) {
            errno = ETIMEDOUT;
            return com_questdb_network_Net_ECONNTIMEOUT;
        }
        if (errno != EINTR) {
            return -1;
        }
        // Interrupted by a signal: loop and recompute the remaining time against
        // the fixed deadline. EINTR storms cannot extend the timeout.
    }
}

JNIEXPORT jint JNICALL Java_io_questdb_client_network_Net_connectAddrInfoTimeout
        (JNIEnv *e, jclass cl, jint fd, jlong lpAddrInfo, jint timeoutMillis) {
    struct addrinfo *addr = (struct addrinfo *) lpAddrInfo;

    // Switch to non-blocking BEFORE connect so connect() returns immediately
    // with EINPROGRESS instead of blocking on the OS connect timeout. The
    // socket is left non-blocking on success, matching the post-connect
    // configureNonBlocking() the callers already perform.
    int flags = fcntl((int) fd, F_GETFL, 0);
    if (flags < 0) {
        return -1;
    }
    if (fcntl((int) fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        return -1;
    }

    int result = connect((int) fd, addr->ai_addr, (int) addr->ai_addrlen);
    if (result == 0) {
        return 0; // connected immediately (e.g. loopback)
    }
    if (errno == EINPROGRESS || errno == EINTR || errno == EWOULDBLOCK) {
        return awaitConnectComplete((int) fd, timeoutMillis);
    }
    return -1; // immediate failure, errno set
}

JNIEXPORT void JNICALL Java_io_questdb_client_network_Net_freeAddrInfo0
        (JNIEnv *e, jclass cl, jlong address) {
    if (address != 0) {
        freeaddrinfo((void *) address);
    }
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_network_Net_getAddrInfo0
        (JNIEnv *e, jclass cl, jlong host, jint port) {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_NUMERICSERV;
    struct addrinfo *addr = NULL;

    char _port[13];
    snprintf(_port, sizeof(_port) / sizeof(_port[0]), "%d", port);
    int gai_err_code = getaddrinfo((const char *) host, (const char *) &_port, &hints, &addr);

    if (gai_err_code == 0) {
        return (jlong) addr;
    }
    return -1;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_network_Net_socketUdp
        (JNIEnv *e, jclass cl) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);

    if (fd > 0 && fcntl(fd, F_SETFL, O_NONBLOCK) < 0) {
        close(fd);
        return -1;
    }

    return fd;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_network_Net_setMulticastInterface
        (JNIEnv *e, jclass cl, jint fd, jint ipv4address) {
    struct in_addr address;
    address.s_addr = (in_addr_t) htonl((__uint32_t) ipv4address);
    return setsockopt((int) fd, IPPROTO_IP, IP_MULTICAST_IF, &address, sizeof(address));
}

JNIEXPORT jint JNICALL Java_io_questdb_client_network_Net_setMulticastTtl
        (JNIEnv *e, jclass cl, jint fd, jint ttl) {
    u_char lTTL = ttl;
    int result = setsockopt(fd, IPPROTO_IP, IP_MULTICAST_TTL, (char *) &lTTL, sizeof(lTTL));
    if (result == 0) {
        return result;
    }
    return -1;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_network_Net_sendTo
        (JNIEnv *e, jclass cl, jint fd, jlong ptr, jint len, jlong sockaddr) {
    return (jint) sendto((int) fd, (const void *) ptr, (size_t) len, 0, (const struct sockaddr *) sockaddr,
                         sizeof(struct sockaddr_in));
}

JNIEXPORT jint JNICALL Java_io_questdb_client_network_Net_sendToScatter
        (JNIEnv *e, jclass cl, jint fd, jlong segmentsPtr, jint segmentCount, jlong sockaddr) {
    if (segmentCount <= 0) {
        return 0;
    }

    // Stack-allocate for the common case (small segment count) to avoid
    // a heap allocation on every UDP send.
    #define STACK_IOV_COUNT 16
    struct iovec stack_iov[STACK_IOV_COUNT];
    struct iovec *iov;

    if (segmentCount <= STACK_IOV_COUNT) {
        iov = stack_iov;
    } else {
        iov = calloc((size_t) segmentCount, sizeof(struct iovec));
        if (iov == NULL) {
            errno = ENOMEM;
            return -1;
        }
    }

    const char *segment = (const char *) segmentsPtr;
    for (int i = 0; i < segmentCount; i++) {
        iov[i].iov_base = (void *) (uintptr_t) (*(const jlong *) segment);
        iov[i].iov_len = (size_t) (*(const jlong *) (segment + 8));
        segment += 16;
    }

    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_name = (void *) sockaddr;
    msg.msg_namelen = sizeof(struct sockaddr_in);
    msg.msg_iov = iov;
    msg.msg_iovlen = (size_t) segmentCount;

    ssize_t sent;
    RESTARTABLE(sendmsg((int) fd, &msg, 0), sent);
    if (iov != stack_iov) {
        free(iov);
    }
    return sent < 0 ? -1 : (jint) sent;
    #undef STACK_IOV_COUNT
}
