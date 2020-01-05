// TCP Server Foundation Framework written in C
//
// Author: Stew Forster (stew675@gmail.com)
//

#define _GNU_SOURCE
#define __USE_GNU
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <sys/eventfd.h>
#include <sys/queue.h>
#include <sys/sysinfo.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <errno.h>
#include <poll.h>
#include <ck_pr.h>
#include <signal.h>
#include <execinfo.h>
#include <sched.h>

#ifndef __TASK_LIB_H__
#define __TASK_LIB_H__

#define	TASK_FLAGS_AFFINITY_DISABLE	0x00000001
#define	TASK_FLAGS_AFFINITY_CPU		0x00000002
#define	TASK_FLAGS_AFFINITY_NET		0x00000004
#define	TASK_FLAGS_AFFINITY_FORCE	0x00000008

// Combo flag setting from the above
#define	TASK_FLAGS_AFFINITY_FORCE_CPU	0x0000000A
#define	TASK_FLAGS_AFFINITY_FORCE_ALL	0x0000000E

// Some handy macros to use to define timeouts
#define	TASK_TIMEOUT_NEVER		        (int64_t)(-1)
#define	TASK_TIMEOUT_NONE		        (int64_t)(-1)
#define	TASK_TIMEOUT_ASAP		         (int64_t)(0)
#define	TASK_TIMEOUT_NOW		         (int64_t)(0)
#define	TASK_TIMEOUT_ONE_SEC		   (int64_t)(1000000)
#define	TASK_TIMEOUT_ONE_MINUTE		  (int64_t)(60000000)
#define	TASK_TIMEOUT_ONE_HOUR		(int64_t)(3600000000)
#define	TASK_TIMEOUT_ONE_DAY	       (int64_t)(86400000000)
#define	TASK_TIMEOUT_ONE_YEAR	    (int64_t)(31622400000000)

//-------------------------------------------------------------------------------------------
// Task Library Timeout API
//-------------------------------------------------------------------------------------------

// Retrieves the current system microsecond time 
int64_t TASK_get_us_time(int64_t *time_us);

// Destroys a timeout task's state, however if the task is also registered against a socket,
// then this call just behaves identically to TASK_timeout_cancel()
int TASK_timeout_destroy(int64_t tfd);

// Cancels the timeout callback against the given tfd
int TASK_timeout_cancel(int64_t tfd);

// Sets the timeout on the given tfd to occur in us_from_now micro-seconds from now
// Returns error if tfd doesn't exist
// If the tfd already has a timeout set, then its value will just be updated
// When the timeout fires, it is automatically cancelled and must be re-armed
int TASK_timeout_set(int64_t tfd, int64_t us_from_now, void *user_data,
		    void (*timeout_cb)(int64_t tfd, int64_t lateness_us, void *user_data));

// Creates an arbitrary timeout task that exists independently of any socket
// When the timeout fires, it is automatically cancelled and must be re-armed
int64_t TASK_timeout_create(int32_t ti, int64_t expires_in_us, void *user_data,
		           void (*timeout_cb)(int64_t tfd, int64_t lateness_us, void *user_data));

//-------------------------------------------------------------------------------------------
// Task Library IO API
//-------------------------------------------------------------------------------------------

// Will write the entire contents of the supplied buffers to the given tfd, or expire trying
ssize_t TASK_socket_writev(int64_t tfd, const struct iovec *iov, int iovcnt, int64_t expires_in_us, void *user_data,
			  void (*wrv_cb)(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result, void *user_data));

// Will write the entire contents of the supplied buffer to the given tfd, or expire trying
ssize_t TASK_socket_write(int64_t tfd, const void *buf, size_t buflen, int64_t expires_in_us, void *user_data,
			 void (*write_cb)(int64_t tfd, const void *buf, ssize_t result, void *user_data));

ssize_t TASK_socket_readv(int64_t tfd, const struct iovec *iov, int iovcnt, int64_t expires_in_us, void *user_data,
			 void (*readv_cb)(int64_t tfd, const struct iovec *iov, int iovcnt, ssize_t result, void *user_data));

ssize_t TASK_socket_read(int64_t tfd, void *buf, size_t buflen, int64_t expires_in_us, void *user_data,
			void (*read_cb)(int64_t tfd, void *buf, ssize_t result, void *user_data));

//-------------------------------------------------------------------------------------------
// Task Library Socket API
//-------------------------------------------------------------------------------------------

// Returns the direct UNIX socket that the tfd is operating on
int TASK_socket_get_fd(int64_t tfd);

// Closes the tfd control over the given WebOps FD.  If a close_cb() has been
// registered against the tfd, then that will be called just prior to closing
// If there is a timeout registered against the tfd, it will be automatically cancelled
// If called against a timeout-only task, it just calls TASK_timeout_destroy(tfd);
int TASK_close(int64_t tfd);
int TASK_socket_shutdown(int64_t tfd, int how);

// Listen for and accept new connections on a given addr
int TASK_socket_listen(int64_t tfd, void *user_data, void (*accept_cb)(int64_t tfd, void *user_data));

// Connect to given destination address. If src_addr is NULL, it will just use the default interface IP and choose any local source port
int TASK_socket_connect(int64_t tfd, struct sockaddr *addr, socklen_t addrlen, int64_t expires_in_us,
		       void *user_data, void (*connect_cb)(int64_t tfd, int result, void *user_data));

int TASK_socket_bind(int64_t tfd, struct sockaddr *addr, socklen_t addrlen);

int TASK_socket_set_close_cb(int64_t tfd, void *user_data, void (*close_cb)(int64_t tfd, void *data));

// Registers an external user supplied file-descriptor with the given task system instance
// Returns an abstract socket descriptor that now owns control of the fd
// close_cb(intptr_t tfd, void user_data), if set, will be called when the task system closes the socket
int64_t TASK_socket_register(int32_t ti, int sock, void *user_data,
			    void (*close_cb)(int64_t tfd, void *user_data));

// Creates a new socket within the task system instance.  Returns an abstract descriptor to the socket
// close_cb(intptr_t tfd, void user_data), if set, will be called when the task system closes the socket
int64_t TASK_socket_create(int32_t ti, int domain, int type, int protocol, void *user_data,
			  void (*close_cb)(int64_t tfd, void *user_data));

//-------------------------------------------------------------------------------------------
// Task Library Blocking Work API
//-------------------------------------------------------------------------------------------

// Spawns a new worker thread on the given task system instance, and calls the supplied via
// work_func(user_data)  If the instance is single-threaded, then this call blocks until the
// call is completed. When the work_func() completes, work_cb(user_data) is called if it is non-NULL
int TASK_do_blocking_work(int32_t ti, void *work_data, void (*work_func)(void *work_data), void *work_cb_data,
			      void (*work_cb_func)(int32_t ti, void *work_cb_data));

//-------------------------------------------------------------------------------------------
// Task Debugging API
//-------------------------------------------------------------------------------------------

// Dumps detailed information about the task to stderr
int TASK_debug_task(int64_t tfd);

//-------------------------------------------------------------------------------------------
// Task Library Instances API
//-------------------------------------------------------------------------------------------

// Destroys the given task instance without prejudice
int TASK_instance_destroy(int32_t ti);

// Waits for a specific instance to completely terminate
int TASK_instance_wait(int32_t ti);

// Instructs an active task instance to shutdown
int TASK_instance_shutdown(int32_t ti, void *user_data, void (*shutdown_cb)(intptr_t ti, void *data));

// Starts a task instance.  If the instance is single threaded, this function will block
// until the instance shuts down via a call to TASK_instance_shutdown()
// If the instance is multi-threaded, this function will return control to the user
int TASK_instance_start(int32_t ti);

// Creates a task instance
int32_t TASK_instance_create(int num_workers_io, int max_workers_blocking, uint32_t max_tasks, int tcp_sndbuf_size, uint64_t flags);

#endif	// __TASK_LIB_H__
