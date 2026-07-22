#define _GNU_SOURCE

#include "mr_porter.h"
#include "mr_wire.h"

#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define MR_PORTER_PROTOCOL_VERSION 1U
#define MR_PORTER_OP_INIT 1U
#define MR_PORTER_OP_DISPATCH 2U
#define MR_PORTER_OP_STOP 3U

#define MR_PORTER_SLOT_EMPTY 0U
#define MR_PORTER_SLOT_READY 1U
#define MR_PORTER_SLOT_DONE 2U
#define MR_PORTER_SLOT_STOP 3U

typedef struct mr_porter_slot {
    volatile uint32_t state;
    volatile uint32_t worker_ready;
    uint32_t worker_preopen_status;
    uint32_t status;
    uint32_t kind;
    uint32_t trace_flags;
    uint64_t task_id;
    uint64_t partition_id;
    uint64_t partition_count;
    uint64_t payload_len;
    uint64_t response_len;
    uint64_t submit_ns;
    uint64_t worker_begin_ns;
    uint64_t restore_ioctl_ns;
    uint64_t template_validate_ns;
    uint64_t task_ns;
    mr_task_trace_v3 trace;
    mr_cxlfork_timing restore_timing;
    mr_template_binding_result binding;
    mr_cxlfork_result restore;
} mr_porter_slot;

typedef struct mr_porter_command {
    uint32_t size;
    uint16_t version;
    uint16_t op;
    uint32_t worker_id;
    uint32_t reserved;
    uint64_t task_id;
    uint64_t dispatch_ns;
} mr_porter_command;

typedef struct mr_porter_reply {
    uint32_t size;
    uint16_t version;
    uint16_t op;
    mr_porter_result result;
} mr_porter_reply;

struct mr_porter {
    pid_t supervisor_pid;
    int control_fd;
    void *shared;
    size_t shared_len;
    uint64_t slot_bytes;
    uint32_t workers;
    bool *busy;
};

static uint64_t nsec_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * UINT64_C(1000000000) + (uint64_t)ts.tv_nsec;
}

static int write_full(int fd, const void *buffer, size_t size)
{
    const char *p = buffer;
    while (size > 0) {
        ssize_t n = write(fd, p, size);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            return errno ? errno : EPIPE;
        }
        p += n;
        size -= (size_t)n;
    }
    return 0;
}

static int read_full(int fd, void *buffer, size_t size)
{
    char *p = buffer;
    while (size > 0) {
        ssize_t n = read(fd, p, size);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            return errno ? errno : EPIPE;
        }
        p += n;
        size -= (size_t)n;
    }
    return 0;
}

static int read_packet_nowait(int fd, void *buffer, size_t size)
{
    ssize_t n;
    do {
        n = recv(fd, buffer, size, MSG_DONTWAIT);
    } while (n < 0 && errno == EINTR);
    if (n < 0) {
        return errno;
    }
    if ((size_t)n != size) {
        return EPROTO;
    }
    return 0;
}

static size_t slot_stride(uint64_t slot_bytes)
{
    return sizeof(mr_porter_slot) + (size_t)slot_bytes * 2U;
}

static mr_porter_slot *slot_at(void *base, uint64_t slot_bytes,
                               uint32_t worker_id)
{
    return (mr_porter_slot *)((char *)base +
                              slot_stride(slot_bytes) * worker_id);
}

static void *slot_payload(mr_porter_slot *slot)
{
    return (char *)slot + sizeof(*slot);
}

static void *slot_response(mr_porter_slot *slot, uint64_t slot_bytes)
{
    return (char *)slot_payload(slot) + slot_bytes;
}

static void slot_publish(mr_porter_slot *slot, uint32_t state)
{
    __sync_synchronize();
    slot->state = state;
    __sync_synchronize();
}

const char *mr_launch_mode_name(mr_launch_mode mode)
{
    switch (mode) {
    case MR_LAUNCH_PERSISTENT:
        return "persistent";
    case MR_LAUNCH_LOCALFORK:
        return "localfork";
    case MR_LAUNCH_LOCAL_PREFORK:
        return "local-prefork";
    case MR_LAUNCH_CXLFORK:
        return "cxlfork";
    default:
        return "unknown";
    }
}

int mr_launch_mode_parse(const char *text, mr_launch_mode *mode)
{
    if (strcmp(text, "persistent") == 0) {
        *mode = MR_LAUNCH_PERSISTENT;
    } else if (strcmp(text, "localfork") == 0) {
        *mode = MR_LAUNCH_LOCALFORK;
    } else if (strcmp(text, "local-prefork") == 0) {
        *mode = MR_LAUNCH_LOCAL_PREFORK;
    } else if (strcmp(text, "cxlfork") == 0) {
        *mode = MR_LAUNCH_CXLFORK;
    } else {
        return EINVAL;
    }
    return 0;
}

static void worker_run_one(const mr_porter_config *config, uint32_t worker_id,
                           mr_porter_slot *slot,
                           mr_cxlfork_worker_context *worker_context)
{
    byte_buf response = {0};
    const MR_WorkerStateV1 *worker_state = NULL;
    const mr_template_options *template = &config->template;
    mr_cxlfork_session *session = NULL;
    int err = 0;

    slot->trace.worker_entry_ns = nsec_now();
    slot->worker_begin_ns = slot->trace.worker_entry_ns;
    memset(&slot->restore, 0, sizeof(slot->restore));
    slot->restore_ioctl_ns = 0;
    slot->template_validate_ns = 0;
    memset(&slot->restore_timing, 0, sizeof(slot->restore_timing));
    memset(&slot->binding, 0, sizeof(slot->binding));
    if (config->mode == MR_LAUNCH_CXLFORK) {
        template = &config->restore.template;
        slot->trace.restore_wrapper_begin_ns = nsec_now();
        if (worker_context != NULL) {
            err = mr_cxlfork_restore_begin_preopened(
                &config->restore, worker_context, &session, &slot->restore,
                &slot->restore_timing);
        } else {
            err = mr_cxlfork_restore_begin(&config->restore, &session,
                                           &slot->restore,
                                           &slot->restore_timing);
        }
        slot->trace.restore_wrapper_end_ns = nsec_now();
        slot->restore_ioctl_ns = slot->restore_timing.wrapper_total_ns;
        slot->trace.restore_postcheck_done_ns =
            slot->trace.restore_wrapper_end_ns;
        if (slot->restore_timing.postcheck_ns <=
            slot->trace.restore_wrapper_end_ns) {
            slot->trace.resume_ioctl_end_ns =
                slot->trace.restore_wrapper_end_ns -
                slot->restore_timing.postcheck_ns;
        }
        if (slot->restore_timing.resume_ioctl_ns <=
            slot->trace.resume_ioctl_end_ns) {
            slot->trace.resume_ioctl_begin_ns =
                slot->trace.resume_ioctl_end_ns -
                slot->restore_timing.resume_ioctl_ns;
        }
    }
    if (err == 0) {
        mr_worker_role expected_role;
        if (slot->kind == MR_TASK_MAP) {
            expected_role = MR_ROLE_MAP;
        } else if (slot->kind == MR_TASK_REDUCE) {
            expected_role = MR_ROLE_REDUCE;
        } else if (slot->kind == MR_TASK_ACTIVATION_PROBE &&
                   slot->payload_len >= sizeof(mr_activation_probe_request)) {
            const mr_activation_probe_request *probe = slot_payload(slot);
            expected_role = (mr_worker_role)probe->expected_role;
        } else {
            expected_role = 0;
            err = EINVAL;
        }
        if (err == 0) {
            slot->trace.template_bind_begin_ns = nsec_now();
            err = mr_template_bind((void *)(uintptr_t)template->base,
                                   template, expected_role, &worker_state,
                                   &slot->binding);
            slot->trace.template_bind_end_ns = nsec_now();
            slot->template_validate_ns =
                slot->trace.template_bind_end_ns -
                slot->trace.template_bind_begin_ns;
            if (err == 0) {
                slot->trace.state_ready_ns =
                    slot->trace.template_bind_end_ns;
            }
        }
        if (err == 0 && config->mode == MR_LAUNCH_CXLFORK &&
            config->restore_hold_ms != 0) {
            usleep((useconds_t)config->restore_hold_ms * 1000U);
        }
    }

    if (err == 0 && slot->payload_len > config->slot_bytes) {
        err = EMSGSIZE;
    } else if (err == 0) {
        slot->trace.business_entry_ns = nsec_now();
        err = config->task_fn(config->task_context, worker_id, slot->kind,
                              slot->task_id, slot->partition_id,
                              slot->partition_count, worker_state,
                              &slot->binding, slot_payload(slot),
                              (size_t)slot->payload_len, &response,
                              &slot->trace);
        slot->trace.task_done_ns = nsec_now();
        slot->task_ns = slot->trace.task_done_ns -
                        slot->trace.business_entry_ns;
        if (err == 0 &&
            (slot->binding.flags &
             (MR_TEMPLATE_BIND_VALIDATED | MR_TEMPLATE_BIND_ROLE_MATCH |
              MR_TEMPLATE_BIND_APP_CONSUMED | MR_TEMPLATE_BIND_REQUIRED)) !=
                (MR_TEMPLATE_BIND_VALIDATED | MR_TEMPLATE_BIND_ROLE_MATCH |
                 MR_TEMPLATE_BIND_APP_CONSUMED |
                 MR_TEMPLATE_BIND_REQUIRED)) {
            err = ENODATA;
        }
    }
    if (session != NULL) {
        slot->trace.restore_collect_begin_ns = nsec_now();
        int collect_err = mr_cxlfork_restore_collect(session, &slot->restore);
        slot->trace.restore_collect_end_ns = nsec_now();
        if (err == 0 && collect_err != 0) {
            err = collect_err;
        }
        mr_cxlfork_restore_end(session);
    }
    if (err == 0 && response.len > config->slot_bytes) {
        err = EMSGSIZE;
    }
    slot->status = (uint32_t)err;
    slot->response_len = err == 0 ? response.len : 0;
    if (err == 0 && response.len > 0) {
        memcpy(slot_response(slot, config->slot_bytes), response.data,
               response.len);
    }
    byte_buf_clear(&response);
    slot->trace.slot_done_publish_ns = nsec_now();
    slot_publish(slot, MR_PORTER_SLOT_DONE);
}

static void worker_main(const mr_porter_config *config, uint32_t worker_id,
                        mr_porter_slot *slot, bool persistent, int control_fd)
{
    mr_cxlfork_worker_context *worker_context = NULL;
    mr_cxlfork_timing preopen_timing = {0};
    int startup_err = 0;

    close(control_fd);
    if (config->mode == MR_LAUNCH_CXLFORK && config->restore_preopen) {
        startup_err = mr_cxlfork_worker_context_open(
            &config->restore, &worker_context, &preopen_timing);
    } else if (config->mode != MR_LAUNCH_CXLFORK) {
        startup_err = mr_template_validate(
            (void *)(uintptr_t)config->template.base,
            &config->template, NULL);
    }
    slot->worker_preopen_status = (uint32_t)startup_err;
    __sync_synchronize();
    slot->worker_ready = 1;
    __sync_synchronize();
    if (startup_err != 0) {
        _exit(120);
    }
    if (!persistent) {
        while (slot->state != MR_PORTER_SLOT_READY &&
               slot->state != MR_PORTER_SLOT_STOP) {
            sched_yield();
        }
        if (slot->state == MR_PORTER_SLOT_STOP) {
            mr_cxlfork_worker_context_close(worker_context);
            _exit(0);
        }
        __sync_synchronize();
        slot->trace.worker_slot_observed_ns = nsec_now();
        worker_run_one(config, worker_id, slot, worker_context);
        mr_cxlfork_worker_context_close(worker_context);
        _exit(slot->status == 0 ? 0 : 1);
    }
    for (;;) {
        while (slot->state != MR_PORTER_SLOT_READY &&
               slot->state != MR_PORTER_SLOT_STOP) {
            sched_yield();
        }
        if (slot->state == MR_PORTER_SLOT_STOP) {
            mr_cxlfork_worker_context_close(worker_context);
            _exit(0);
        }
        if (slot->state == MR_PORTER_SLOT_READY) {
            __sync_synchronize();
            slot->trace.worker_slot_observed_ns = nsec_now();
            worker_run_one(config, worker_id, slot, worker_context);
        }
    }
}

static pid_t spawn_worker(const mr_porter_config *config, uint32_t worker_id,
                          mr_porter_slot *slot, bool persistent, int control_fd)
{
    slot->worker_ready = 0;
    slot->worker_preopen_status = 0;
    __sync_synchronize();
    pid_t pid = fork();
    if (pid == 0) {
        worker_main(config, worker_id, slot, persistent, control_fd);
    }
    return pid;
}

static int wait_worker_ready(pid_t pid, mr_porter_slot *slot)
{
    uint64_t deadline = nsec_now() + UINT64_C(5000000000);

    while (!slot->worker_ready) {
        int status;
        pid_t done = waitpid(pid, &status, WNOHANG);

        if (done == pid) {
            return ECHILD;
        }
        if (done < 0) {
            return errno;
        }
        if (nsec_now() >= deadline) {
            return ETIMEDOUT;
        }
        sched_yield();
    }
    __sync_synchronize();
    return (int)slot->worker_preopen_status;
}

static void stop_workers(pid_t *pids, const mr_porter_config *config,
                         void *shared)
{
    for (uint32_t i = 0; i < config->workers; i++) {
        if (pids[i] > 0) {
            slot_publish(slot_at(shared, config->slot_bytes, i),
                         MR_PORTER_SLOT_STOP);
        }
    }
    for (uint32_t i = 0; i < config->workers; i++) {
        if (pids[i] > 0) {
            (void)waitpid(pids[i], NULL, 0);
            pids[i] = -1;
        }
    }
}

static void supervisor_main(mr_porter_config config, void *shared,
                            int control_fd)
{
    pid_t *pids = calloc(config.workers, sizeof(*pids));
    pid_t *executed_pids = calloc(config.workers, sizeof(*executed_pids));
    uint64_t *begin_ns = calloc(config.workers, sizeof(*begin_ns));
    uint64_t *dispatch_ns = calloc(config.workers, sizeof(*dispatch_ns));
    uint64_t *fork_ns = calloc(config.workers, sizeof(*fork_ns));
    bool *inflight = calloc(config.workers, sizeof(*inflight));
    mr_porter_reply reply = {
        .size = sizeof(reply),
        .version = MR_PORTER_PROTOCOL_VERSION,
        .op = MR_PORTER_OP_INIT,
    };
    void *template_mapping = NULL;
    int err = 0;

    if (pids == NULL || executed_pids == NULL || begin_ns == NULL ||
        dispatch_ns == NULL ||
        fork_ns == NULL || inflight == NULL) {
        err = ENOMEM;
    } else if (config.mode != MR_LAUNCH_CXLFORK) {
        err = mr_template_map_local(&config.template, &template_mapping);
        if (err == 0) {
            err = mr_template_validate(template_mapping, &config.template,
                                       NULL);
        }
    }
    if (err == 0 && (config.mode == MR_LAUNCH_PERSISTENT ||
                     config.mode == MR_LAUNCH_LOCAL_PREFORK ||
                     config.mode == MR_LAUNCH_CXLFORK)) {
        for (uint32_t i = 0; i < config.workers; i++) {
            pids[i] = spawn_worker(&config, i,
                                   slot_at(shared, config.slot_bytes, i),
                                   config.mode == MR_LAUNCH_PERSISTENT,
                                   control_fd);
            if (pids[i] < 0) {
                err = errno;
                break;
            }
            err = wait_worker_ready(
                pids[i], slot_at(shared, config.slot_bytes, i));
            if (err != 0) {
                break;
            }
        }
    }
    if (err != 0 && pids != NULL) {
        stop_workers(pids, &config, shared);
    }
    reply.result.status = (uint32_t)err;
    if (write_full(control_fd, &reply, sizeof(reply)) != 0 || err != 0) {
        mr_template_unmap(template_mapping, config.template.bytes);
        free(inflight);
        free(fork_ns);
        free(dispatch_ns);
        free(begin_ns);
        free(executed_pids);
        free(pids);
        close(control_fd);
        _exit(err == 0 ? 121 : 1);
    }

    bool stopping = false;
    while (!stopping) {
        bool progress = false;
        mr_porter_command command;
        int command_err =
            read_packet_nowait(control_fd, &command, sizeof(command));
        if (command_err == 0) {
            progress = true;
            if (command.size != sizeof(command) ||
                command.version != MR_PORTER_PROTOCOL_VERSION) {
                break;
            }
            if (command.op == MR_PORTER_OP_STOP) {
                bool any_inflight = false;
                for (uint32_t i = 0; i < config.workers; i++) {
                    any_inflight |= inflight[i];
                }
                if (any_inflight) {
                    break;
                }
                reply.op = MR_PORTER_OP_STOP;
                memset(&reply.result, 0, sizeof(reply.result));
                (void)write_full(control_fd, &reply, sizeof(reply));
                stopping = true;
                continue;
            }
            if (command.op != MR_PORTER_OP_DISPATCH ||
                command.worker_id >= config.workers ||
                inflight[command.worker_id]) {
                break;
            }

            uint32_t worker = command.worker_id;
            mr_porter_slot *slot =
                slot_at(shared, config.slot_bytes, worker);
            begin_ns[worker] = nsec_now();
            slot->trace.supervisor_command_received_ns = begin_ns[worker];
            dispatch_ns[worker] = begin_ns[worker] - command.dispatch_ns;
            fork_ns[worker] = 0;
            executed_pids[worker] = pids[worker];
            inflight[worker] = true;
            if (config.mode == MR_LAUNCH_LOCALFORK) {
                uint64_t phase = nsec_now();
                slot->trace.fork_begin_ns = phase;
                executed_pids[worker] =
                    spawn_worker(&config, worker, slot, false, control_fd);
                slot->trace.fork_return_ns = nsec_now();
                fork_ns[worker] = slot->trace.fork_return_ns - phase;
                if (executed_pids[worker] < 0) {
                    slot->status = (uint32_t)errno;
                    slot_publish(slot, MR_PORTER_SLOT_DONE);
                } else {
                    slot->trace.slot_ready_publish_ns = nsec_now();
                    slot->submit_ns = slot->trace.slot_ready_publish_ns;
                    slot_publish(slot, MR_PORTER_SLOT_READY);
                }
            } else {
                slot->trace.slot_ready_publish_ns = nsec_now();
                slot->submit_ns = slot->trace.slot_ready_publish_ns;
                slot_publish(slot, MR_PORTER_SLOT_READY);
            }
        } else if (command_err != EAGAIN && command_err != EWOULDBLOCK) {
            break;
        }

        for (uint32_t worker = 0; worker < config.workers; worker++) {
            if (!inflight[worker]) {
                continue;
            }
            mr_porter_slot *slot =
                slot_at(shared, config.slot_bytes, worker);
            if (slot->state != MR_PORTER_SLOT_DONE) {
                continue;
            }
            __sync_synchronize();
            progress = true;
            slot->trace.supervisor_done_observed_ns = nsec_now();
            uint64_t phase;
            memset(&reply.result, 0, sizeof(reply.result));
            reply.op = MR_PORTER_OP_DISPATCH;
            reply.result.worker_id = worker;
            reply.result.task_id = slot->task_id;
            reply.result.worker_pid = executed_pids[worker];
            reply.result.dispatch_ns = dispatch_ns[worker];
            reply.result.fork_ns = fork_ns[worker];
            reply.result.wakeup_ns =
                slot->worker_begin_ns >= slot->submit_ns
                    ? slot->worker_begin_ns - slot->submit_ns
                    : 0;
            reply.result.task_ns = slot->task_ns;
            reply.result.status = slot->status;
            reply.result.restore_ioctl_ns = slot->restore_ioctl_ns;
            reply.result.template_validate_ns = slot->template_validate_ns;
            reply.result.binding = slot->binding;
            reply.result.restore = slot->restore;
            reply.result.trace_flags = slot->trace_flags;
            reply.result.trace = slot->trace;
            reply.result.restore_timing = slot->restore_timing;

            if (config.mode != MR_LAUNCH_PERSISTENT &&
                executed_pids[worker] > 0) {
                phase = nsec_now();
                reply.result.trace.reap_begin_ns = phase;
                (void)waitpid(executed_pids[worker], NULL, 0);
                reply.result.trace.reap_end_ns = nsec_now();
                reply.result.reap_ns =
                    reply.result.trace.reap_end_ns - phase;
            }
            if (config.mode == MR_LAUNCH_LOCAL_PREFORK ||
                config.mode == MR_LAUNCH_CXLFORK) {
                phase = nsec_now();
                reply.result.trace.refill_begin_ns = phase;
                pids[worker] = spawn_worker(&config, worker, slot, false,
                                            control_fd);
                if (pids[worker] < 0 && reply.result.status == 0) {
                    reply.result.status = (uint32_t)errno;
                } else if (pids[worker] > 0) {
                    int ready_err = wait_worker_ready(pids[worker], slot);
                    if (ready_err != 0 && reply.result.status == 0) {
                        reply.result.status = (uint32_t)ready_err;
                    }
                }
                reply.result.trace.refill_end_ns = nsec_now();
                reply.result.trace.pool_ready_ns =
                    reply.result.trace.refill_end_ns;
                reply.result.refill_ns =
                    reply.result.trace.refill_end_ns - phase;
            }
            reply.result.total_ns = nsec_now() - begin_ns[worker];
            inflight[worker] = false;
            if (write_full(control_fd, &reply, sizeof(reply)) != 0) {
                stopping = true;
                break;
            }
        }
        if (!progress) {
            sched_yield();
        }
    }

    if (pids != NULL) {
        stop_workers(pids, &config, shared);
    }
    mr_template_unmap(template_mapping, config.template.bytes);
    free(inflight);
    free(fork_ns);
    free(dispatch_ns);
    free(begin_ns);
    free(executed_pids);
    free(pids);
    close(control_fd);
    _exit(0);
}

int mr_porter_start(const mr_porter_config *config, mr_porter **out)
{
    int sockets[2];
    mr_porter_reply reply = {0};
    mr_porter *porter;

    if (config == NULL || out == NULL || config->workers == 0 ||
        config->slot_bytes == 0 || config->task_fn == NULL) {
        return EINVAL;
    }
    porter = calloc(1, sizeof(*porter));
    if (porter == NULL) {
        return ENOMEM;
    }
    porter->control_fd = -1;
    porter->workers = config->workers;
    porter->slot_bytes = config->slot_bytes;
    porter->busy = calloc(config->workers, sizeof(*porter->busy));
    if (porter->busy == NULL) {
        free(porter);
        return ENOMEM;
    }
    porter->shared_len = slot_stride(config->slot_bytes) * config->workers;
    porter->shared = mmap(NULL, porter->shared_len, PROT_READ | PROT_WRITE,
                          MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (porter->shared == MAP_FAILED) {
        int err = errno;
        free(porter->busy);
        free(porter);
        return err;
    }
    if (socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, sockets) != 0) {
        int err = errno;
        munmap(porter->shared, porter->shared_len);
        free(porter->busy);
        free(porter);
        return err;
    }
    porter->supervisor_pid = fork();
    if (porter->supervisor_pid < 0) {
        int err = errno;
        close(sockets[0]);
        close(sockets[1]);
        munmap(porter->shared, porter->shared_len);
        free(porter->busy);
        free(porter);
        return err;
    }
    if (porter->supervisor_pid == 0) {
        close(sockets[0]);
        supervisor_main(*config, porter->shared, sockets[1]);
    }
    close(sockets[1]);
    porter->control_fd = sockets[0];
    int err = read_full(porter->control_fd, &reply, sizeof(reply));
    if (err == 0 && (reply.size != sizeof(reply) ||
                     reply.version != MR_PORTER_PROTOCOL_VERSION ||
                     reply.op != MR_PORTER_OP_INIT)) {
        err = EPROTO;
    }
    if (err == 0) {
        err = (int)reply.result.status;
    }
    if (err != 0) {
        close(porter->control_fd);
        (void)waitpid(porter->supervisor_pid, NULL, 0);
        munmap(porter->shared, porter->shared_len);
        free(porter->busy);
        free(porter);
        return err;
    }
    *out = porter;
    return 0;
}

int mr_porter_submit(mr_porter *porter, uint32_t worker_id, uint32_t kind,
                     uint64_t task_id, uint64_t partition_id,
                     uint64_t partition_count, const void *payload,
                     size_t payload_len, uint64_t agent_request_received_ns,
                     uint64_t agent_parse_done_ns)
{
    if (porter == NULL || worker_id >= porter->workers ||
        payload_len > porter->slot_bytes) {
        return EINVAL;
    }
    mr_porter_slot *slot = slot_at(porter->shared, porter->slot_bytes, worker_id);
    if (porter->busy[worker_id] || slot->state != MR_PORTER_SLOT_EMPTY) {
        return EBUSY;
    }
    slot->status = 0;
    memset(&slot->trace, 0, sizeof(slot->trace));
    memset(&slot->restore_timing, 0, sizeof(slot->restore_timing));
    slot->trace.porter_submit_begin_ns = nsec_now();
    slot->trace.agent_request_received_ns = agent_request_received_ns;
    slot->trace.agent_parse_done_ns = agent_parse_done_ns;
    slot->kind = kind;
    slot->task_id = task_id;
    slot->partition_id = partition_id;
    slot->partition_count = partition_count;
    slot->payload_len = payload_len;
    slot->response_len = 0;
    slot->submit_ns = 0;
    slot->worker_begin_ns = 0;
    slot->restore_ioctl_ns = 0;
    slot->template_validate_ns = 0;
    slot->task_ns = 0;
    slot->trace_flags = 0;
    memset(&slot->binding, 0, sizeof(slot->binding));
    memset(&slot->restore, 0, sizeof(slot->restore));
    if (payload_len > 0) {
        memcpy(slot_payload(slot), payload, payload_len);
    }
    if (kind == MR_TASK_ACTIVATION_PROBE &&
        payload_len >= sizeof(mr_activation_probe_request)) {
        const mr_activation_probe_request *probe = slot_payload(slot);
        slot->trace_flags = probe->flags | MR_TRACE_F_ACTIVATION_PROBE;
    }
    slot->trace.payload_copy_done_ns = nsec_now();
    porter->busy[worker_id] = true;
    mr_porter_command command = {
        .size = sizeof(command),
        .version = MR_PORTER_PROTOCOL_VERSION,
        .op = MR_PORTER_OP_DISPATCH,
        .worker_id = worker_id,
        .task_id = task_id,
        .dispatch_ns = nsec_now(),
    };
    int err = write_full(porter->control_fd, &command, sizeof(command));
    if (err != 0) {
        slot_publish(slot, MR_PORTER_SLOT_EMPTY);
        porter->busy[worker_id] = false;
    }
    return err;
}

int mr_porter_collect(mr_porter *porter, uint32_t *worker_id,
                      byte_buf *response, mr_porter_result *result)
{
    if (porter == NULL) {
        return EINVAL;
    }
    mr_porter_reply reply;
    int err = read_packet_nowait(porter->control_fd, &reply, sizeof(reply));
    if (err == 0 && (reply.size != sizeof(reply) ||
                     reply.version != MR_PORTER_PROTOCOL_VERSION ||
                     reply.op != MR_PORTER_OP_DISPATCH ||
                     reply.result.worker_id >= porter->workers)) {
        err = EPROTO;
    }
    if (err != 0) {
        return err;
    }
    uint32_t completed_worker = reply.result.worker_id;
    mr_porter_slot *slot =
        slot_at(porter->shared, porter->slot_bytes, completed_worker);
    if (!porter->busy[completed_worker] ||
        slot->state != MR_PORTER_SLOT_DONE ||
        slot->task_id != reply.result.task_id) {
        return EPROTO;
    }
    if (err == 0 && slot->response_len > porter->slot_bytes) {
        err = EMSGSIZE;
    }
    if (err == 0 && slot->status == 0 && slot->response_len > 0 &&
        response != NULL) {
        byte_buf_append(response, slot_response(slot, porter->slot_bytes),
                        (size_t)slot->response_len);
    }
    if (err == 0) {
        err = reply.result.status != 0 ? (int)reply.result.status
                                       : (int)slot->status;
    }
    if (worker_id != NULL) {
        *worker_id = completed_worker;
    }
    if (result != NULL) {
        *result = reply.result;
    }
    slot_publish(slot, MR_PORTER_SLOT_EMPTY);
    porter->busy[completed_worker] = false;
    return err;
}

int mr_porter_call(mr_porter *porter, uint32_t worker_id, uint32_t kind,
                   uint64_t task_id, uint64_t partition_id,
                   uint64_t partition_count, const void *payload,
                   size_t payload_len, byte_buf *response,
                   mr_porter_result *result)
{
    int err = mr_porter_submit(porter, worker_id, kind, task_id,
                               partition_id, partition_count, payload,
                               payload_len, 0, 0);
    if (err != 0) {
        return err;
    }
    for (;;) {
        uint32_t completed_worker = UINT32_MAX;
        err = mr_porter_collect(porter, &completed_worker, response, result);
        if (err == EAGAIN || err == EWOULDBLOCK) {
            sched_yield();
            continue;
        }
        if (completed_worker != worker_id) {
            return EPROTO;
        }
        return err;
    }
}

void mr_porter_stop(mr_porter *porter)
{
    if (porter == NULL) {
        return;
    }
    mr_porter_command command = {
        .size = sizeof(command),
        .version = MR_PORTER_PROTOCOL_VERSION,
        .op = MR_PORTER_OP_STOP,
    };
    mr_porter_reply reply;
    (void)write_full(porter->control_fd, &command, sizeof(command));
    (void)read_full(porter->control_fd, &reply, sizeof(reply));
    close(porter->control_fd);
    (void)waitpid(porter->supervisor_pid, NULL, 0);
    munmap(porter->shared, porter->shared_len);
    free(porter->busy);
    free(porter);
}
