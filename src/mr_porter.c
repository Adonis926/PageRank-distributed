#define _GNU_SOURCE

#include "mr_porter.h"

#include <errno.h>
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
    uint32_t status;
    uint32_t kind;
    uint32_t reserved;
    uint64_t task_id;
    uint64_t partition_id;
    uint64_t partition_count;
    uint64_t payload_len;
    uint64_t response_len;
    uint64_t restore_ioctl_ns;
    uint64_t template_validate_ns;
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

static void slot_wait_for(const volatile uint32_t *state, uint32_t wanted)
{
    while (*state != wanted) {
        sched_yield();
    }
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
                           mr_porter_slot *slot)
{
    byte_buf response = {0};
    mr_cxlfork_session *session = NULL;
    int err = 0;

    memset(&slot->restore, 0, sizeof(slot->restore));
    slot->restore_ioctl_ns = 0;
    slot->template_validate_ns = 0;
    if (config->mode == MR_LAUNCH_CXLFORK) {
        uint64_t begin = nsec_now();
        err = mr_cxlfork_restore_begin(&config->restore, &session,
                                       &slot->restore);
        slot->restore_ioctl_ns = nsec_now() - begin;
        if (err == 0) {
            begin = nsec_now();
            err = mr_template_validate(
                (void *)(uintptr_t)config->restore.template.base,
                &config->restore.template, NULL);
            slot->template_validate_ns = nsec_now() - begin;
            if (config->restore_hold_ms != 0) {
                usleep((useconds_t)config->restore_hold_ms * 1000U);
            }
        }
    }

    if (err == 0 && slot->payload_len > config->slot_bytes) {
        err = EMSGSIZE;
    } else if (err == 0) {
        err = config->task_fn(config->task_context, worker_id, slot->kind,
                              slot->task_id, slot->partition_id,
                              slot->partition_count, slot_payload(slot),
                              (size_t)slot->payload_len, &response);
    }
    if (session != NULL) {
        int collect_err = mr_cxlfork_restore_collect(session, &slot->restore);
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
    slot_publish(slot, MR_PORTER_SLOT_DONE);
}

static void worker_main(const mr_porter_config *config, uint32_t worker_id,
                        mr_porter_slot *slot, bool persistent, int control_fd)
{
    close(control_fd);
    if (config->mode != MR_LAUNCH_CXLFORK &&
        mr_template_validate((void *)(uintptr_t)config->template.base,
                             &config->template, NULL) != 0) {
        _exit(120);
    }
    if (!persistent) {
        while (slot->state != MR_PORTER_SLOT_READY &&
               slot->state != MR_PORTER_SLOT_STOP) {
            sched_yield();
        }
        if (slot->state == MR_PORTER_SLOT_STOP) {
            _exit(0);
        }
        __sync_synchronize();
        worker_run_one(config, worker_id, slot);
        _exit(slot->status == 0 ? 0 : 1);
    }
    for (;;) {
        while (slot->state != MR_PORTER_SLOT_READY &&
               slot->state != MR_PORTER_SLOT_STOP) {
            sched_yield();
        }
        if (slot->state == MR_PORTER_SLOT_STOP) {
            _exit(0);
        }
        if (slot->state == MR_PORTER_SLOT_READY) {
            worker_run_one(config, worker_id, slot);
        }
    }
}

static pid_t spawn_worker(const mr_porter_config *config, uint32_t worker_id,
                          mr_porter_slot *slot, bool persistent, int control_fd)
{
    pid_t pid = fork();
    if (pid == 0) {
        worker_main(config, worker_id, slot, persistent, control_fd);
    }
    return pid;
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
    mr_porter_reply reply = {
        .size = sizeof(reply),
        .version = MR_PORTER_PROTOCOL_VERSION,
        .op = MR_PORTER_OP_INIT,
    };
    void *template_mapping = NULL;
    int err = 0;

    if (pids == NULL) {
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
        }
    }
    if (err != 0 && pids != NULL) {
        stop_workers(pids, &config, shared);
    }
    reply.result.status = (uint32_t)err;
    if (write_full(control_fd, &reply, sizeof(reply)) != 0 || err != 0) {
        mr_template_unmap(template_mapping, config.template.bytes);
        free(pids);
        close(control_fd);
        _exit(err == 0 ? 121 : 1);
    }

    for (;;) {
        mr_porter_command command;
        if (read_full(control_fd, &command, sizeof(command)) != 0) {
            break;
        }
        if (command.size != sizeof(command) ||
            command.version != MR_PORTER_PROTOCOL_VERSION) {
            break;
        }
        if (command.op == MR_PORTER_OP_STOP) {
            reply.op = MR_PORTER_OP_STOP;
            memset(&reply.result, 0, sizeof(reply.result));
            (void)write_full(control_fd, &reply, sizeof(reply));
            break;
        }
        if (command.op != MR_PORTER_OP_DISPATCH ||
            command.worker_id >= config.workers) {
            break;
        }

        mr_porter_slot *slot =
            slot_at(shared, config.slot_bytes, command.worker_id);
        uint64_t begin = nsec_now();
        uint64_t phase;
        pid_t executed_pid = pids[command.worker_id];
        memset(&reply.result, 0, sizeof(reply.result));
        reply.op = MR_PORTER_OP_DISPATCH;
        reply.result.worker_id = command.worker_id;
        reply.result.task_id = command.task_id;
        reply.result.dispatch_ns = begin - command.dispatch_ns;

        if (config.mode == MR_LAUNCH_LOCALFORK) {
            phase = nsec_now();
            executed_pid = spawn_worker(&config, command.worker_id, slot,
                                        false, control_fd);
            reply.result.fork_ns = nsec_now() - phase;
            if (executed_pid < 0) {
                reply.result.status = (uint32_t)errno;
            }
        }
        if (reply.result.status == 0) {
            reply.result.worker_pid = executed_pid;
            phase = nsec_now();
            slot_wait_for(&slot->state, MR_PORTER_SLOT_DONE);
            reply.result.task_ns = nsec_now() - phase;
            reply.result.status = slot->status;
            reply.result.restore_ioctl_ns = slot->restore_ioctl_ns;
            reply.result.template_validate_ns = slot->template_validate_ns;
            reply.result.restore = slot->restore;
        }
        if (config.mode != MR_LAUNCH_PERSISTENT && executed_pid > 0) {
            phase = nsec_now();
            (void)waitpid(executed_pid, NULL, 0);
            reply.result.reap_ns = nsec_now() - phase;
        }
        if (config.mode == MR_LAUNCH_LOCAL_PREFORK ||
            config.mode == MR_LAUNCH_CXLFORK) {
            phase = nsec_now();
            pids[command.worker_id] = spawn_worker(
                &config, command.worker_id, slot, false, control_fd);
            if (pids[command.worker_id] < 0 && reply.result.status == 0) {
                reply.result.status = (uint32_t)errno;
            }
            reply.result.refill_ns = nsec_now() - phase;
        }
        reply.result.total_ns = nsec_now() - begin;
        if (write_full(control_fd, &reply, sizeof(reply)) != 0) {
            break;
        }
    }

    if (pids != NULL) {
        stop_workers(pids, &config, shared);
    }
    mr_template_unmap(template_mapping, config.template.bytes);
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
    porter->shared_len = slot_stride(config->slot_bytes) * config->workers;
    porter->shared = mmap(NULL, porter->shared_len, PROT_READ | PROT_WRITE,
                          MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (porter->shared == MAP_FAILED) {
        int err = errno;
        free(porter);
        return err;
    }
    if (socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, sockets) != 0) {
        int err = errno;
        munmap(porter->shared, porter->shared_len);
        free(porter);
        return err;
    }
    porter->supervisor_pid = fork();
    if (porter->supervisor_pid < 0) {
        int err = errno;
        close(sockets[0]);
        close(sockets[1]);
        munmap(porter->shared, porter->shared_len);
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
        free(porter);
        return err;
    }
    *out = porter;
    return 0;
}

int mr_porter_call(mr_porter *porter, uint32_t worker_id, uint32_t kind,
                   uint64_t task_id, uint64_t partition_id,
                   uint64_t partition_count, const void *payload,
                   size_t payload_len, byte_buf *response,
                   mr_porter_result *result)
{
    if (porter == NULL || worker_id >= porter->workers ||
        payload_len > porter->slot_bytes) {
        return EINVAL;
    }
    mr_porter_slot *slot = slot_at(porter->shared, porter->slot_bytes, worker_id);
    if (slot->state != MR_PORTER_SLOT_EMPTY) {
        return EBUSY;
    }
    slot->status = 0;
    slot->kind = kind;
    slot->task_id = task_id;
    slot->partition_id = partition_id;
    slot->partition_count = partition_count;
    slot->payload_len = payload_len;
    slot->response_len = 0;
    slot->restore_ioctl_ns = 0;
    slot->template_validate_ns = 0;
    memset(&slot->restore, 0, sizeof(slot->restore));
    if (payload_len > 0) {
        memcpy(slot_payload(slot), payload, payload_len);
    }
    slot_publish(slot, MR_PORTER_SLOT_READY);

    mr_porter_command command = {
        .size = sizeof(command),
        .version = MR_PORTER_PROTOCOL_VERSION,
        .op = MR_PORTER_OP_DISPATCH,
        .worker_id = worker_id,
        .task_id = task_id,
        .dispatch_ns = nsec_now(),
    };
    int err = write_full(porter->control_fd, &command, sizeof(command));
    mr_porter_reply reply;
    if (err == 0) {
        err = read_full(porter->control_fd, &reply, sizeof(reply));
    }
    if (err == 0 && (reply.size != sizeof(reply) ||
                     reply.version != MR_PORTER_PROTOCOL_VERSION ||
                     reply.op != MR_PORTER_OP_DISPATCH ||
                     reply.result.task_id != task_id ||
                     reply.result.worker_id != worker_id)) {
        err = EPROTO;
    }
    if (err == 0 && slot->response_len > porter->slot_bytes) {
        err = EMSGSIZE;
    }
    if (err == 0 && slot->status == 0 && slot->response_len > 0) {
        byte_buf_append(response, slot_response(slot, porter->slot_bytes),
                        (size_t)slot->response_len);
    }
    if (err == 0) {
        err = reply.result.status != 0 ? (int)reply.result.status
                                       : (int)slot->status;
    }
    if (result != NULL && err != EPROTO) {
        *result = reply.result;
    }
    slot_publish(slot, MR_PORTER_SLOT_EMPTY);
    return err;
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
    free(porter);
}
