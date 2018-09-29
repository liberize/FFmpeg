#include "config.h"

#include "libavutil/avassert.h"
#include "libavutil/avstring.h"
#include "libavutil/opt.h"
#include "libavutil/time.h"
#include "libavutil/parseutils.h"
#include "libavutil/base64.h"
#include "libavutil/rc4.h"
#include "libavutil/md5.h"
#include "libavutil/thread.h"

#include "avformat.h"
#include "internal.h"
#include "network.h"
#include "os_support.h"
#include "url.h"
#include "fileutil.h"
#include "urldecode.h"
#include "baidupcs.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#ifndef WIN32
#include <unistd.h>
#endif
#include <curl/curl.h>
#include <curl/multi.h>


struct Worker;

typedef struct Chunk {
    uint8_t *buffer;
    int64_t size;
    int64_t read_pos;
    int64_t end_pos;
    int64_t start;
    struct Worker *worker;
    struct Chunk *prev;
    struct Chunk *next;
} Chunk;

typedef struct Worker {
    CURL *curl_handle;
    int64_t start;
    int64_t end;
    int64_t downloaded_size;
    int64_t next_reconnect;
    struct Chunk *current_chunk;
    struct Worker *prev;
    struct Worker *next;
} Worker;

typedef Chunk *ChunkPool;   // double linked list (chunk_pool point to head node)
typedef Worker *WorkerPool; // double linked list (worker_pool point to tail node)

typedef struct MTSPContext {
    const AVClass *class;
    pthread_t download_thread;
    pthread_mutex_t mutex_init;
    pthread_cond_t cond_init_done;
    pthread_mutex_t mutex_data;
    pthread_cond_t cond_data_avail;
    int init_done;
    int abort_download;
    int exit_code;
    Chunk *current_read_chunk;
    int64_t read_pos;
    int buffer_not_enough;
    int seek_end_for_meta;
    char *url;
    CURLM *curl_multi_handle;
    char *file_dir;
    int64_t file_size;
    char *file_name;
    char *file_md5;
    char *mime_type;
    char *progress_file_name;
    WorkerPool worker_pool;
    int running_workers;
    int reconnect_interval;
    int update_speed_interval;
    int64_t downloaded_size;
    int worker_avg_speed;
    int finished_workers;
    ChunkPool chunk_pool;
    int dont_write_disk;
    int disk_cache;
    FILE *fp;
    FILE *progress_fp;
    int max_conn;
    int bitrate;            // 0: unknown, other: bitrate
    int throttled_speed;    // 0: unknown, -1: no throttle, other: throttled speed
    int min_range_len;
    int max_range_len;
    int last_range_len;
    char *user_agent;
    char *referer;
    char *cookies;
    char *headers;
    uint8_t *post_data;
    int post_data_len;
    char *ssl_public_key;
    char *preset;
    char *md5_blacklist;
} MTSPContext;

typedef struct CURLWriteData {
    MTSPContext *context;
    Worker *worker;
} CURLWriteData;

typedef struct CURLHeaderData {
    MTSPContext *context;
    CURL *curl_handle;
} CURLHeaderData;

typedef struct ByteRange {
    int64_t start;
    int64_t end;
} ByteRange;

#define OFFSET(x) offsetof(MTSPContext, x)
#define D AV_OPT_FLAG_DECODING_PARAM
#define E AV_OPT_FLAG_ENCODING_PARAM
#define DEFAULT_USER_AGENT "Lavf/" AV_STRINGIFY(LIBAVFORMAT_VERSION)
#define WHITESPACES " \n\t\r"
#define MAX_RANGES 4096
#define MD5_BLOCK_SIZE (1024 * 1024)
#define BUFFER_ENOUGH_THRESHOLD (2 * 1024 * 1024)

// rc4 key (md5 for "multi-thread streaming protocol")
const char MTSP_RC4_KEY[16] = { 0xbf, 0xa8, 0xf0, 0x70, 0x1e, 0xc7, 0xdc, 0x86, 0x60, 0xad, 0xb4, 0x42, 0x88, 0x77, 0x94, 0xcf };

static const AVOption options[] = {
    { "file_dir", "output directory path", OFFSET(file_dir), AV_OPT_TYPE_STRING, { .str = NULL }, 0, 0, D },
    { "reconnect_interval", "reconnect interval in seconds", OFFSET(reconnect_interval), AV_OPT_TYPE_INT, { .i64 = 3 }, 0, 60, D },
    { "update_speed_interval", "calculate speed interval in seconds", OFFSET(update_speed_interval), AV_OPT_TYPE_INT, { .i64 = 5 }, 0, 60, D },
    { "dont_write_disk", "buffering data in memory only", OFFSET(dont_write_disk), AV_OPT_TYPE_BOOL, { .i64 = 0 }, 0, 1, D },
    { "disk_cache", "disk cache size in bytes", OFFSET(disk_cache), AV_OPT_TYPE_INT, { .i64 = 1 * 1024 * 1024 }, 0, 64 * 1024 * 1024, D },
    { "max_conn", "max simultaneous connections to server", OFFSET(max_conn), AV_OPT_TYPE_INT, { .i64 = 64 }, 1, 2048, D },
    { "bitrate", "bit rate of stream", OFFSET(bitrate), AV_OPT_TYPE_INT, { .i64 = 0 }, 0, INT_MAX, D },
    { "throttled_speed", "speed limit by server", OFFSET(throttled_speed), AV_OPT_TYPE_INT, { .i64 = 0 }, -1, INT_MAX, D },
    { "min_range_len", "min download range in bytes", OFFSET(min_range_len), AV_OPT_TYPE_INT, { .i64 = 100 * 1024 }, 0, INT_MAX, D },
    { "max_range_len", "max download range in bytes", OFFSET(max_range_len), AV_OPT_TYPE_INT, { .i64 = 2 * 1024 * 1024 }, 0, INT_MAX, D },
    { "user_agent", "User-Agent header", OFFSET(user_agent), AV_OPT_TYPE_STRING, { .str = NULL }, 0, 0, D },
    { "referer", "Referer header", OFFSET(referer), AV_OPT_TYPE_STRING, { .str = NULL }, 0, 0, D },
    { "cookies", "cookies to be sent in future requests, ';' delimited", OFFSET(cookies), AV_OPT_TYPE_STRING, { .str = NULL }, 0, 0, D },
    { "headers", "custom HTTP headers, can override default headers", OFFSET(headers), AV_OPT_TYPE_STRING, { .str = NULL }, 0, 0, D },
    { "post_data", "custom HTTP post data", OFFSET(post_data), AV_OPT_TYPE_BINARY, .flags = D },
    { "ssl_public_key", "ssl public key for verification", OFFSET(ssl_public_key), AV_OPT_TYPE_STRING, { .str = NULL }, 0, 0, D },
    { "preset", "preset configurations", OFFSET(preset), AV_OPT_TYPE_STRING, { .str = NULL }, 0, 0, D },
    { "md5_blacklist", "file md5 blacklist, ';' delimited", OFFSET(md5_blacklist), AV_OPT_TYPE_STRING, { .str = NULL }, 0, 0, D },
    { NULL }
};


static int init_curl_handle(MTSPContext *s, Worker *worker);
static void destroy_curl_handle(MTSPContext *s, Worker *worker);
static int is_download_complete(MTSPContext *s);


static Chunk *insert_new_chunk(MTSPContext *s, Chunk *chunk, int64_t start, int64_t size, Worker *worker)
{
    av_log(s, AV_LOG_DEBUG, "insert_new_chunk <%"PRId64"-%"PRId64">\n", start, start + size);
    Chunk *new_chunk = av_malloc(sizeof(Chunk));
    if (!new_chunk) {
        av_log(s, AV_LOG_ERROR, "failed to alloc new chunk\n");
        return NULL;
    }
    new_chunk->size = size;
    new_chunk->read_pos = 0;
    new_chunk->worker = worker;
    if (!worker) {
        new_chunk->buffer = NULL;
        new_chunk->end_pos = size;
    } else {
        new_chunk->buffer = av_malloc(size);
        if (!new_chunk->buffer) {
            av_log(s, AV_LOG_ERROR, "failed to alloc buffer for new chunk, size: %lld\n", size);
            av_free(new_chunk);
            return NULL;
        }
        new_chunk->end_pos = 0;
        worker->current_chunk = new_chunk;
    }
    if (chunk) {
        new_chunk->start = start < 0 ?  chunk->start + chunk->size : start;
        new_chunk->prev = chunk;
        new_chunk->next = chunk->next;
        if (chunk->next)
            chunk->next->prev = new_chunk;
        chunk->next = new_chunk;
    } else {
        Chunk *head = s->chunk_pool;
        new_chunk->start = start < 0 ? 0 : start;
        new_chunk->prev = NULL;
        new_chunk->next = head;
        if (head)
            head->prev = new_chunk;
        s->chunk_pool = new_chunk;
    }
    if (!s->current_read_chunk && s->read_pos == start)
        s->current_read_chunk = new_chunk;
    return new_chunk;
}

static void remove_chunk(MTSPContext *s, Chunk *chunk)
{
    av_log(s, AV_LOG_DEBUG, "remove_chunk <%"PRId64"-%"PRId64">\n",
        chunk->start, chunk->start + chunk->size);
    av_assert0(!chunk->worker);
    if (chunk->prev)
        chunk->prev->next = chunk->next;
    if (chunk->next)
        chunk->next->prev = chunk->prev;
    if (chunk == s->chunk_pool)
        s->chunk_pool = chunk->next;
    av_free(chunk->buffer);
    av_free(chunk);
}

static int merge_next_chunk(MTSPContext *s, Chunk *chunk)
{
    Chunk *next = chunk->next;
    if (!next)
        return -1;
    if ((chunk->buffer && !next->buffer) || (!chunk->buffer && next->buffer))
        return -2;
    if (chunk->end_pos < chunk->size || next->end_pos < next->size)
        return -3;
    if (chunk->start + chunk->size != next->start)
        return -4;
    if (chunk == s->current_read_chunk || chunk->next == s->current_read_chunk)
        return -5;

    av_log(s, AV_LOG_DEBUG, "merge_next_chunk <%"PRId64"-%"PRId64"> and <%"PRId64"-%"PRId64">\n",
        chunk->start, chunk->start + chunk->size, next->start, next->start + next->size);
    int64_t new_size = chunk->size + next->size;
    if (chunk->buffer) {
        uint8_t *new_buffer = av_realloc(chunk->buffer, new_size);
        if (!new_buffer)
            return AVERROR(ENOMEM);
        memcpy(new_buffer + chunk->size, next->buffer, next->size);
        av_free(next->buffer);
        chunk->buffer = new_buffer;
        chunk->worker = next->worker;
        if (chunk->worker)
            chunk->worker->current_chunk = chunk;
    } else
        chunk->worker = NULL;

    chunk->size = new_size;
    if (chunk->read_pos)
        chunk->read_pos = new_size;
    chunk->end_pos = new_size;
    chunk->next = next->next;
    if (chunk->next)
        chunk->next->prev = chunk;
    av_free(next);
    return 0;
}

static int check_output_dir(MTSPContext *s)
{
    av_log(s, AV_LOG_DEBUG, "check_output_dir\n");
    if (s->file_dir) {
        create_directory(s->file_dir);
        if (!is_writable(s->file_dir)) {
            return AVERROR(EIO);
        }
    } else {
        char *dir = current_directory();
        if (dir && is_writable(dir))
            s->file_dir = dir;
        else {
            av_free(dir);
            dir = temp_directory_path();
            if (dir && is_writable(dir))
                s->file_dir = dir;
            else {
                av_free(dir);
                return AVERROR(EIO);
            }
        }
    }
    return 0;
}

static int open_local_file(MTSPContext *s, int *exists)
{
    if (s->dont_write_disk)
        return -1;
    if (s->fp)
        return -2;
    if (check_output_dir(s) < 0)
        return -3;

    if (!s->file_name) {
        char *p = strrchr(s->url, '/');
        s->file_name = av_strdup(p ? p + 1 : s->url);
    }

    if (!s->file_size) {
        av_log(s, AV_LOG_ERROR, "file size unknown\n");
        return AVERROR(EINVAL);
    }

    av_log(s, AV_LOG_DEBUG, "open_local_file %s\n", s->file_name);
    char *file_path = av_asprintf("%s/%s", s->file_dir, s->file_name);
    *exists = 0;
    s->fp = fopen(file_path, "r+b");
    if (s->fp) {
        size_t size = file_size(s->fp);
        if (size == s->file_size)
            *exists = 1;
        else {
            fclose(s->fp);
            s->fp = NULL;
        }
    }
    if (!s->fp) {
        s->fp = fopen(file_path, "w+b");
        if (!s->fp) {
            av_log(s, AV_LOG_ERROR, "failed to open file: %s\n", file_path);
            return AVERROR(EIO);
        }
        int ret = allocate_file(s->fp, 0, s->file_size, 0);
        if (ret < 0) {
            ret = truncate_file(s->fp, s->file_size);
            if (ret < 0)
                return ret;
        }
    }
    return 0;
}

static void close_local_file(MTSPContext *s)
{
    av_log(s, AV_LOG_DEBUG, "close_local_file\n");
    if (s->fp) {
        fclose(s->fp);
        s->fp = NULL;
    }
}

static int open_progress_file(MTSPContext *s, const char *mode)
{
    if (s->dont_write_disk)
        return -1;
    if (s->progress_fp)
        return -2;
    if (check_output_dir(s) < 0)
        return -3;

    if (!s->progress_file_name) {
        if (!s->file_name)
            return -4;
        s->progress_file_name = av_asprintf("%s.tmp", s->file_name);
        av_log(s, AV_LOG_INFO, "file name: %s, progress file name: %s\n", s->file_name, s->progress_file_name);
    }

    av_log(s, AV_LOG_DEBUG, "open_progress_file %s\n", s->progress_file_name);
    char *file_path = av_asprintf("%s/%s", s->file_dir, s->progress_file_name);
    s->progress_fp = fopen(file_path, mode);
    av_free(file_path);
    if (!s->progress_fp) {
        av_log(s, AV_LOG_ERROR, "failed to open progress file: %s with mode: %s\n", file_path, mode);
        return AVERROR(EIO);
    }
    return 0;
}

static void close_progress_file(MTSPContext *s)
{
    av_log(s, AV_LOG_DEBUG, "close_progress_file\n");
    if (s->progress_fp) {
        fclose(s->progress_fp);
        s->progress_fp = NULL;
    }
}

static int remove_progress_file(MTSPContext *s)
{
    if (check_output_dir(s) < 0)
        return -1;
    if (!s->progress_file_name)
        return -2;

    av_log(s, AV_LOG_DEBUG, "remove_progress_file\n");
    char *file_path = av_asprintf("%s/%s", s->file_dir, s->progress_file_name);
    int ret = remove_file(file_path);
    if (ret < 0)
        return ret;
    return 0;
}

static int save_progress(MTSPContext *s)
{
    av_log(s, AV_LOG_DEBUG, "save_progress\n");
    if (!s->progress_fp)
        return -1;

    ByteRange *ranges = malloc(MAX_RANGES * sizeof(ByteRange));
    int i = -1;
    Chunk *chunk = s->chunk_pool;
    while (chunk) {
        if (!chunk->buffer) {
            if (i == -1) {
                i = 0;
                ranges[i].start = chunk->start;
            } else if (ranges[i].end != chunk->start) {
                if (i + 1 >= MAX_RANGES) {
                    av_log(s, AV_LOG_ERROR, "too many ranges\n");
                    break;
                }
                ranges[++i].start = chunk->start;
            }
            ranges[i].end = chunk->start + chunk->end_pos;
        }
        chunk = chunk->next;
    }
    if (i != -1) {
        fseek(s->progress_fp, 0, SEEK_SET);
        int nwritten = fwrite(ranges, sizeof(ByteRange), i + 1, s->progress_fp);
        if (nwritten != i + 1) {
            av_log(s, AV_LOG_ERROR, "failed to write progress\n");
            return AVERROR(EIO);
        }
    }
    return 0;
}

static void save_progress_to_file(MTSPContext *s)
{
    int ret = open_progress_file(s, "wb");
    if (ret == 0) {
        save_progress(s);
        close_progress_file(s);
    }
}

static int load_progress(MTSPContext *s)
{
    av_log(s, AV_LOG_DEBUG, "load_progress\n");
    if (!s->progress_fp)
        return -1;
    if (s->chunk_pool)
        return -2;

    size_t size = file_size(s->progress_fp);
    if (size % sizeof(ByteRange)) {
        av_log(s, AV_LOG_ERROR, "progress file size error\n");
        return -3;
    }
    
    ByteRange *ranges = malloc(size);
    int count = size / sizeof(ByteRange);
    int nread = fread(ranges, sizeof(ByteRange), count, s->progress_fp);
    if (nread != count) {
        av_log(s, AV_LOG_ERROR, "failed to read progress\n");
        return AVERROR(EIO);
    }

    for (int i = 0; i < count; i++) {
        if (ranges[i].start < 0 || ranges[i].start >= ranges[i].end ||
            (i && ranges[i].start < ranges[i-1].end)) {
            av_log(s, AV_LOG_ERROR, "invalid progress file, start: %"PRId64", end: %"PRId64"\n", ranges[i].start, ranges[i].end);
            return -4;
        }
    }

    Chunk *chunk = NULL;
    for (int i = 0; i < count; i++) {
        Chunk *new_chunk = insert_new_chunk(s, chunk, ranges[i].start, ranges[i].end - ranges[i].start, NULL);
        if (new_chunk)
            chunk = new_chunk;
        else
            av_log(s, AV_LOG_ERROR, "failed to recover chunk\n");
    }

    if (!s->current_read_chunk && s->chunk_pool && s->chunk_pool->start == 0)
        s->current_read_chunk = s->chunk_pool;
    return 0;
}

static void load_progress_from_file(MTSPContext *s)
{
    int ret = open_progress_file(s, "rb");
    if (ret == 0) {
        load_progress(s);
        close_progress_file(s);
    }
}

static int write_chunk_to_disk(MTSPContext *s, Chunk *chunk, int flush)
{
    if (s->dont_write_disk)
        return -1;
    if (!chunk->buffer)
        return -2;
    if (!flush) {
        if (chunk->end_pos < chunk->size)
            return -3;
        if (chunk == s->current_read_chunk)
            return -4;
        if (chunk->size < s->disk_cache)
            return -5;
    }
    if (!s->fp) {
        av_log(s, AV_LOG_ERROR, "file not opened\n");
        return AVERROR(ENOTSUP);
    }
    av_log(s, AV_LOG_DEBUG, "write_chunk_to_disk <%"PRId64"-%"PRId64">\n",
        chunk->start, chunk->start + chunk->size);
    fseek(s->fp, chunk->start, SEEK_SET);
    size_t nwritten = fwrite(chunk->buffer, 1, chunk->end_pos, s->fp);
    if (nwritten != chunk->end_pos) {
        av_log(s, AV_LOG_ERROR, "failed to write chunk (%"PRIu64"-%"PRIu64")\n",
            chunk->start, chunk->start + chunk->size - 1);
        return AVERROR(EIO);
    }
    av_free(chunk->buffer);
    chunk->buffer = NULL;
    if (!flush) {
        int ret = open_progress_file(s, "wb");
        if (ret == 0) {
            save_progress(s);
            close_progress_file(s);
        }
    }
    return 0;
}

static Chunk *on_chunk_full(MTSPContext *s, Chunk *chunk)
{
    av_log(s, AV_LOG_DEBUG, "on_chunk_full <%"PRId64"-%"PRId64">\n",
        chunk->start, chunk->start + chunk->size);
    if (chunk->worker && chunk->start + chunk->size == chunk->worker->end)
        chunk->worker->current_chunk = NULL;
    chunk->worker = NULL;
    merge_next_chunk(s, chunk);
    Chunk *prev = chunk->prev;
    if (prev && merge_next_chunk(s, prev) == 0)
        chunk = prev;
    if (!s->dont_write_disk)
        if (write_chunk_to_disk(s, chunk, 0) == 0) {
            merge_next_chunk(s, chunk);
            prev = chunk->prev;
            if (prev && merge_next_chunk(s, prev) == 0)
                chunk = prev;
        }
    return chunk;
}

static Chunk *on_chunk_read(MTSPContext *s, Chunk *chunk)
{
    av_log(s, AV_LOG_DEBUG, "on_chunk_read <%"PRId64"-%"PRId64">\n",
        chunk->start, chunk->start + chunk->size);
    Chunk *prev = chunk->prev;
    if (prev && merge_next_chunk(s, prev) == 0)
        chunk = prev;
    if (s->dont_write_disk)
        // remove played chunk to save memory
        remove_chunk(s, chunk);
    else if (write_chunk_to_disk(s, chunk, 0) == 0) {
        prev = chunk->prev;
        if (prev && merge_next_chunk(s, prev) == 0)
            chunk = prev;
    }
    return chunk;
}

static void find_max_unfinished(Chunk *start, Chunk *end, int *max_unfinished, Chunk **max_chunk)
{
    *max_unfinished = 0;
    *max_chunk = NULL;
    Chunk *chunk = start;
    while (chunk != end) {
        if (chunk->size - chunk->end_pos > *max_unfinished) {
            *max_unfinished = chunk->size - chunk->end_pos;
            *max_chunk = chunk;
        }
        chunk = chunk->next;
    }
}

static void reset_worker_end(MTSPContext *s, Worker *worker, int64_t end)
{
    if (!worker || end >= worker->end)
        return;
    av_log(s, AV_LOG_DEBUG, "reset_worker_end <%"PRId64"-%"PRId64"> to %"PRId64"\n",
            worker->start, worker->end, end);
    worker->end = end;
    Chunk *chunk = worker->current_chunk;
    if (chunk && chunk->start + chunk->size > end) {
        av_log(s, AV_LOG_INFO, "reset_worker_end, shrink chunk\n");
        av_assert0(chunk->start + chunk->end_pos <= end);
        if (end == chunk->start) {
            chunk->worker->current_chunk = NULL;
            chunk->worker = NULL;
            remove_chunk(s, chunk);
        } else {
            chunk->size = end - chunk->start;
            uint8_t *new_buffer = av_realloc(chunk->buffer, chunk->size);
            if (!new_buffer)
                av_log(s, AV_LOG_WARNING, "failed to shrink chunk buffer\n");
            else
                chunk->buffer = new_buffer;
            if (chunk->end_pos == chunk->size)
                chunk = on_chunk_full(s, chunk);
        }
    }
}

static int pick_next_range(MTSPContext *s, int64_t *start, int64_t *end)
{
    int max_conn = s->max_conn;
    if (s->seek_end_for_meta)
        max_conn += 1;
    if (s->running_workers >= max_conn)
        return -1;

    int64_t end_pos = s->file_size;
    *start = 0;
    int fill_hole = 0;
    Chunk *chunk = s->current_read_chunk;
    if (chunk) {
        av_assert0(s->read_pos == chunk->start + chunk->read_pos);
        while (chunk->next && chunk->next->start == chunk->start + chunk->size)
            chunk = chunk->next;
        *start = chunk->start + chunk->size;
        if (chunk->next)
            end_pos = chunk->next->start;
        if (s->buffer_not_enough)
            *start = FFMIN(*start, chunk->start + chunk->end_pos + s->min_range_len);
        if (s->dont_write_disk && *start > s->read_pos + BUFFER_ENOUGH_THRESHOLD * 10)
            return -2;
        reset_worker_end(s, chunk->worker, *start);
        if (*start == s->file_size) {
            s->buffer_not_enough = 0;
            if (s->dont_write_disk)
                return -3;
            fill_hole = 1;
        }
    } else if (s->chunk_pool) {
        chunk = s->chunk_pool;
        while (chunk && chunk->start <= s->read_pos)
            chunk = chunk->next;
        *start = s->read_pos;
        if (chunk)
            end_pos = chunk->start;
    }
    av_assert0(fill_hole || end_pos > *start);

    if (fill_hole) {
        chunk = s->current_read_chunk;
        while (chunk->prev && chunk->start == chunk->prev->start + chunk->prev->size)
            chunk = chunk->prev;
        if (chunk->prev) {
            *start = chunk->prev->start + chunk->prev->size;
            end_pos = chunk->start;
            reset_worker_end(s, chunk->prev->worker, *start);
        } else if (chunk->start) {
            *start = 0;
            end_pos = chunk->start;
        } else {
            int max_unfinished = 0;
            Chunk *max_chunk = NULL;
            find_max_unfinished(s->current_read_chunk, NULL, &max_unfinished, &max_chunk);
            if (max_unfinished < s->min_range_len) {
                find_max_unfinished(s->chunk_pool, s->current_read_chunk, &max_unfinished, &max_chunk);
                if (max_unfinished < s->min_range_len)
                    return -4;
            }
            *start = max_chunk->start + max_chunk->end_pos + max_unfinished / 2;
            end_pos = max_chunk->start + max_chunk->size;
            reset_worker_end(s, max_chunk->worker, *start);
        }
    }

    if (s->throttled_speed == -1)
        *end = FFMIN(*start + s->max_range_len, end_pos);
    else {
        int len = s->min_range_len;
        if (s->seek_end_for_meta) {
            len = s->min_range_len;
        } else if (fill_hole) {
            len = end_pos - *start;
            len = FFMIN(FFMAX(len, s->min_range_len), s->max_range_len);
        } else if (s->buffer_not_enough) {
            len = s->min_range_len;
        } else if (s->bitrate && s->throttled_speed > 0) {
            len = s->throttled_speed * (*start - s->read_pos) / (s->bitrate / 8 - s->throttled_speed);
            len = len * 3 / 4;
            len = FFMIN(FFMAX(len, s->min_range_len), s->max_range_len);
        } else if (s->running_workers + s->finished_workers < max_conn) {
            len = s->min_range_len;
        } else if (s->last_range_len) {
            double stable = sqrt((s->min_range_len / 1024.0) * (s->max_range_len / 1024.0)) * 1024.0;
            len = FFMIN(s->last_range_len * 1.1, stable);
        }
        *end = FFMIN(*start + len, end_pos);
        s->last_range_len = len;
    }
    av_log(s, AV_LOG_DEBUG, "pick_next_range, max_conn: %d, start: %"PRId64", end: %"PRId64", len: %"PRId64", "
            "buffer_not_enough: %d, throttled_speed: %d, fill_hole: %d\n",
            max_conn, *start, *end, *end - *start, s->buffer_not_enough, s->throttled_speed, fill_hole);
    return 0;
}

static Worker *create_worker(MTSPContext *s, int64_t start, int64_t end)
{
    av_log(s, AV_LOG_DEBUG, "create_worker <%"PRId64"-%"PRId64">, running_workers: %d\n",
            start, end, s->running_workers);
    Worker *new_worker = av_malloc(sizeof(Worker));
    if (!new_worker) {
        av_log(s, AV_LOG_ERROR, "failed to alloc new worker\n");
        return NULL;
    }
    new_worker->start = start;
    new_worker->end = end;
    new_worker->downloaded_size = 0;
    new_worker->next_reconnect = 0;
    init_curl_handle(s, new_worker);

    Worker *prev_worker = s->worker_pool, *next_worker = NULL;
    while (prev_worker && prev_worker->start > start) {
        next_worker = prev_worker;
        prev_worker = prev_worker->prev;
    }
    Chunk *next_chunk = s->chunk_pool, *prev_chunk = NULL;
    while (next_chunk && next_chunk->start < start) {
        prev_chunk = next_chunk;
        next_chunk = next_chunk->next;
    }
    av_assert0(!prev_chunk || prev_chunk->start + prev_chunk->size <= start);
    new_worker->current_chunk = insert_new_chunk(s, prev_chunk, start, FFMIN(end - start, s->disk_cache), new_worker);
    new_worker->next = next_worker;
    new_worker->prev = prev_worker;
    if (prev_worker)
        prev_worker->next = new_worker;
    if (next_worker)
        next_worker->prev = new_worker;
    else
        s->worker_pool = new_worker;
    s->running_workers++;
    return new_worker;
}

static void destroy_worker(MTSPContext *s, Worker *worker)
{
    av_log(s, AV_LOG_DEBUG, "destroy_worker, <%"PRId64"-%"PRId64">, running_workers: %d\n",
            worker->start, worker->end, s->running_workers);
    Chunk *chunk = worker->current_chunk;
    if (chunk) {
        av_log(s, AV_LOG_DEBUG, "destroy_worker, current chunk <%"PRId64"-%"PRId64">, end pos: %"PRId64"\n",
            chunk->start, chunk->start + chunk->size, chunk->start + chunk->end_pos);
        reset_worker_end(s, worker, chunk->start + chunk->end_pos);
    }
    destroy_curl_handle(s, worker);
    if (worker->prev)
        worker->prev->next = worker->next;
    if (worker->next)
        worker->next->prev = worker->prev;
    if (worker == s->worker_pool)
        s->worker_pool = worker->prev;
    av_free(worker);
    s->running_workers--;
}

static void on_worker_done(MTSPContext *s, Worker *worker)
{
    s->finished_workers++;

    curl_off_t size, speed;
    CURLcode res = curl_easy_getinfo(worker->curl_handle, CURLINFO_SPEED_DOWNLOAD_T, &speed);
    res = res || curl_easy_getinfo(worker->curl_handle, CURLINFO_SIZE_DOWNLOAD_T, &size);
    if (res)
        av_log(s, AV_LOG_WARNING, "failed to get average speed\n");
    else if (size >= s->min_range_len)
        s->worker_avg_speed = (s->worker_avg_speed * (s->finished_workers - 1) + speed) / s->finished_workers;

    av_log(s, AV_LOG_DEBUG, "on_worker_done <%"PRId64"-%"PRId64">, speed: %.1fkB/s, finished workers: %d, avg speed: %.1fkB/s\n",
        worker->start, worker->end, speed / 1024.0, s->finished_workers, s->worker_avg_speed / 1024.0);
    destroy_worker(s, worker);
}

static void on_worker_fail(MTSPContext *s, Worker *worker)
{
    av_log(s, AV_LOG_DEBUG, "on_worker_fail <%"PRId64"-%"PRId64">\n",
        worker->start, worker->end);
    int64_t now = av_gettime_relative();
    worker->next_reconnect = now + s->reconnect_interval * 1000000;
}

static void worker_reconnect(MTSPContext *s, Worker *worker)
{
    Chunk *chunk = worker->current_chunk;
    av_assert0(chunk);
    av_log(s, AV_LOG_DEBUG, "worker_reconnect <%"PRId64"-%"PRId64">, new start: %"PRId64"\n",
        worker->start, worker->end, chunk->start + chunk->end_pos);
    worker->next_reconnect = 0;
    destroy_curl_handle(s, worker);
    worker->start = chunk->start + chunk->end_pos;
    init_curl_handle(s, worker);
}

static void destroy_worker_pool(MTSPContext *s)
{
    av_log(s, AV_LOG_DEBUG, "destroy_worker_pool\n");
    while (s->worker_pool) {
        Worker *tail = s->worker_pool;
        s->worker_pool = tail->prev;
        if (tail->current_chunk)
            reset_worker_end(s, tail, tail->current_chunk->start + tail->current_chunk->end_pos);
        destroy_curl_handle(s, tail);
        av_free(tail);
    }
    s->running_workers = 0;
    s->finished_workers = 0;
}

static void flush_chunk_pool(MTSPContext *s)
{
    av_log(s, AV_LOG_DEBUG, "flush_chunk_pool\n");
    Chunk *chunk = s->chunk_pool;
    while (chunk) {
        write_chunk_to_disk(s, chunk, 1);
        chunk = chunk->next;
    }
    save_progress_to_file(s);
    if (is_download_complete(s))
        remove_progress_file(s);
}

static void destroy_chunk_pool(MTSPContext *s)
{
    av_log(s, AV_LOG_DEBUG, "destroy_chunk_pool\n");
    while (s->chunk_pool) {
        Chunk *head = s->chunk_pool;
        s->chunk_pool = head->next;
        if (head->buffer) {
            av_log(s, AV_LOG_WARNING, "data will be lost\n");
            av_free(head->buffer);
        }
        if (head->worker)
            head->worker->current_chunk = NULL;
        av_free(head);
    }
    s->current_read_chunk = NULL;
}

static void on_buffer_empty(MTSPContext *s)
{
    if (s->seek_end_for_meta)
        return;
    s->buffer_not_enough = 1;
    Chunk *chunk = s->current_read_chunk;
    if (chunk) {
        Worker *worker = chunk->worker;
        if (worker && worker->next_reconnect)
            worker_reconnect(s, worker);
    }
}

static void on_buffer_not_enough(MTSPContext *s)
{
    if (s->seek_end_for_meta)
        return;
    s->buffer_not_enough = 1;
    Worker *worker = s->worker_pool;
    while (worker) {
        if (!worker->current_chunk)
            worker = worker->prev;
        else {
            int64_t pos = worker->current_chunk->start + worker->current_chunk->end_pos;
            if (pos < s->read_pos || pos > s->read_pos + BUFFER_ENOUGH_THRESHOLD * 10) {
                Worker *next = worker->next;
                destroy_worker(s, worker);
                worker = next ? next->prev : s->worker_pool;
            } else
                worker = worker->prev;
        }
    }
}

static void check_buffer_len(MTSPContext *s)
{
    Chunk *chunk = s->current_read_chunk;
    if (!chunk) {
        on_buffer_empty(s);
        return;
    }

    while (chunk->next && chunk->next->start == chunk->start + chunk->end_pos)
        chunk = chunk->next;

    int64_t buffered_data = chunk->start + chunk->end_pos - s->read_pos;
    if (!buffered_data) {
        on_buffer_empty(s);
        return;
    }

    av_log(s, AV_LOG_DEBUG, "check_buffer_len, buffered data: %"PRId64"\n", buffered_data);
    pthread_cond_signal(&s->cond_data_avail);

    if (chunk->start + chunk->end_pos == s->file_size) {
        s->buffer_not_enough = 0;
        return;
    }

    int64_t remain = chunk->size - chunk->end_pos;
    if (remain > 0 && s->bitrate && s->throttled_speed > 0) {
        int64_t len = s->throttled_speed * buffered_data / (s->bitrate / 8 - s->throttled_speed);
        if (len < remain) {
            on_buffer_not_enough(s);
            return;
        }
    }
    if (buffered_data < BUFFER_ENOUGH_THRESHOLD) {
        on_buffer_not_enough(s);
        return;
    }
    s->buffer_not_enough = 0;
}

static size_t put_data_to_pool(MTSPContext *s, Worker *worker, const uint8_t *data, size_t len)
{
    if (!worker->current_chunk)
        return 0;

    Chunk *chunk = worker->current_chunk;
    int64_t remain = len;
    while (remain) {
        int64_t avail = chunk->size - chunk->end_pos;
        if (avail) {
            int64_t copied = FFMIN(remain, avail);
            memcpy(chunk->buffer + chunk->end_pos, data, copied);
            chunk->end_pos += copied;
            data += copied;
            remain -= copied;
        }
        if (chunk->end_pos == chunk->size) {
            chunk = on_chunk_full(s, chunk);
            int64_t empty = worker->end - (chunk->start + chunk->size);
            if (empty <= 0)     // destroy worker
                break;
            chunk = insert_new_chunk(s, chunk, -1, FFMIN(empty, s->disk_cache), worker);
            worker->current_chunk = chunk;
        }
    }
    if (worker->current_chunk)
        av_log(s, AV_LOG_DEBUG, "put_data_to_pool, worker <%"PRId64"-%"PRId64">, len: %lu, end pos: %"PRId64"\n",
            worker->start, worker->end, len, worker->current_chunk->start + worker->current_chunk->end_pos);
    return len - remain;
}

static size_t get_data_from_pool(MTSPContext *s, uint8_t *data, size_t len)
{
    av_log(s, AV_LOG_DEBUG, "get_data_from_pool, len: %lu, read pos: %"PRId64", current chunk: %p\n",
            len, s->read_pos, s->current_read_chunk);
    if (s->read_pos >= s->file_size)
        return AVERROR_EOF;
    if (!s->current_read_chunk) {
        on_buffer_empty(s);
        return 0;
    }
    Chunk *chunk = s->current_read_chunk;
    int64_t remain = len;
    while (remain) {
        int64_t avail = chunk->end_pos - chunk->read_pos;
        if (avail) {
            int64_t copied = FFMIN(avail, remain);
            if (chunk->buffer) {
                memcpy(data, chunk->buffer + chunk->read_pos, copied);
            } else {
                if (!s->fp) {
                    av_log(s, AV_LOG_ERROR, "file not opened, this shouldn't happen\n");
                    break;
                }
                fseek(s->fp, chunk->start + chunk->read_pos, SEEK_SET);
                size_t nread = fread(data, 1, copied, s->fp);
                if (nread != copied)
                    av_log(s, AV_LOG_WARNING, "failed to read chunk data from disk (%"PRIu64"-%"PRIu64")\n",
                        chunk->start + chunk->read_pos, chunk->start + chunk->read_pos + copied - 1);
                copied = nread;
            }
            chunk->read_pos += copied;
            data += copied;
            remain -= copied;
        }
        if (chunk->read_pos == chunk->end_pos) {
            if (chunk->end_pos != chunk->size)
                break;
            chunk = on_chunk_read(s, chunk);
            if (!chunk->next || chunk->next->start != chunk->start + chunk->size)
                break;
            chunk = chunk->next;
            chunk->read_pos = 0;
        }
    }
    s->current_read_chunk = chunk;
    s->read_pos = chunk->start + chunk->read_pos;
    return len - remain;
}

static void set_read_pos(MTSPContext *s, int64_t read_pos)
{
    av_log(s, AV_LOG_DEBUG, "set_read_pos, read pos: %"PRId64"\n", read_pos);
    s->seek_end_for_meta = 0;
    if (s->read_pos < 5 * 1024 * 1024 && read_pos > s->file_size - 100 * 1024) {
        s->seek_end_for_meta = 1;
        av_log(s, AV_LOG_WARNING, "seek end for meta detected\n");
    }
    s->read_pos = read_pos;

    Chunk *chunk = s->chunk_pool, *prev = NULL;
    while (chunk && chunk->start <= read_pos) {
        prev = chunk;
        chunk = chunk->next;
    }
    int64_t buffered_data = 0;
    if (prev) {
        int64_t diff = prev->start + prev->end_pos - read_pos;
        buffered_data = FFMAX(diff, 0);
    }
    if (buffered_data) {
        prev->read_pos = read_pos - prev->start;
        s->current_read_chunk = prev;
    } else {
        s->current_read_chunk = NULL;
        // destroy worker pool here may cause curl crash (thread sync issue)
        // move it to download thread
        s->last_range_len = 0;
    }
}

static size_t write_func(void *contents, size_t size, size_t nmemb, void *userp)
{
    CURLWriteData *data = userp;
    MTSPContext *s = data->context;
    size_t l = size * nmemb;

    long status_code;
    CURLcode res = curl_easy_getinfo(data->worker->curl_handle, CURLINFO_RESPONSE_CODE, &status_code);
    if (!res && status_code / 100 != 2) {
        av_log(s, AV_LOG_WARNING, "status code: %ld\n", status_code);
        return l;
    }

    pthread_mutex_lock(&s->mutex_data);
    size_t ret = put_data_to_pool(s, data->worker, contents, l);
    pthread_mutex_unlock(&s->mutex_data);
    return ret;
}

static int set_curl_opt(MTSPContext *s, CURL *curl_handle)
{
    curl_easy_setopt(curl_handle, CURLOPT_URL, s->url);
    curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L);
    if (s->user_agent)
        curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, s->user_agent);
    if (s->referer)
        curl_easy_setopt(curl_handle, CURLOPT_REFERER, s->referer);
    if (s->cookies)
        curl_easy_setopt(curl_handle, CURLOPT_COOKIE, s->cookies);
    if (s->headers) {
        char *header, *headers, *next;
        next = headers = av_strdup(s->headers);
        if (!next)
            return AVERROR(ENOMEM);
        struct curl_slist *chunk = NULL;
        while (header = av_strtok(next, "\r\n", &next))
            chunk = curl_slist_append(chunk, header);
        curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, chunk);
        av_free(headers);
    }
    if (s->post_data && s->post_data_len > 0) {
        curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDSIZE, (long)s->post_data_len);
        curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, s->post_data);
    }
    if (s->ssl_public_key)
        curl_easy_setopt(curl_handle, CURLOPT_PINNEDPUBLICKEY, s->ssl_public_key);
    return 0;
}

static int init_curl_handle(MTSPContext *s, Worker *worker)
{
    worker->curl_handle = curl_easy_init();

    CURLWriteData *data = av_malloc(sizeof(CURLWriteData));
    data->context = s;
    data->worker = worker;

    curl_easy_setopt(worker->curl_handle, CURLOPT_WRITEFUNCTION, write_func);
    curl_easy_setopt(worker->curl_handle, CURLOPT_WRITEDATA, data);
    curl_easy_setopt(worker->curl_handle, CURLOPT_HEADER, 0L);
    curl_easy_setopt(worker->curl_handle, CURLOPT_PRIVATE, data);
    // curl_easy_setopt(worker->curl_handle, CURLOPT_VERBOSE, 0L);
    // curl_easy_setopt(worker->curl_handle, CURLOPT_MAX_RECV_SPEED_LARGE, (curl_off_t)(10 * 1024));   // test throttle

    int ret = set_curl_opt(s, worker->curl_handle);
    if (ret < 0)
        return ret;

    if (worker->start >= 0) {
        char *range;
        if (worker->end > worker->start && worker->end < s->file_size)
            range = av_asprintf("%"PRIu64"-%"PRIu64, worker->start, worker->end - 1);
        else
            range = av_asprintf("%"PRIu64"-", worker->start);
        if (!range)
            return AVERROR(ENOMEM);
        curl_easy_setopt(worker->curl_handle, CURLOPT_RANGE, range);
        av_free(range);
    }

    curl_multi_add_handle(s->curl_multi_handle, worker->curl_handle);
    return 0;
}

static void destroy_curl_handle(MTSPContext *s, Worker *worker)
{
    CURLWriteData *data = NULL;
    CURLcode res = curl_easy_getinfo(worker->curl_handle, CURLINFO_PRIVATE, &data);
    if (res)
        av_log(s, AV_LOG_WARNING, "failed to get curl user data\n");
    else
        av_free(data);

    curl_multi_remove_handle(s->curl_multi_handle, worker->curl_handle);
    curl_easy_cleanup(worker->curl_handle);
    worker->curl_handle = NULL;
}

static size_t header_func(void *header, size_t size, size_t nmemb, void *userp)
{
    char *h = header;
    size_t l = size * nmemb;

    int i = l - 1;
    while (i >= 0 && (h[i] == '\r' || h[i] == '\n'))
        i--;
    if (i < 0)
        return l;

    h = av_strndup(header, i + 1);
    if (!h)
        return l;

    CURLHeaderData *data = userp;
    MTSPContext *s = data->context;
    av_log(s, AV_LOG_DEBUG, "response header: %s\n", h);

    char *p = h;
    while (*p != '\0' && *p != ':')
        p++;
    if (*p != ':')
        goto fail;

    long status_code;
    CURLcode res = curl_easy_getinfo(data->curl_handle, CURLINFO_RESPONSE_CODE, &status_code);
    if (!res && status_code / 100 != 2)
        goto fail;

    *p = '\0';
    p++;
    while (av_isspace(*p))
        p++;

    if (!av_strcasecmp(h, "Content-Disposition")) {
        char *param, *next_param = p;
        while ((param = av_strtok(next_param, ";", &next_param))) {
            char *name, *value;
            param += strspn(param, WHITESPACES);
            if ((name = av_strtok(param, "=", &value))) {
                if (!av_strcasecmp(name, "filename")) {
                    size_t len = strlen(value);
                    if (len > 2 && value[0] == '"' && value[len-1] == '"')
                        value++[len-1] = '\0';
                    char *utf8_name = ff_urldecode(value);
                    av_free(s->file_name);
                    s->file_name = conv_file_name(utf8_name);
                    av_free(utf8_name);
                    av_log(s, AV_LOG_DEBUG, "file name: %s\n", s->file_name);
                    break;
                }
            }
        }
    } else if (!av_strcasecmp(h, "Content-Type")) {
        av_free(s->mime_type);
        s->mime_type = av_strdup(p);
        av_log(s, AV_LOG_DEBUG, "mime type: %s\n", s->mime_type);
    } else if (!av_strcasecmp(h, "Content-Length")) {
        s->file_size = strtoull(p, NULL, 10);
        av_log(s, AV_LOG_DEBUG, "file size: %"PRId64"\n", s->file_size);
    } else if (!av_strcasecmp(h, "Content-MD5")) {
        av_free(s->file_md5);
        s->file_md5 = av_strdup(p);
    }

fail:
    av_free(h);
    return l;
}

static int get_file_info(MTSPContext *s)
{
    av_log(s, AV_LOG_DEBUG, "get_file_info\n");
    CURL *curl_handle = curl_easy_init();
    CURLHeaderData data = { .context = s, .curl_handle = curl_handle };

    // curl_easy_setopt(curl_handle, CURLOPT_HEADER, 1);
    curl_easy_setopt(curl_handle, CURLOPT_NOBODY, 1);
    curl_easy_setopt(curl_handle, CURLOPT_HEADERFUNCTION, header_func);
    curl_easy_setopt(curl_handle, CURLOPT_HEADERDATA, &data);

    int ret = set_curl_opt(s, curl_handle);
    if (ret < 0)
        return ret;

    curl_easy_perform(curl_handle);

    // we already got file_size by parse Content-Length header
    // double filesize = 0.0;
    // CURLcode ret = curl_easy_getinfo(curl_handle, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &filesize);
    curl_easy_cleanup(curl_handle);

    if (!s->file_size) {
        av_log(s, AV_LOG_ERROR, "get file size failed\n");
        return -1;
    }
    if (s->file_md5 && s->md5_blacklist && av_stristr(s->md5_blacklist, s->file_md5)) {
        av_log(s, AV_LOG_ERROR, "file md5 in blacklist\n");
        return -2;
    }
    return 0;
}

static void calc_speed(MTSPContext *s, double interval, int print)
{
    int total_worker = 0, low_speed_worker = 0, active_worker = 0;
    Worker *worker = s->worker_pool;
    while (worker) {
        double total_time, start_time;
        curl_off_t size, avg_speed;
        CURLcode res = curl_easy_getinfo(worker->curl_handle, CURLINFO_TOTAL_TIME, &total_time);
        res = res || curl_easy_getinfo(worker->curl_handle, CURLINFO_STARTTRANSFER_TIME, &start_time);
        res = res || curl_easy_getinfo(worker->curl_handle, CURLINFO_SIZE_DOWNLOAD_T, &size);
        res = res || curl_easy_getinfo(worker->curl_handle, CURLINFO_SPEED_DOWNLOAD_T, &avg_speed);
        if (res)
            av_log(s, AV_LOG_WARNING, "failed to get curl info\n");
        else {
            double temp_speed = (size - worker->downloaded_size) / interval;
            double time_spent = total_time - start_time;
            if (print)
                av_log(s, AV_LOG_INFO, "worker <%6"PRId64"-%6"PRId64">\t"
                        "temp speed: %7.1fkB/s, avg speed: %7.1fkB/s, time: %5.1fs, end pos: %"PRId64"\n",
                        worker->start, worker->end, temp_speed / 1024.0, avg_speed / 1024.0, time_spent,
                        worker->current_chunk ? worker->current_chunk->start + worker->current_chunk->end_pos : worker->end);
            worker->downloaded_size = size;
        }
        total_worker++;
        if (!worker->next_reconnect)
            active_worker++;
        worker = worker->prev;
    }
    int64_t total_size = 0;
    Chunk *chunk = s->chunk_pool;
    while (chunk) {
        total_size += chunk->end_pos;
        chunk = chunk->next;
    }
    double temp_speed = (total_size - s->downloaded_size) / interval;
    if (print)
        av_log(s, AV_LOG_INFO, "total worker: %d, active worker: %d, total speed: %7.1fkB/s\n",
                total_worker, active_worker, temp_speed / 1024.0);
    s->downloaded_size = total_size;
}

static int is_download_complete(MTSPContext *s)
{
    Chunk *chunk = s->chunk_pool;
    if (!chunk || chunk->start != 0)
        return 0;
    while (chunk->next && chunk->next->start == chunk->start + chunk->size)
        chunk = chunk->next;
    if (chunk->start + chunk->size != s->file_size)
        return 0;
    return 1;
}

static int check_md5(MTSPContext *s)
{
    av_log(s, AV_LOG_DEBUG, "check_md5\n");
    if (!s->file_md5)
        return -1;
    if (!s->fp)
        return -2;
    if (!is_download_complete(s))
        return -3;

    struct AVMD5 *md5 = av_md5_alloc();
    if (!md5)
        return AVERROR(ENOMEM);

    av_md5_init(md5);

    fseek(s->fp, 0, SEEK_SET);
    uint8_t *buf = av_malloc(MD5_BLOCK_SIZE);
    int nread;
    while (nread = fread(buf, 1, MD5_BLOCK_SIZE, s->fp))
        av_md5_update(md5, buf, nread);
    av_free(buf);

    uint8_t result[16];
    av_md5_final(md5, result);

    char str_result[33] = {'\0'};
    for (int i = 0; i < 16; i++)
        sprintf(str_result + i * 2, "%02x", result[i]);

    if (strcmp(str_result, s->file_md5)) {
        av_log(s, AV_LOG_ERROR, "md5 mismatch, server: %s, local: %s\n", s->file_md5, str_result);
        return -4;
    }
    av_log(s, AV_LOG_INFO, "md5 match\n");
    return 0;
}

static void merge_chunk_pool(MTSPContext *s)
{
    int written = 0;
    Chunk *chunk = s->chunk_pool;
    while (chunk) {
        while (merge_next_chunk(s, chunk) == 0);
        if (chunk != s->current_read_chunk && chunk->end_pos == chunk->size && chunk->buffer) {
            int prev_written = (chunk->prev && !chunk->prev->buffer &&
                                chunk->prev->start + chunk->prev->size == chunk->start) ||
                               (!chunk->prev && !chunk->start);
            int next_written = (chunk->next && !chunk->next->buffer &&
                                chunk->start + chunk->size == chunk->next->start) ||
                               (!chunk->next && chunk->start + chunk->size == s->file_size);
            if (chunk->size >= s->disk_cache || (prev_written && next_written))
                if (write_chunk_to_disk(s, chunk, 1) == 0)
                    written = 1;
        }
        chunk = chunk->next;
    }
    if (written) {
        while (chunk) {
            while (merge_next_chunk(s, chunk) == 0);
            chunk = chunk->next;
        }
        save_progress_to_file(s);
    }
}

static void *download_task(void *arg)
{
    MTSPContext *s = arg;

    int ret = 0;
    CURLMsg *msg;
    long curl_timeout;
    int maxfd, msgs_left, running_handles = -1;
    fd_set fdread, fdwrite, fdexcep;
    struct timeval timeout;
    int64_t start, end;
    int64_t last_update_speed = 0;
    int64_t last_merge_chunk = 0;

    curl_global_init(CURL_GLOBAL_ALL);

    if (!s->file_name || !s->file_size) {
        ret = get_file_info(s);
        if (ret < 0)
            goto fail;
    }

    if (!s->dont_write_disk) {
        int exists = 0;
        ret = open_local_file(s, &exists);
        if (ret < 0)
            s->dont_write_disk = 1;
        else if (exists)
            load_progress_from_file(s);
    }

    s->curl_multi_handle = curl_multi_init();
    // curl_multi_setopt(s->curl_multi_handle, CURLMOPT_MAXCONNECTS, (long)s->max_conn);

    pthread_mutex_lock(&s->mutex_init);
    s->exit_code = 0;
    s->init_done = 1;
    pthread_cond_signal(&s->cond_init_done);
    pthread_mutex_unlock(&s->mutex_init);

    while (!s->abort_download) {
        while (1) {
            pthread_mutex_lock(&s->mutex_data);
            int ret = pick_next_range(s, &start, &end);
            pthread_mutex_unlock(&s->mutex_data);
            if (ret)
                break;
            pthread_mutex_lock(&s->mutex_data);
            create_worker(s, start, end);
            pthread_mutex_unlock(&s->mutex_data);
        }
        // if (!s->running_workers)
        //     break;

        curl_multi_perform(s->curl_multi_handle, &running_handles);

        if (running_handles) {
            FD_ZERO(&fdread);
            FD_ZERO(&fdwrite);
            FD_ZERO(&fdexcep);

            if (ret = curl_multi_fdset(s->curl_multi_handle, &fdread, &fdwrite, &fdexcep, &maxfd)) {
                av_log(s, AV_LOG_ERROR, "E: curl_multi_fdset\n");
                goto sleep;
            }

            if (ret = curl_multi_timeout(s->curl_multi_handle, &curl_timeout)) {
                av_log(s, AV_LOG_ERROR, "E: curl_multi_timeout\n");
                goto sleep;
            }
            if (curl_timeout == -1)
                curl_timeout = 100;

            if (maxfd == -1) {
                av_usleep(curl_timeout * 1000);
            } else {
                timeout.tv_sec = curl_timeout / 1000;
                timeout.tv_usec = (curl_timeout % 1000) * 1000;

                if ((ret = select(maxfd + 1, &fdread, &fdwrite, &fdexcep, &timeout)) < 0)
                    av_log(s, AV_LOG_ERROR, "E: select(%i,,,,%li): %i: %s\n",
                            maxfd + 1, curl_timeout, errno, strerror(errno));
            }
            goto next;
        }
sleep:
        av_usleep(100 * 1000);
next:
        while ((msg = curl_multi_info_read(s->curl_multi_handle, &msgs_left))) {
            CURLWriteData *data = NULL;
            CURLcode res = curl_easy_getinfo(msg->easy_handle, CURLINFO_PRIVATE, &data);
            if (res) {
                av_log(s, AV_LOG_ERROR, "failed to get curl user data\n");
                continue;
            }
            char *worker_str = av_asprintf("worker <%"PRIu64"-%"PRIu64">", data->worker->start,
                                           data->worker->end);
            if (msg->msg == CURLMSG_DONE)
                av_log(s, AV_LOG_INFO, "R: %s %d - %s\n", worker_str, msg->data.result,
                       curl_easy_strerror(msg->data.result));
            else
                av_log(s, AV_LOG_ERROR, "E: %s CURLMsg (%d)\n", worker_str, msg->msg);
            av_free(worker_str);

            pthread_mutex_lock(&s->mutex_data);
            if (!data->worker->current_chunk)
                on_worker_done(s, data->worker);
            else
                on_worker_fail(s, data->worker);
            pthread_mutex_unlock(&s->mutex_data);
        }

        int64_t now = av_gettime_relative();

        if (!last_merge_chunk)
            last_merge_chunk = now;
        else if (now >= last_merge_chunk + 5000000) {
            pthread_mutex_lock(&s->mutex_data);
            merge_chunk_pool(s);
            pthread_mutex_unlock(&s->mutex_data);
            last_merge_chunk = now;
        }

        pthread_mutex_lock(&s->mutex_data);
        Worker *worker = s->worker_pool;
        while (worker) {
            if (worker->next_reconnect && now >= worker->next_reconnect)
                worker_reconnect(s, worker);
            worker = worker->prev;
        }
        pthread_mutex_unlock(&s->mutex_data);

        int64_t interval = s->update_speed_interval * 1000000;
        if (!last_update_speed)
            last_update_speed = now;
        else if (now >= last_update_speed + interval) {
            pthread_mutex_lock(&s->mutex_data);
            calc_speed(s, (now - last_update_speed) / 1000000.0, 1);
            pthread_mutex_unlock(&s->mutex_data);
            last_update_speed = now;
        }

        pthread_mutex_lock(&s->mutex_data);
        if (s->read_pos && !s->current_read_chunk && !s->seek_end_for_meta)
            destroy_worker_pool(s);
        check_buffer_len(s);
        pthread_mutex_unlock(&s->mutex_data);
    }

fail:
    pthread_mutex_lock(&s->mutex_data);
    destroy_worker_pool(s);
    flush_chunk_pool(s);
    // check_md5(s);
    destroy_chunk_pool(s);
    close_local_file(s);
    pthread_mutex_unlock(&s->mutex_data);

    curl_multi_cleanup(s->curl_multi_handle);
    s->curl_multi_handle = NULL;
    curl_global_cleanup();

    av_freep(&s->file_name);
    av_freep(&s->file_md5);
    av_freep(&s->mime_type);
    av_freep(&s->progress_file_name);

    pthread_mutex_lock(&s->mutex_init);
    s->exit_code = ret;
    if (!s->init_done) {
        s->init_done = 1;
        pthread_cond_signal(&s->cond_init_done);
    }
    pthread_mutex_unlock(&s->mutex_init);
    return &s->exit_code;
}

static int parse_url(MTSPContext *s)
{
    char *url = av_strdup(s->url);
    char *p;
    char proto[10];
    char real_url[MAX_URL_SIZE];
    int ret = 0;

    if ((p = strchr(url, ':'))) {
        av_strlcpy(proto, url, FFMIN(sizeof(proto), p + 1 - url));
        if (!strcmp(proto, "http") || !strcmp(proto, "https"))
            goto exit;
        p++; /* skip ':' */
        if (*p == '/')
            p++;
        if (*p == '/')
            p++;
    } else {
        p = url;
    }

    char *q = strchr(p, '?');
    if (q) {
        *q++ = 0;
        char *param, *next_param = q;
        while ((param = av_strtok(next_param, "&", &next_param))) {
            char *name, *value;
            if ((name = av_strtok(param, "=", &value))) {
                char *decoded_value = ff_urldecode(value);
                av_log(s, AV_LOG_INFO, "set option from url, -%s \"%s\"\n", name, decoded_value);
                int ret = av_opt_set(s, name, decoded_value, 0);
                if (ret < 0)
                    av_log(s, AV_LOG_WARNING, "failed to set option from url params, err: %d\n", ret);
                av_free(decoded_value);
            }
        }
    }

    ret = av_base64_decode(real_url, p, MAX_URL_SIZE);
    if (ret < 0) {
        ret = AVERROR(EINVAL);
        goto exit;
    }

    if (!av_strstart(real_url, "http://", NULL) &&
        !av_strstart(real_url, "https://", NULL)) {

        struct AVRC4 *rc4 = av_rc4_alloc();
        if (!rc4) {
            ret = AVERROR(ENOMEM);
            goto exit;
        }
        av_rc4_init(rc4, MTSP_RC4_KEY, 128, 1);
        av_rc4_crypt(rc4, real_url, real_url, ret, NULL, 1);
        av_free(rc4);

        if (!av_strstart(real_url, "http://", NULL) &&
            !av_strstart(real_url, "https://", NULL)) {
            ret = AVERROR(EINVAL);
            goto exit;
        }
    }
    
    av_strlcpy(s->url, real_url, ret + 1);
    av_log(s, AV_LOG_INFO, "parse_url, real url: %s\n", s->url);
exit:
    av_free(url);
    return ret;
}

static void append_to_list(char **list, char *items)
{
    if (!*list)
        *list = items;
    else {
        int len = strlen(*list) + 1 + strlen(items) + 1;
        if (av_reallocp(list, len) == 0)
            av_strlcatf(*list, len, ";%s", items);
        av_free(items);
    }
}

static void load_preset(MTSPContext *s)
{
    if (!s->preset || !strcmp(s->preset, "default")) {
        if (!s->user_agent)
            s->user_agent = av_strdup(DEFAULT_USER_AGENT);
    } else if (!strcmp(s->preset, "pcs")) {
        if (!s->user_agent)
            s->user_agent = pcs_get_user_agent(PCS_WIN_UA);
        if (!s->referer)
            s->referer = pcs_get_referer();
        // set ssl_public_key to prevent packet capturing
        // append_to_list(&s->ssl_public_key, pcs_get_ssl_public_key());
        append_to_list(&s->md5_blacklist, pcs_get_md5_blacklist());
    }
}

static int mtsp_open(URLContext *h, const char *uri, int flags,
                     AVDictionary **options)
{
    // for debug
    av_log_set_level(AV_LOG_DEBUG);

    av_log(h, AV_LOG_INFO, "mtsp_open, uri: %s\n", uri);
    MTSPContext *s = h->priv_data;

    s->url = av_strdup(uri);
    if (!s->url)
        return AVERROR(ENOMEM);

    int ret = parse_url(s);
    if (ret < 0)
        goto fail;

    load_preset(s);

    ret = pthread_mutex_init(&s->mutex_init, NULL);
    if (ret) {
        av_log(h, AV_LOG_FATAL, "pthread_mutex_init failed, error: %s\n", av_err2str(ret));
        goto fail;
    }
    ret = pthread_cond_init(&s->cond_init_done, NULL);
    if (ret) {
        av_log(h, AV_LOG_FATAL, "pthread_cond_init failed, error: %s\n", av_err2str(ret));
        goto cond_init_done_fail;
    }
    ret = pthread_mutex_init(&s->mutex_data, NULL);
    if (ret) {
        av_log(h, AV_LOG_FATAL, "pthread_mutex_init failed, error: %s\n", av_err2str(ret));
        goto mutex_data_fail;
    }
    ret = pthread_cond_init(&s->cond_data_avail, NULL);
    if (ret) {
        av_log(h, AV_LOG_FATAL, "pthread_cond_init failed, error: %s\n", av_err2str(ret));
        goto cond_data_avail_fail;
    }
    ret = pthread_create(&s->download_thread, NULL, download_task, s);
    if (ret) {
        av_log(h, AV_LOG_FATAL, "pthread_create failed, error: %s\n", av_err2str(ret));
        goto thread_fail;
    }

    pthread_mutex_lock(&s->mutex_init);
    while (!s->init_done)
        pthread_cond_wait(&s->cond_init_done, &s->mutex_init);
    ret = s->exit_code;
    pthread_mutex_unlock(&s->mutex_init);

    if (ret) {
        int err = pthread_join(s->download_thread, NULL);
        if (err)
            av_log(h, AV_LOG_ERROR, "pthread_join failed, error: %s\n", av_err2str(err));
        goto thread_fail;
    }

    av_log(h, AV_LOG_DEBUG, "open url success\n");
    return 0;

thread_fail:
    pthread_cond_destroy(&s->cond_data_avail);
cond_data_avail_fail:
    pthread_mutex_destroy(&s->mutex_data);
mutex_data_fail:
    pthread_cond_destroy(&s->cond_init_done);
cond_init_done_fail:
    pthread_mutex_destroy(&s->mutex_init);
fail:
    av_freep(&s->url);
    return ret;
}

static int mtsp_close(URLContext *h)
{
    av_log(h, AV_LOG_INFO, "mtsp_close\n");
    MTSPContext *s = h->priv_data;

    s->abort_download = 1;
    int ret = pthread_join(s->download_thread, NULL);
    if (ret)
        av_log(h, AV_LOG_ERROR, "pthread_join failed, error: %s\n", av_err2str(ret));

    pthread_cond_destroy(&s->cond_data_avail);
    pthread_mutex_destroy(&s->mutex_data);
    pthread_cond_destroy(&s->cond_init_done);
    pthread_mutex_destroy(&s->mutex_init);
    av_freep(&s->url);
    return ret ? ret : s->exit_code;
}

static int wait_data_timeout(MTSPContext *s, int64_t timeout, AVIOInterruptCB *int_cb)
{
    int64_t wait_start = 0;
    while (1) {
        if (ff_check_interrupt(int_cb))
            return AVERROR_EXIT;
        int64_t t = av_gettime() + POLLING_TIME * 1000;
        struct timespec ts = { .tv_sec  =  t / 1000000,
                               .tv_nsec = (t % 1000000) * 1000 };
        int ret = pthread_cond_timedwait(&s->cond_data_avail, &s->mutex_data, &ts);
        if (ret != ETIMEDOUT)
            return ret;
        if (timeout > 0) {
            if (!wait_start)
                wait_start = av_gettime_relative();
            else if (av_gettime_relative() - wait_start > timeout)
                return AVERROR(ETIMEDOUT);
        }
    }
}

static int mtsp_read(URLContext *h, uint8_t *buf, int size)
{
    MTSPContext *s = h->priv_data;

    pthread_mutex_lock(&s->mutex_data);
    int ret = get_data_from_pool(s, buf, size);
    if (!ret && !(h->flags & AVIO_FLAG_NONBLOCK))
        if (!wait_data_timeout(s, h->rw_timeout, &h->interrupt_callback))
            ret = get_data_from_pool(s, buf, size);
    pthread_mutex_unlock(&s->mutex_data);
    return ret;
}

static int64_t mtsp_seek(URLContext *h, int64_t off, int whence)
{
    av_log(h, AV_LOG_INFO, "mtsp_seek, off: %"PRId64", whence: %d\n", off, whence);
    MTSPContext *s = h->priv_data;

    if (whence == AVSEEK_SIZE)
        return s->file_size;
    else if ((whence == SEEK_CUR && off == 0) ||
             (whence == SEEK_SET && off == s->read_pos))
        return s->read_pos;
    else if (!s->file_size && whence == SEEK_END)
        return AVERROR(ENOSYS);

    if (whence == SEEK_CUR)
        off += s->read_pos;
    else if (whence == SEEK_END)
        off += s->file_size;
    else if (whence != SEEK_SET)
        return AVERROR(EINVAL);
    if (off < 0)
        return AVERROR(EINVAL);

    pthread_mutex_lock(&s->mutex_data);
    set_read_pos(s, off);
    pthread_mutex_unlock(&s->mutex_data);

    return off;
}

static int mtsp_get_short_seek(URLContext *h)
{
    return CURL_MAX_WRITE_SIZE;
}

static const AVClass mtsp_context_class = {
    .class_name = "mtsp",
    .item_name  = av_default_item_name,
    .option     = options,
    .version    = LIBAVUTIL_VERSION_INT,
};

const URLProtocol ff_mtsp_protocol = {
    .name                = "mtsp",
    .url_open2           = mtsp_open,
    .url_read            = mtsp_read,
    .url_seek            = mtsp_seek,
    .url_close           = mtsp_close,
    .url_get_short_seek  = mtsp_get_short_seek,
    .priv_data_size      = sizeof(MTSPContext),
    .priv_data_class     = &mtsp_context_class,
    .flags               = URL_PROTOCOL_FLAG_NETWORK,
    .default_whitelist   = "mtsp"
};
