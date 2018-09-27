#include "fileutil.h"

#include "os_support.h"
#include "libavutil/common.h"
#include "libavutil/log.h"

#ifdef _WIN32
    #include <windows.h>
    #include <io.h>
    #include <direct.h>
#else
#if defined(__linux__)
    #include <linux/limits.h>
#elif defined(__APPLE__)
    #include <sys/syslimits.h>
    #include <fcntl.h>
#else
    #include <limits.h>
#endif
    #include <unistd.h>
    #include <sys/types.h>
    #include <sys/stat.h>
#endif

#ifdef _WIN32
    typedef HANDLE FD;
#else
    typedef int FD;
#endif

static inline FD file_no(FILE *fp)
{
#ifdef _WIN32
    return (HANDLE)_get_osfhandle(_fileno(fp));
#else
    return fileno(fp);
#endif
}

static inline int last_error()
{
#ifdef _WIN32
    return GetLastError();
#else
    return errno;
#endif
}

int truncate_file(FILE *fp, int64_t length)
{
    FD fd = file_no(fp);
#ifdef _WIN32
    // Since mingw32's ftruncate cannot handle over 2GB files, we use
    // SetEndOfFile instead.
    seek(length);
    if (SetEndOfFile(fd) == 0)
#else
    if (ftruncate(fd, length) == -1)
#endif
    {
        int err = last_error();
        av_log(NULL, AV_LOG_ERROR, "File truncation failed. errno: %d\n", err);
        return AVERROR(err);
    }
    return 0;
}

int allocate_file(FILE *fp, int64_t offset, int64_t length, int sparse)
{
    FD fd = file_no(fp);
    if (sparse) {
#ifdef _WIN32
        DWORD bytesReturned;
        if (!DeviceIoControl(fd, FSCTL_SET_SPARSE, 0, 0, 0, 0, &bytesReturned, 0)) {
            av_log(NULL, AV_LOG_WARNING, "Making file sparse failed or pending: %d\n", last_error());
        }
#endif
        int ret = truncate_file(fp, offset + length);
        if (ret != 0) {
            return ret;
        }
        return 0;
    }
#ifdef _WIN32
    int ret = truncate_file(fp, offset + length);
    if (ret != 0) {
        return ret;
    }
    if (!SetFileValidData(fd, offset + length)) {
        av_log(NULL, AV_LOG_WARNING,
                "File allocation (SetFileValidData) failed (errno: %d). File will be "
                "allocated by filling zero, which blocks whole execution. Run "
                "as an administrator or use a different file allocation method.\n",
                last_error());
    }
#elif defined(__APPLE__) && defined(__MACH__)
    int64_t toalloc = offset + length - file_size(fp);
    fstore_t fstore = { F_ALLOCATECONTIG | F_ALLOCATEALL, F_PEOFPOSMODE, 0, toalloc, 0 };
    if (fcntl(fd, F_PREALLOCATE, &fstore) == -1) {
        // Retry non-contig.
        fstore.fst_flags = F_ALLOCATEALL;
        if (fcntl(fd, F_PREALLOCATE, &fstore) == -1) {
            int err = last_error();
            av_log(NULL, AV_LOG_ERROR, "fcntl(F_PREALLOCATE) of %" PRId64 " failed. errno: %d\n",
                                     fstore.fst_length, err);
            return AVERROR(err);
        }
    }
    // This forces the allocation on disk.
    ftruncate(fd, offset + length);
#elif __linux__
    // For linux, we use fallocate to detect file system supports
    // fallocate or not.
    int r;
    while ((r = fallocate(fd, 0, offset, length)) == -1 && errno == EINTR)
        ;
    if (r == -1) {
        int err = last_error();
        av_log(NULL, AV_LOG_ERROR, "fallocate failed. errno: %d\n", err);
        return AVERROR(err);
    }
#elif defined(_POSIX_VERSION)
    int r = posix_fallocate(fd, offset, length);
    if (r != 0) {
        int err = last_error();
        av_log(NULL, AV_LOG_ERROR, "posix_fallocate failed. errno: %d\n", err);
        return AVERROR(err);
    }
#else
#error "no *_fallocate function available."
#endif
    return 0;
}

char *temp_directory_path(void)
{
    const char *val = NULL;
    (val = getenv("TMPDIR" )) ||
    (val = getenv("TMP"    )) ||
    (val = getenv("TEMP"   )) ||
    (val = getenv("TEMPDIR"));
    if (val)
        return av_strdup(val);
#ifdef _WIN32
    char buf[MAX_PATH];
    if (GetTempPathA(MAX_PATH, buf) != 0)
        return av_strdup(buf);
    return NULL;
#else
#ifdef __ANDROID__
    const char* default_tmp = "/data/local/tmp";
#else
    const char* default_tmp = "/tmp";
#endif
    return av_strdup(default_tmp);
#endif
}

int create_directory(const char *path)
{
#if defined(_WIN32)
    int err = _mkdir(path);
#else 
    mode_t mode = 0755;
    int err = mkdir(path, mode);
#endif
    if (err != 0 && errno != EEXIST) {
        av_log(NULL, AV_LOG_ERROR, "create dir failed. errno: %d\n", errno);
        return AVERROR(errno);
    }
    return 0;
}

int is_writable(const char *path)
{
#if defined(_WIN32)
    int err = _access(path, 2)
#else 
    int err = access(path, W_OK);
#endif
    if (err != 0)
        return 0;
    return 1;
}

char *current_directory(void)
{
#if defined(_WIN32)
    char buf[MAX_PATH];
    char *cwd = _getcwd(buf, MAX_PATH);
#else
    char buf[PATH_MAX];
    char *cwd = getcwd(buf, PATH_MAX);
#endif
    if (!cwd) {
        av_log(NULL, AV_LOG_ERROR, "get current directory failed. errno: %d\n", errno);
        return NULL;
    }
    return av_strdup(cwd);
}

size_t file_size(FILE *fp)
{
    size_t prev = ftell(fp);
    fseek(fp, 0L, SEEK_END);
    size_t size = ftell(fp);
    fseek(fp, prev, SEEK_SET);
    return size;
}

char *conv_file_name(char *utf8_name)
{
#if defined(_WIN32)
    int len = ::MultiByteToWideChar(CP_UTF8, 0, utf8_name, -1, NULL, 0);
    wchar_t *unicode = av_mallocz((len + 1) * sizeof(wchar_t));
    ::MultiByteToWideChar(CP_UTF8, 0, utf8_name, -1, unicode, len);

    len = ::WideCharToMultiByte(CP_ACP, 0, unicode, -1, NULL, 0, NULL, NULL);
    char *ansi_name = av_mallocz((len + 1) * sizeof(char));
    ::WideCharToMultiByte(CP_ACP, 0, unicode, -1, ansi_name, len, NULL, NULL);  
  
    av_free(unicode);
    return ansi_name;
#else
    return av_strdup(utf8_name);
#endif
}

int remove_file(const char *path)
{
#if defined(_WIN32)
    int err = _unlink(path);
#else 
    int err = unlink(path);
#endif
    if (err != 0 && errno != ENOENT) {
        av_log(NULL, AV_LOG_ERROR, "remove file failed. errno: %d\n", errno);
        return AVERROR(errno);
    }
    return 0;
}
