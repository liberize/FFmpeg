#ifndef AVFORMAT_FILEUTIL_H
#define AVFORMAT_FILEUTIL_H

#include <stdint.h>
#include <stdio.h>

int av_truncate_file(FILE *fp, int64_t length);
int av_allocate_file(FILE *fp, int64_t offset, int64_t length, int sparse);
char *av_temp_directory_path(void);
int av_create_directory(const char *path);
int av_is_writable(const char *path);
char *av_current_directory(void);
size_t av_file_size(FILE *fp);
char *av_conv_file_name(char *utf8_name);
int av_remove_file(const char *path);

#endif
