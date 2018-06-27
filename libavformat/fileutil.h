#ifndef AVFORMAT_FILEUTIL_H
#define AVFORMAT_FILEUTIL_H

#include <stdint.h>
#include <stdio.h>

int truncate_file(FILE *fp, int64_t length);
int allocate_file(FILE *fp, int64_t offset, int64_t length, int sparse);
char *temp_directory_path(void);
int create_directory(const char *path);
int is_writable(const char *path);
char *current_directory(void);
size_t file_size(FILE *fp);

#endif
