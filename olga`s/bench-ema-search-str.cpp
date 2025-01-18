#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "../include/lab2.h"

#define CHUNK_SIZE (4 * 1024 * 1024)
void kmp_search(const char *pattern, const char *text, size_t offset);

void build_prefix_function(const char *pattern, int *prefix) {
    int m = strlen(pattern);
    int j = 0;
    for (int i = 1; i < m; ++i) {
        while (j > 0 && pattern[i] != pattern[j]) {
            j = prefix[j - 1];
        }
        if (pattern[i] == pattern[j]) {
            ++j;
        }
        prefix[i] = j;
    }
}

void kmp_search(const char *pattern, const char *text, size_t offset) {
    int prefix[strlen(pattern)];
    build_prefix_function(pattern, prefix);
    int m = strlen(pattern);
    int n = strlen(text);
    int j = 0;

    for (int i = 0; i < n; ++i) {
        while (j > 0 && text[i] != pattern[j]) {
            j = prefix[j - 1];
        }
        if (text[i] == pattern[j]) {
            ++j;
        }
        if (j == m) {
            printf("Found a match at: %zu\n", i - m + 1 + offset);
            j = prefix[j - 1];
        }
    }
}

void process_file_chunk(const char *pattern, int fd, size_t chunk_size, int iter, size_t *total_read_bytes) {
    char *chunk = (char*)malloc(chunk_size);
    if (!chunk) {
        perror("Failed to allocate memory.");
        return;
    }

    size_t file_offset = 0;
    ssize_t bytes_read;
    int iteration = 0;

    while (iteration < iter) {
        bytes_read = lab2_read(fd, chunk, chunk_size);
        if (bytes_read <= 0) {
            lab2_lseek(fd, 0, SEEK_SET);
            file_offset = 0;
            break;
        }

        *total_read_bytes += bytes_read;

        char *buffer = (char*)malloc(bytes_read);
        if (!buffer) {
            perror("Failed to allocate memory for buffer.");
            free(chunk);
            return;
        }

        memcpy(buffer, chunk, bytes_read);

        kmp_search(pattern, buffer, file_offset);

        file_offset += bytes_read;
        free(buffer);

        iteration++;
    }

    free(chunk);
}

void search_substring_in_file(const char *filename, const char *pattern, int iter) {
    clock_t begin_time = clock();

    int fd = lab2_open(filename);
    if (fd == -1) {
        perror("Failed to open file.");
        return;
    }
    size_t total_read_bytes = 0;

    for (int rep = 0; rep < iter; ++rep) {
        lab2_lseek(fd, 0, SEEK_SET);
        process_file_chunk(pattern, fd, CHUNK_SIZE, iter, &total_read_bytes);
    }
    lab2_close(fd);

    clock_t finish_time = clock();
    double elapsed_time = (double)(finish_time - begin_time) / CLOCKS_PER_SEC;
    printf("Duration: %.2f seconds\n", elapsed_time);
}

int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <filename> <pattern> <repeat>\n", argv[0]);
        return 1;
    }

    const char *filename = argv[1];
    const char *pattern = argv[2];
    int iter = atoi(argv[3]);

    if (strlen(pattern) == 0) {
        fprintf(stderr, "Error: Pattern must not be empty.\n");
        return 1;
    }

    search_substring_in_file(filename, pattern, iter);

    return 0;
}
