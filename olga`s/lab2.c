#define _CRT_SECURE_NO_WARNINGS
#include "../include/lab2.h"
#include <windows.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#define max_open_files 256
#define size_of_page 4096
#define size_of_cache 64

typedef struct CachePage {
    LARGE_INTEGER offset;
    size_t size;
    void *data;
    struct CachePage *next;
    int is_dirty;
    int is_referenced;
} CachePage;

typedef struct Cache {
    CachePage pages[size_of_cache];
    CachePage *hand;
    CRITICAL_SECTION lock;
} Cache;

typedef struct File {
    HANDLE handle;
    LARGE_INTEGER position;
    Cache cache;
} File;

static File *opened_files[max_open_files];
static CRITICAL_SECTION files_lock;
static File *get_file_by_descriptor(int fd);
static int add_file(File *file);
static void remove_file(int fd);
static void initialize_cache(Cache *cache);
static void destroy_cache(Cache *cache);
static CachePage *find_in_cache(Cache *cache, LARGE_INTEGER offset);
static CachePage* get_or_create_page(File *file, LARGE_INTEGER page_offset, size_t size);
static int flush_page(File *file, CachePage *page);
static int64_t seek_set(File *file, LARGE_INTEGER offset);
static int64_t seek_cur(File *file, LARGE_INTEGER offset);
static int64_t seek_end(File *file, LARGE_INTEGER offset);

static CachePage* clock_cache(Cache *cache) {
    CachePage *page = cache->hand;
    while (1) {
        if (page->is_referenced == 0) {
            cache->hand = page + 1;
            if (cache->hand == cache->pages + size_of_cache)
                cache->hand = cache->pages;
            return page;
        } else {
            page->is_referenced = 0;
            page++;
            if (page == cache->pages + size_of_cache)
                page = cache->pages;
            cache->hand = page;
        }
    }
}

static File *get_file_by_descriptor(int fd) {
    if (fd < 0 || fd >= max_open_files) {
        return NULL;
    }
    return opened_files[fd];
}

static int add_file(File *file) {
    EnterCriticalSection(&files_lock);
    for (int i = 0; i < max_open_files; i++) {
        if (!opened_files[i]) {
            opened_files[i] = file;
            LeaveCriticalSection(&files_lock);
            return i;
        }
    }
    LeaveCriticalSection(&files_lock);
    return -1;
}

static void remove_file(int fd) {
    EnterCriticalSection(&files_lock);
    if (fd >= 0 && fd < max_open_files) {
        opened_files[fd] = NULL;
    }
    LeaveCriticalSection(&files_lock);
}

static void initialize_cache(Cache *cache) {
    memset(cache->pages, 0, sizeof(cache->pages));
    cache->hand = &cache->pages[0];
    InitializeCriticalSection(&cache->lock);
}

static void destroy_cache(Cache *cache) {
    for (int i = 0; i < size_of_cache; i++) {
        if (cache->pages[i].data) {
            _aligned_free(cache->pages[i].data);
            cache->pages[i].data = NULL;
        }
    }
    DeleteCriticalSection(&cache->lock);
}

static CachePage *find_in_cache(Cache *cache, LARGE_INTEGER offset) {
    for (int i = 0; i < size_of_cache; i++) {
        if (cache->pages[i].data && cache->pages[i].offset.QuadPart == offset.QuadPart) {
            return &cache->pages[i];
        }
    }
    return NULL;
}


static CachePage* get_or_create_page(File *file, LARGE_INTEGER page_offset, size_t size) {
    CachePage *page = find_in_cache(&file->cache, page_offset);
    
    if (!page) {
        page = clock_cache(&file->cache);

        if (page->is_dirty) {
            if (flush_page(file, page) == -1) {
                return NULL;
            }
        }

        if (!page->data) {
            page->data = _aligned_malloc(size, size);
            if (!page->data) {
                return NULL;
            }
        }

        DWORD bytes_read;
        SetFilePointerEx(file->handle, page_offset, NULL, FILE_BEGIN);
        if (!ReadFile(file->handle, page->data, size, &bytes_read, NULL)) {
            return NULL;
        }

        if (bytes_read != size) {
            memset((char *)page->data + bytes_read, 0, size - bytes_read);
        }

        page->offset = page_offset;
        page->size = size;
        page->is_dirty = 0;
        page->is_referenced = 1;
    } else {
        page->is_referenced = 1;
    }
    
    return page;
}

static int flush_page(File *file, CachePage *page) {
    if (page->is_dirty) {
        DWORD bytes_written;
        LARGE_INTEGER offset = page->offset;
        SetFilePointerEx(file->handle, offset, NULL, FILE_BEGIN);
        if (!WriteFile(file->handle, page->data, size_of_page, &bytes_written, NULL) || bytes_written != size_of_page) {
            return -1;
        }
        page->is_dirty = 0;
    }
    return 0;
}

int lab2_open(const char *path) {
    InitializeCriticalSection(&files_lock);
    HANDLE handle = CreateFileA(path, GENERIC_READ | GENERIC_WRITE, 0, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
    if (handle == INVALID_HANDLE_VALUE) {
        DWORD error = GetLastError();
        return -1;
    }

    File *file = (File*)_aligned_malloc(sizeof(File), sizeof(void*));
    if (!file) {
        CloseHandle(handle);
        return -1;
    }

    file->handle = handle;
    file->position.QuadPart = 0;
    initialize_cache(&file->cache);
    int fd = add_file(file);

    if (fd == -1) {
        destroy_cache(&file->cache);
        CloseHandle(handle);
        _aligned_free(file);
        return -1;
    }

    return fd;
}

int lab2_close(int fd) {
    File *file = get_file_by_descriptor(fd);
    if (!file) {
        SetLastError(ERROR_INVALID_HANDLE);
        return -1;
    }

    lab2_fsync(fd);
    destroy_cache(&file->cache);
    CloseHandle(file->handle);
    remove_file(fd);
    _aligned_free(file);
    DeleteCriticalSection(&files_lock);
    return 0;
}

ssize_t lab2_read(int fd, void *buffer, size_t count) {
    File *file = get_file_by_descriptor(fd);
    if (!file) {
        SetLastError(ERROR_INVALID_HANDLE);
        return -1;
    }

    EnterCriticalSection(&file->cache.lock);

    size_t total_read = 0;
    size_t bytes_left = count;
    char *data = (char*)buffer;

    while (bytes_left > 0) {
        LARGE_INTEGER page_offset;
        page_offset.QuadPart = file->position.QuadPart / size_of_page * size_of_page; //округление вниз до ближайшего кратного
        size_t offset_in_page = file->position.QuadPart % size_of_page;
        size_t bytes_from_page = size_of_page - offset_in_page;

        if (bytes_from_page > bytes_left)
            bytes_from_page = bytes_left;

        CachePage *page = get_or_create_page(file, page_offset, size_of_page);

        if (!page) {
            LeaveCriticalSection(&file->cache.lock);
            return -1;
        }

        memcpy(data, (char *)page->data + offset_in_page, bytes_from_page);

        data += bytes_from_page;
        file->position.QuadPart += bytes_from_page;
        total_read += bytes_from_page;
        bytes_left -= bytes_from_page;
    }

    LeaveCriticalSection(&file->cache.lock);
    return total_read;
}

ssize_t lab2_write(int fd, const void *buffer, size_t count) {
    EnterCriticalSection(&files_lock);
    File *file = get_file_by_descriptor(fd);
    if (!file) {
        LeaveCriticalSection(&files_lock);
        SetLastError(ERROR_INVALID_HANDLE);
        return -1;
    }
    LeaveCriticalSection(&files_lock);

    EnterCriticalSection(&file->cache.lock);

    size_t total_written = 0;
    size_t bytes_left = count;
    const char *data = (const char*)buffer;

    while (bytes_left > 0) {
        LARGE_INTEGER page_offset;
        page_offset.QuadPart = (file->position.QuadPart / size_of_page) * size_of_page;
        size_t offset_in_page = file->position.QuadPart % size_of_page;
        size_t bytes_to_write = size_of_page - offset_in_page;
        if (bytes_to_write > bytes_left)
            bytes_to_write = bytes_left;

        CachePage *page = get_or_create_page(file, page_offset, size_of_page);

        if (!page) {
            LeaveCriticalSection(&file->cache.lock);
            return -1;
        }

        memcpy((char*)page->data + offset_in_page, data, bytes_to_write);
        page->is_dirty = 1;

        data += bytes_to_write;
        file->position.QuadPart += bytes_to_write;
        total_written += bytes_to_write;
        bytes_left -= bytes_to_write;
    }

    LeaveCriticalSection(&file->cache.lock);
    return total_written;
}

int64_t lab2_lseek(int fd, int64_t offset, int whence) {
    EnterCriticalSection(&files_lock);
    File *file = get_file_by_descriptor(fd);
    if (!file) {
        LeaveCriticalSection(&files_lock);
        SetLastError(ERROR_INVALID_HANDLE);
        return -1;
    }
    LeaveCriticalSection(&files_lock);

    LARGE_INTEGER new_pos;
    LARGE_INTEGER offset_li;
    offset_li.QuadPart = offset;

    if (whence == SEEK_SET) {
        return seek_set(file, offset_li);
    } else if (whence == SEEK_CUR) {
        return seek_cur(file, offset_li);
    } else if (whence == SEEK_END) {
        return seek_end(file, offset_li);
    }

    return -1;
}

static int64_t seek_set(File *file, LARGE_INTEGER offset) {
    LARGE_INTEGER new_pos;
    if (!SetFilePointerEx(file->handle, offset, &new_pos, FILE_BEGIN)) {
        return -1;
    }
    file->position = new_pos;
    return new_pos.QuadPart;
}

static int64_t seek_cur(File *file, LARGE_INTEGER offset) {
    LARGE_INTEGER new_pos;
    offset.QuadPart = file->position.QuadPart + offset.QuadPart;
    if (!SetFilePointerEx(file->handle, offset, &new_pos, FILE_BEGIN)) {
        return -1;
    }
    file->position = new_pos;
    return new_pos.QuadPart;
}

static int64_t seek_end(File *file, LARGE_INTEGER offset) {
    LARGE_INTEGER new_pos;
    LARGE_INTEGER file_size;
    if (!GetFileSizeEx(file->handle, &file_size)) {
        return -1;
    }
    offset.QuadPart = file_size.QuadPart + offset.QuadPart;
    if (!SetFilePointerEx(file->handle, offset, &new_pos, FILE_BEGIN)) {
        return -1;
    }
    file->position = new_pos;
    return new_pos.QuadPart;
}

int lab2_fsync(int fd) {
    EnterCriticalSection(&files_lock);
    File *file = get_file_by_descriptor(fd);
    if (!file) {
        LeaveCriticalSection(&files_lock);
        SetLastError(ERROR_INVALID_HANDLE);
        return -1;
    }
    LeaveCriticalSection(&files_lock);

    EnterCriticalSection(&file->cache.lock);

    for (int i = 0; i < size_of_cache; i++) {
        CachePage *page = &file->cache.pages[i];
        if (page->data && page->is_dirty) {
            if (flush_page(file, page) == -1) {
                LeaveCriticalSection(&file->cache.lock);
                return -1;
            }
        }
    }

    LeaveCriticalSection(&file->cache.lock);
    return 0;
}
