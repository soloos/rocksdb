#pragma once

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <time.h>
#include <stdio.h>

#define LOG(fmt, args...) printf("%s:%d ", __FILE__, __LINE__); printf(fmt, ##args); printf("\n");

#ifndef O_RDONLY
#define O_RDONLY 1
#endif

#ifndef O_WRONLY
#define O_WRONLY 2
#endif

#ifndef EINTERNAL
#define EINTERNAL 255
#endif

typedef struct SdfsInternal_s {} SdfsInternal;
typedef struct SdfsFileInternal_s {} SdfsFileInternal;

typedef SdfsInternal *sdfsFS;
typedef uint64_t sdfsFile;
typedef uint16_t tPort;

typedef int32_t   tSize;
typedef time_t    tTime;
typedef int64_t   tOffset;
typedef uint16_t  tPort;
typedef enum tObjectKind {
	kObjectKindFile = 'F',
	kObjectKindDirectory = 'D',
} tObjectKind;

typedef struct DancestorFileInfo_s {
	tOffset mSize;
	tTime mLastMod;

} sdfsFileInfo;

sdfsFS sdfsConnectNewInstance(const char* nn, tPort port);
int sdfsDisconnect(sdfsFS fs);
int sdfsGetPathInfo(sdfsFS fs, const char* path, sdfsFileInfo* pFileInfo);

inline void sdfsFreeArr(void **ret, int num) {
  for (int i = 0; i < num; i++) {
    free(ret[i]);
  }
}

int sdfsCreateDirectory(sdfsFS fs, const char* path);
sdfsFile sdfsOpenFile(sdfsFS fs, const char* path, int flags,
                                int bufferSize, short replication, tSize blocksize);
void sdfsListDirectory(sdfsFS fs, const char* path, void** inodePathArr, int* numEntries);
tOffset sdfsTell(sdfsFS fs, sdfsFile file);
int sdfsSeek(sdfsFS fs, sdfsFile file, tOffset desiredPos);
tSize sdfsPread(sdfsFS fs, sdfsFile file, tOffset position,
                     void* buffer, tSize length);
tSize sdfsRead(sdfsFS fs, sdfsFile file, void* buffer, tSize length);
tSize sdfsWrite(sdfsFS fs, sdfsFile file, const void* buffer,
                     tSize length);
int sdfsExists(sdfsFS fs, const char* path);
int sdfsCopy(sdfsFS srcFS, const char* src, sdfsFS dstFS, const char* dst);
int sdfsDelete(sdfsFS fs, const char* path, int recursive);
int sdfsRename(sdfsFS fs, const char* oldPath, const char* newPath);
int sdfsCloseFile(sdfsFS fs, sdfsFile file);
int sdfsFlush(sdfsFS fs, sdfsFile file);
int sdfsHFlush(sdfsFS fs, sdfsFile file);
int sdfsHSync(sdfsFS fs, sdfsFile file);
