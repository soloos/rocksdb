#include "sdfs/env_sdfs.h"

sdfsFS sdfsConnectNewInstance(const char* nn, tPort port) {
    (void)nn;
    (void)port;
    LOG("sdfsConnectNewInstance");
    exit(0);
    return nullptr;
}

int sdfsDisconnect(sdfsFS fs) {
    (void)fs;
    LOG("sdfsDisconnect");
    exit(0);
    return 0;
}

int sdfsGetPathInfo(sdfsFS fs, const char* path, sdfsFileInfo* pFileInfo) {
    (void)fs;
    GoSdfsGetPathInfo_return ret = GoSdfsGetPathInfo(const_cast<char*>(path));
    if (ret.r3 != 0) {
        return ret.r3;
    }

    pFileInfo->mSize = ret.r1;
    pFileInfo->mLastMod = ret.r2;

    return 0;
}

int sdfsCreateDirectory(sdfsFS fs, const char* path) {
    (void)fs;
    int ret = GoSdfsCreateDirectory(const_cast<char*>(path));

    return ret;
}

void sdfsListDirectory(sdfsFS fs, const char* path, void** inodePathArr, int* numEntries) {
    (void)fs;
    GoSdfsListDirectory(const_cast<char*>(path), inodePathArr, numEntries);
}

sdfsFile sdfsOpenFile(sdfsFS fs, const char* path, int flags,
                      int bufferSize, short replication, tSize blocksize) {
    (void)fs;
    GoSdfsOpenFile_return ret = GoSdfsOpenFile(const_cast<char*>(path), flags, bufferSize, replication, blocksize);

    assert(ret.r1 == 0);
    return ret.r0;
}

tOffset sdfsTell(sdfsFS fs, sdfsFile file) {
    (void)fs;
    (void)file;
    LOG("sdfsTell");
    exit(0);
    return 0;
}

int sdfsSeek(sdfsFS fs, sdfsFile file, tOffset desiredPos) {
    (void)fs;
    (void)file;
    (void)desiredPos;
    LOG("sdfsSeek");
    exit(0);
    return 0;
}

tSize sdfsPread(sdfsFS fs, sdfsFile file, tOffset position,
                void* buffer, tSize length) {
    (void)fs;
    GoSdfsPread_return ret = GoSdfsPread(file, const_cast<void*>(buffer), length, position);
    if (ret.r1 != 0) {
        errno = ret.r1;
        return 0;
    }

    return ret.r0;
}

tSize sdfsRead(sdfsFS fs, sdfsFile file, void* buffer, tSize length) {
    (void)fs;
    GoSdfsRead_return ret = GoSdfsRead(file, const_cast<void*>(buffer), length);
    if (ret.r1 != 0) {
        errno = ret.r1;
        return 0;
    }

    return ret.r0;
}

tSize sdfsWrite(sdfsFS fs, sdfsFile file, const void* buffer,
                tSize length) {
    (void)fs;
    GoSdfsAppend_return ret = GoSdfsAppend(file, const_cast<void*>(buffer), length);
    if (ret.r1 != 0) {
        errno = ret.r1;
        return 0;
    }

    return ret.r0;
}

int sdfsExists(sdfsFS fs, const char* path) {
    (void)fs;
    int ret = GoSdfsExists(const_cast<char*>(path));

    if (ret == 0) {
        return SDFS_EXISTS;
    }
    return SDFS_DOESNT_EXIST;
}

int sdfsCopy(sdfsFS srcFS, const char* src, sdfsFS dstFS, const char* dst) {
    (void)srcFS;
    (void)src;
    (void)dstFS;
    (void)dst;
    LOG("sdfsCopy");
    exit(0);
    return 0;
}

int sdfsDelete(sdfsFS fs, const char* path, int recursive) {
    (void)fs;
    (void)path;
    (void)recursive;
    return GoSdfsDelete(const_cast<char*>(path), recursive);
}

int sdfsRename(sdfsFS fs, const char* oldPath, const char* newPath) {
    (void)fs;
    (void)oldPath;
    (void)newPath;
    return GoSdfsRename(const_cast<char*>(oldPath), const_cast<char*>(newPath));
}

int sdfsCloseFile(sdfsFS fs, sdfsFile file) {
    (void)fs;
    int ret = GoSdfsCloseFile(file);
    return ret;
}

int sdfsFlush(sdfsFS fs, sdfsFile file) {
    (void)fs;
    int ret = GoSdfsFlushFile(file);
    return ret;
}

int sdfsHFlush(sdfsFS fs, sdfsFile file) {
    (void)fs;
    int ret = GoSdfsHFlushINode(file);
    return ret;
}

int sdfsHSync(sdfsFS fs, sdfsFile file) {
    (void)fs;
    int ret = GoSdfsHSyncINode(file);
    return ret;
}
