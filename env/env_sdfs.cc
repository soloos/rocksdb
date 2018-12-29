
#include "rocksdb/env.h"
#include "sdfs/env_sdfs.h"

#ifdef USE_SDFS
#ifndef ROCKSDB_SDFS_FILE_C
#define ROCKSDB_SDFS_FILE_C

#include <algorithm>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <strings.h>
#include <iostream>
#include <sstream>
#include "rocksdb/status.h"
#include "util/string_util.h"
#include "util/logging.h"

namespace rocksdb {

namespace {

static Status IOError(const std::string& context, int err_number) {
    return (err_number == ENOSPC) ?
        Status::NoSpace(context, strerror(err_number)) :
        Status::IOError(context, strerror(err_number));
}

static Logger* mylog = nullptr;

class SdfsReadableFile : virtual public SequentialFile,
    virtual public RandomAccessFile {
     private:
      sdfsFS fileSys_;
      std::string filename_;
      sdfsFile hfile_;

     public:
      SdfsReadableFile(sdfsFS fileSys, const std::string& fname)
          : fileSys_(fileSys), filename_(fname), hfile_(0) {
              ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsReadableFile opening file %s\n",
                              filename_.c_str());
              hfile_ = sdfsOpenFile(fileSys_, filename_.c_str(), O_RDONLY, 0, 0, 0);
              ROCKS_LOG_DEBUG(mylog,
                              "[sdfs] SdfsReadableFile opened file %s hfile_=0x%p\n",
                              filename_.c_str(), hfile_);
          }

      virtual ~SdfsReadableFile() {
          ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsReadableFile closing file %s\n",
                          filename_.c_str());
          sdfsCloseFile(fileSys_, hfile_);
          ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsReadableFile closed file %s\n",
                          filename_.c_str());
          hfile_ = 0;
      }

      bool isValid() {
          return hfile_ != 0;
      }

      virtual Status Read(size_t n, Slice* result, char* scratch) {
          Status s;
          ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsReadableFile reading %s %ld\n",
                          filename_.c_str(), n);

          char* buffer = scratch;
          size_t total_bytes_read = 0;
          tSize bytes_read = 0;
          tSize remaining_bytes = (tSize)n;

          while (remaining_bytes > 0) {
              bytes_read = sdfsRead(fileSys_, hfile_, buffer, remaining_bytes);
              if (bytes_read <= 0) {
                  break;
              }
              assert(bytes_read <= remaining_bytes);

              total_bytes_read += bytes_read;
              remaining_bytes -= bytes_read;
              buffer += bytes_read;
          }
          assert(total_bytes_read <= n);

          ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsReadableFile read %s\n",
                          filename_.c_str());

          if (bytes_read < 0) {
              s = IOError(filename_, errno);
          } else {
              *result = Slice(scratch, total_bytes_read);
          }

          return s;
      }

      virtual Status Read(uint64_t offset, size_t n, Slice* result,
                          char* scratch) const {
          Status s;
          ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsReadableFile preading %s\n",
                          filename_.c_str());
          ssize_t bytes_read = sdfsPread(fileSys_, hfile_, offset,
                                         (void*)scratch, (tSize)n);
          ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsReadableFile pread %s\n",
                          filename_.c_str());
          *result = Slice(scratch, (bytes_read < 0) ? 0 : bytes_read);
          if (bytes_read < 0) {

              s = IOError(filename_, errno);
          }
          return s;
      }

      virtual Status Skip(uint64_t n) {
          ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsReadableFile skip %s\n",
                          filename_.c_str());

          tOffset current = sdfsTell(fileSys_, hfile_);
          if (current < 0) {
              return IOError(filename_, errno);
          }

          tOffset newoffset = current + n;
          int val = sdfsSeek(fileSys_, hfile_, newoffset);
          if (val < 0) {
              return IOError(filename_, errno);
          }
          return Status::OK();
      }

     private:

      bool feof() {
          ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsReadableFile feof %s\n",
                          filename_.c_str());
          if (sdfsTell(fileSys_, hfile_) == fileSize()) {
              return true;
          }
          return false;
      }

      tOffset fileSize() {
          ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsReadableFile fileSize %s\n",
                          filename_.c_str());
          sdfsFileInfo pFileInfo;
          int code = sdfsGetPathInfo(fileSys_, filename_.c_str(), &pFileInfo);
          tOffset size = 0L;
          if (code != 0) {
              size = pFileInfo.mSize;
          } else {
              throw SdfsFatalException("fileSize on unknown file " + filename_);
          }
          return size;
      }
    };

class SdfsWritableFile: public WritableFile {
 private:
  sdfsFS fileSys_;
  std::string filename_;
  sdfsFile hfile_;

 public:
  SdfsWritableFile(sdfsFS fileSys, const std::string& fname)
      : fileSys_(fileSys), filename_(fname) , hfile_(0) {
          ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsWritableFile opening %s\n",
                          filename_.c_str());
          hfile_ = sdfsOpenFile(fileSys_, filename_.c_str(), O_WRONLY, 0, 0, 0);
          ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsWritableFile opened %s\n",
                          filename_.c_str());
          assert(hfile_ != 0);
      }
  virtual ~SdfsWritableFile() {
      if (hfile_ != 0) {
          ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsWritableFile closing %s\n",
                          filename_.c_str());
          sdfsCloseFile(fileSys_, hfile_);
          ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsWritableFile closed %s\n",
                          filename_.c_str());
          hfile_ = 0;
      }
  }

  bool isValid() {
      return !(hfile_ == 0);
  }

  const std::string& getName() {
      return filename_;
  }

  virtual Status Append(const Slice& data) {
      ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsWritableFile Append %s\n",
                      filename_.c_str());
      const char* src = data.data();
      size_t left = data.size();
      size_t ret = sdfsWrite(fileSys_, hfile_, src, left);
      ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsWritableFile Appended %s\n",
                      filename_.c_str());
      if (ret != left) {
          return IOError(filename_, errno);
      }
      return Status::OK();
  }

  virtual Status PositionedAppend(const Slice& data, uint64_t offset) {
      (void)data;
      (void)offset;
      return Status::OK();
  }

  virtual Status Flush() {
      return Status::OK();
  }

  virtual Status Sync() {
      Status s;
      ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsWritableFile Sync %s\n",
                      filename_.c_str());
      if (sdfsFlush(fileSys_, hfile_) == -1) {
          return IOError(filename_, errno);
      }
      if (sdfsHSync(fileSys_, hfile_) == -1) {
          return IOError(filename_, errno);
      }
      ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsWritableFile Synced %s\n",
                      filename_.c_str());
      return Status::OK();
  }

  virtual Status Append(const char* src, size_t size) {
      if (sdfsWrite(fileSys_, hfile_, src, size) != (tSize)size) {
          return IOError(filename_, errno);
      }
      return Status::OK();
  }

  virtual Status Close() {
      ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsWritableFile closing %s\n",
                      filename_.c_str());
      if (sdfsCloseFile(fileSys_, hfile_) != 0) {
          return IOError(filename_, errno);
      }
      ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsWritableFile closed %s\n",
                      filename_.c_str());
      hfile_ = 0;
      return Status::OK();
  }
};

class SdfsLogger : public Logger {
 private:
  SdfsWritableFile* file_;
  uint64_t (*gettid_)();  

 public:
  using Logger::Logv;
  SdfsLogger(SdfsWritableFile* f, uint64_t (*gettid)())
      : file_(f), gettid_(gettid) {
          ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsLogger opened %s\n",
                          file_->getName().c_str());
      }

  virtual ~SdfsLogger() {
      ROCKS_LOG_DEBUG(mylog, "[sdfs] SdfsLogger closed %s\n",
                      file_->getName().c_str());
      delete file_;
      if (mylog != nullptr && mylog == this) {
          mylog = nullptr;
      }
  }

  virtual void Logv(const char* format, va_list ap) {
      const uint64_t thread_id = (*gettid_)();

      char buffer[500];
      for (int iter = 0; iter < 2; iter++) {
          char* base;
          int bufsize;
          if (iter == 0) {
              bufsize = sizeof(buffer);
              base = buffer;
          } else {
              bufsize = 30000;
              base = new char[bufsize];
          }
          char* p = base;
          char* limit = base + bufsize;

          struct timeval now_tv;
          gettimeofday(&now_tv, nullptr);
          const time_t seconds = now_tv.tv_sec;
          struct tm t;
          localtime_r(&seconds, &t);
          p += snprintf(p, limit - p,
                        "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                        t.tm_year + 1900,
                        t.tm_mon + 1,
                        t.tm_mday,
                        t.tm_hour,
                        t.tm_min,
                        t.tm_sec,
                        static_cast<int>(now_tv.tv_usec),
                        static_cast<long long unsigned int>(thread_id));

          if (p < limit) {
              va_list backup_ap;
              va_copy(backup_ap, ap);
              p += vsnprintf(p, limit - p, format, backup_ap);
              va_end(backup_ap);
          }

          if (p >= limit) {
              if (iter == 0) {
                  continue;       
              } else {
                  p = limit - 1;
              }
          }

          if (p == base || p[-1] != '\n') {
              *p++ = '\n';
          }

          assert(p <= limit);
          file_->Append(base, p-base);
          file_->Flush();
          if (base != buffer) {
              delete[] base;
          }
          break;
      }
  }
};

}  

const std::string SdfsEnv::kProto = "sdfs:";
const std::string SdfsEnv::pathsep = "/";

SdfsEnv::SdfsEnv(const std::string& fsname) {
    (void)fsname;
    posixEnv = Env::Default();
    fileSys_ = connectToPath(fsname_);
    GoSdfsInit();
}

SdfsEnv::~SdfsEnv() {
    fprintf(stderr, "Destroying HdfsEnv::Default()\n");
    sdfsDisconnect(fileSys_);
}

Status SdfsEnv::NewSequentialFile(const std::string& fname,
                                  unique_ptr<SequentialFile>* result,
                                  const EnvOptions& options) {
    (void)options;
    result->reset();
    SdfsReadableFile* f = new SdfsReadableFile(fileSys_, fname);
    if (f == nullptr || !f->isValid()) {
        delete f;
        *result = nullptr;
        return IOError(fname, errno);
    }
    result->reset(dynamic_cast<SequentialFile*>(f));
    return Status::OK();
}

Status SdfsEnv::NewRandomAccessFile(const std::string& fname,
                                    unique_ptr<RandomAccessFile>* result,
                                    const EnvOptions& options) {
    (void)options;
    result->reset();
    SdfsReadableFile* f = new SdfsReadableFile(fileSys_, fname);
    if (f == nullptr || !f->isValid()) {
        delete f;
        *result = nullptr;
        return IOError(fname, errno);
    }
    result->reset(dynamic_cast<RandomAccessFile*>(f));
    return Status::OK();
}

Status SdfsEnv::NewWritableFile(const std::string& fname,
                                unique_ptr<WritableFile>* result,
                                const EnvOptions& options) {
    (void)options;
    result->reset();
    Status s;
    SdfsWritableFile* f = new SdfsWritableFile(fileSys_, fname);
    if (f == nullptr || !f->isValid()) {
        delete f;
        *result = nullptr;
        return IOError(fname, errno);
    }
    result->reset(dynamic_cast<WritableFile*>(f));
    return Status::OK();
}

class SdfsDirectory : public Directory {
 public:
  explicit SdfsDirectory(int fd) : fd_(fd) {}
  ~SdfsDirectory() {}

  virtual Status Fsync() { return Status::OK(); }

 private:
  int fd_;
};

Status SdfsEnv::NewDirectory(const std::string& name,
                             unique_ptr<Directory>* result) {
    int value = sdfsExists(fileSys_, name.c_str());
    switch (value) {
        case SDFS_EXISTS:
            result->reset(new SdfsDirectory(0));
            return Status::OK();
        default:  
            ROCKS_LOG_FATAL(mylog, "NewDirectory sdfsExists call failed");
            throw SdfsFatalException("sdfsExists call failed with error " +
                                     ToString(value) + " on path " + name +
                                     ".\n");
    }
}

Status SdfsEnv::FileExists(const std::string& fname) {
    int value = sdfsExists(fileSys_, fname.c_str());
    switch (value) {
        case SDFS_EXISTS:
            return Status::OK();
        case SDFS_DOESNT_EXIST:
            return Status::NotFound();
        default:  
            ROCKS_LOG_FATAL(mylog, "FileExists sdfsExists call failed");
            return Status::IOError("sdfsExists call failed with error " +
                                   ToString(value) + " on path " + fname + ".\n");
    }
}

Status SdfsEnv::GetChildren(const std::string& path,
                            std::vector<std::string>* result) {
    int value = sdfsExists(fileSys_, path.c_str());
    switch (value) {
        case SDFS_EXISTS: {  
            int numEntries;
            void** inodePathArr = nullptr;
            sdfsListDirectory(fileSys_, path.c_str(), (void**)(&inodePathArr), &numEntries);
            if (numEntries >= 0) {
                for(int i = 0; i < numEntries; i++) {
                    char* pathname = (char*)inodePathArr[i];
                    char* filename = rindex(pathname, '/');
                    if (filename != nullptr) {
                        result->push_back(filename+1);
                    }
                }
                sdfsFreeArr(inodePathArr, numEntries);
            } else {

                ROCKS_LOG_FATAL(mylog, "sdfsListDirectory call failed with error ");
                throw SdfsFatalException(
                    "sdfsListDirectory call failed negative error.\n");
            }
            break;
        }
        case SDFS_DOESNT_EXIST:  
            return Status::NotFound();
        default:          
            ROCKS_LOG_FATAL(mylog, "GetChildren sdfsExists call failed");
            throw SdfsFatalException("sdfsExists call failed with error " +
                                     ToString(value) + ".\n");
    }
    return Status::OK();
}

Status SdfsEnv::DeleteFile(const std::string& fname) {
    if (sdfsDelete(fileSys_, fname.c_str(), 1) == 0) {
        return Status::OK();
    }
    return IOError(fname, errno);
};

Status SdfsEnv::CreateDir(const std::string& name) {
    if (sdfsCreateDirectory(fileSys_, name.c_str()) == 0) {
        return Status::OK();
    }
    return IOError(name, errno);
};

Status SdfsEnv::CreateDirIfMissing(const std::string& name) {
    const int value = sdfsExists(fileSys_, name.c_str());

    switch (value) {
        case SDFS_EXISTS:
            return Status::OK();
        case SDFS_DOESNT_EXIST:
            return CreateDir(name);
        default:  
            ROCKS_LOG_FATAL(mylog, "CreateDirIfMissing sdfsExists call failed");
            throw SdfsFatalException("sdfsExists call failed with error " +
                                     ToString(value) + ".\n");
    }
};

Status SdfsEnv::DeleteDir(const std::string& name) {
    return DeleteFile(name);
};

Status SdfsEnv::GetFileSize(const std::string& fname, uint64_t* size) {
    *size = 0L;
    sdfsFileInfo pFileInfo;
    int code = sdfsGetPathInfo(fileSys_, fname.c_str(), &pFileInfo);
    if (code == 0) {
        *size = pFileInfo.mSize;
        return Status::OK();
    }
    return IOError(fname, errno);
}

Status SdfsEnv::GetFileModificationTime(const std::string& fname,
                                        uint64_t* time) {
    sdfsFileInfo pFileInfo;
    int code = sdfsGetPathInfo(fileSys_, fname.c_str(), &pFileInfo);
    if (code == 0) {
        *time = static_cast<uint64_t>(pFileInfo.mLastMod);
        return Status::OK();
    }
    return IOError(fname, errno);

}

Status SdfsEnv::RenameFile(const std::string& src, const std::string& target) {
    sdfsDelete(fileSys_, target.c_str(), 1);
    if (sdfsRename(fileSys_, src.c_str(), target.c_str()) == 0) {
        return Status::OK();
    }
    return IOError(src, errno);
}

Status SdfsEnv::LockFile(const std::string& fname, FileLock** lock) {
    (void)fname;
    *lock = nullptr;
    return Status::OK();
}

Status SdfsEnv::UnlockFile(FileLock* lock) {
    (void)lock;
    return Status::OK();
}

Status SdfsEnv::NewLogger(const std::string& fname,
                          shared_ptr<Logger>* result) {
    SdfsWritableFile* f = new SdfsWritableFile(fileSys_, fname);
    if (f == nullptr || !f->isValid()) {
        delete f;
        *result = nullptr;
        return IOError(fname, errno);
    }
    SdfsLogger* h = new SdfsLogger(f, &SdfsEnv::gettid);
    result->reset(h);
    if (mylog == nullptr) {

    }
    return Status::OK();
}

Status NewSdfsEnv(Env** sdfs_env, const std::string& fsname) {
    *sdfs_env = new SdfsEnv(fsname);
    return Status::OK();
}
}  

#endif 

#else 

namespace rocksdb {
Status SdfsEnv::NewSequentialFile(const std::string& fname,
                                  unique_ptr<SequentialFile>* result,
                                  const EnvOptions& options) {
    return Status::NotSupported("Not compiled with sdfs support");
}

Status NewSdfsEnv(Env** sdfs_env, const std::string& fsname) {
    return Status::NotSupported("Not compiled with sdfs support");
}
}

#endif
