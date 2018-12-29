#pragma once
#include <algorithm>
#include <stdio.h>
#include <time.h>
#include <iostream>
#include "port/sys_time.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"

#ifdef USE_SDFS
#include <libsdfs.h>
#include "sdfs/sdfs.h"

#define SDFS_EXISTS 0
#define SDFS_DOESNT_EXIST -1
#define SDFS_SUCCESS 0

namespace rocksdb {

class SdfsUsageException : public std::exception { };

class SdfsFatalException : public std::exception {
 public:
  explicit SdfsFatalException(const std::string& s) : what_(s) { }
  virtual ~SdfsFatalException() throw() { }
  virtual const char* what() const throw() {
      return what_.c_str();
  }
 private:
  const std::string what_;
};

class SdfsEnv : public Env {

 public:
  explicit SdfsEnv(const std::string& fsname);

  virtual ~SdfsEnv();

  virtual Status NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options);

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     std::unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& options);

  virtual Status NewWritableFile(const std::string& fname,
                                 std::unique_ptr<WritableFile>* result,
                                 const EnvOptions& options);

  virtual Status NewDirectory(const std::string& name,
                              std::unique_ptr<Directory>* result);

  virtual Status FileExists(const std::string& fname);

  virtual Status GetChildren(const std::string& path,
                             std::vector<std::string>* result);

  virtual Status DeleteFile(const std::string& fname);

  virtual Status CreateDir(const std::string& name);

  virtual Status CreateDirIfMissing(const std::string& name);

  virtual Status DeleteDir(const std::string& name);

  virtual Status GetFileSize(const std::string& fname, uint64_t* size);

  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* file_mtime);

  virtual Status RenameFile(const std::string& src, const std::string& target);

  virtual Status LinkFile(const std::string& src, const std::string& target) {
      (void)src;
      (void)target;
      return Status::NotSupported(); 
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock);

  virtual Status UnlockFile(FileLock* lock);

  virtual Status NewLogger(const std::string& fname,
                           std::shared_ptr<Logger>* result);

  virtual void Schedule(void (*function)(void* arg), void* arg,
                        Priority pri = LOW, void* tag = nullptr, void (*unschedFunction)(void* arg) = 0) {
      posixEnv->Schedule(function, arg, pri, tag, unschedFunction);
  }

  virtual int UnSchedule(void* tag, Priority pri) {
      return posixEnv->UnSchedule(tag, pri);
  }

  virtual void StartThread(void (*function)(void* arg), void* arg) {
      posixEnv->StartThread(function, arg);
  }

  virtual void WaitForJoin() { posixEnv->WaitForJoin(); }

  virtual unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const
      override {
          return posixEnv->GetThreadPoolQueueLen(pri);
      }

  virtual Status GetTestDirectory(std::string* path) {
      return posixEnv->GetTestDirectory(path);
  }

  virtual uint64_t NowMicros() {
      return posixEnv->NowMicros();
  }

  virtual void SleepForMicroseconds(int micros) {
      posixEnv->SleepForMicroseconds(micros);
  }

  virtual Status GetHostName(char* name, uint64_t len) {
      return posixEnv->GetHostName(name, len);
  }

  virtual Status GetCurrentTime(int64_t* unix_time) {
      return posixEnv->GetCurrentTime(unix_time);
  }

  virtual Status GetAbsolutePath(const std::string& db_path,
                                 std::string* output_path) {
      return posixEnv->GetAbsolutePath(db_path, output_path);
  }

  virtual void SetBackgroundThreads(int number, Priority pri = LOW) {
      posixEnv->SetBackgroundThreads(number, pri);
  }

  virtual int GetBackgroundThreads(Priority pri = LOW) {
      return posixEnv->GetBackgroundThreads(pri);
  }

  virtual void IncBackgroundThreadsIfNeeded(int number, Priority pri) override {
      posixEnv->IncBackgroundThreadsIfNeeded(number, pri);
  }

  virtual std::string TimeToString(uint64_t number) {
      return posixEnv->TimeToString(number);
  }

  static uint64_t gettid() {
      assert(sizeof(pthread_t) <= sizeof(uint64_t));
      return (uint64_t)pthread_self();
  }

  virtual uint64_t GetThreadID() const override {
      return SdfsEnv::gettid();
  }

 private:
  std::string fsname_;  
  sdfsFS fileSys_;      
  Env*  posixEnv;       

  static const std::string kProto;
  static const std::string pathsep;

  sdfsFS connectToPath(const std::string& uri) {
      if (uri.empty()) {
          return nullptr;
      }
      if (uri.find(kProto) != 0) {

          return sdfsConnectNewInstance("default", 0);
      }
      const std::string hostport = uri.substr(kProto.length());

      std::vector <std::string> parts;
      split(hostport, ':', parts);
      if (parts.size() != 2) {
          throw SdfsFatalException("Bad uri for sdfs " + uri);
      }

      std::string host(parts[0]);
      std::string remaining(parts[1]);

      int rem = remaining.find(pathsep);
      std::string portStr = (rem == 0 ? remaining :
                             remaining.substr(0, rem));

      tPort port;
      port = atoi(portStr.c_str());
      if (port == 0) {
          throw SdfsFatalException("Bad host-port for sdfs " + uri);
      }
      sdfsFS fs = sdfsConnectNewInstance(host.c_str(), port);
      return fs;
  }

  void split(const std::string &s, char delim,
             std::vector<std::string> &elems) {
      elems.clear();
      size_t prev = 0;
      size_t pos = s.find(delim);
      while (pos != std::string::npos) {
          elems.push_back(s.substr(prev, pos));
          prev = pos + 1;
          pos = s.find(delim, prev);
      }
      elems.push_back(s.substr(prev, s.size()));
  }
};

}  

#else 

namespace rocksdb {

static const Status sdfsNotSup;

class SdfsEnv : public Env {

 public:
  explicit SdfsEnv(const std::string& fsname) {
      fprintf(stderr, "You have not build rocksdb with SDFS support\n");
      fprintf(stderr, "Please see sdfs/README for details\n");
      abort();
  }

  virtual ~SdfsEnv() {
  }

  virtual Status NewSequentialFile(const std::string& fname,
                                   unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) override;

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& options) override {
      return sdfsNotSup;
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 unique_ptr<WritableFile>* result,
                                 const EnvOptions& options) override {
      return sdfsNotSup;
  }

  virtual Status NewDirectory(const std::string& name,
                              unique_ptr<Directory>* result) override {
      return sdfsNotSup;
  }

  virtual Status FileExists(const std::string& fname) override {
      return sdfsNotSup;
  }

  virtual Status GetChildren(const std::string& path,
                             std::vector<std::string>* result) override {
      return sdfsNotSup;
  }

  virtual Status DeleteFile(const std::string& fname) override {
      return sdfsNotSup;
  }

  virtual Status CreateDir(const std::string& name) override { return sdfsNotSup; }

  virtual Status CreateDirIfMissing(const std::string& name) override {
      return sdfsNotSup;
  }

  virtual Status DeleteDir(const std::string& name) override { return sdfsNotSup; }

  virtual Status GetFileSize(const std::string& fname,
                             uint64_t* size) override {
      return sdfsNotSup;
  }

  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* time) override {
      return sdfsNotSup;
  }

  virtual Status RenameFile(const std::string& src,
                            const std::string& target) override {
      return sdfsNotSup;
  }

  virtual Status LinkFile(const std::string& src,
                          const std::string& target) override {
      return sdfsNotSup;
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) override {
      return sdfsNotSup;
  }

  virtual Status UnlockFile(FileLock* lock) override { return sdfsNotSup; }

  virtual Status NewLogger(const std::string& fname,
                           shared_ptr<Logger>* result) override {
      return sdfsNotSup;
  }

  virtual void Schedule(void (*function)(void* arg), void* arg,
                        Priority pri = LOW, void* tag = nullptr,
                        void (*unschedFunction)(void* arg) = 0) override {}

  virtual int UnSchedule(void* tag, Priority pri) override { return 0; }

  virtual void StartThread(void (*function)(void* arg), void* arg) override {}

  virtual void WaitForJoin() override {}

  virtual unsigned int GetThreadPoolQueueLen(
      Priority pri = LOW) const override {
      return 0;
  }

  virtual Status GetTestDirectory(std::string* path) override { return sdfsNotSup; }

  virtual uint64_t NowMicros() override { return 0; }

  virtual void SleepForMicroseconds(int micros) override {}

  virtual Status GetHostName(char* name, uint64_t len) override {
      return sdfsNotSup;
  }

  virtual Status GetCurrentTime(int64_t* unix_time) override { return sdfsNotSup; }

  virtual Status GetAbsolutePath(const std::string& db_path,
                                 std::string* outputpath) override {
      return sdfsNotSup;
  }

  virtual void SetBackgroundThreads(int number, Priority pri = LOW) override {}
  virtual int GetBackgroundThreads(Priority pri = LOW) override { return 0; }
  virtual void IncBackgroundThreadsIfNeeded(int number, Priority pri) override {
  }
  virtual std::string TimeToString(uint64_t number) override { return ""; }

  virtual uint64_t GetThreadID() const override {
      return 0;
  }
};
}

#endif 
