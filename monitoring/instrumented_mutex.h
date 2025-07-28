//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <execinfo.h>
#include "monitoring/statistics.h"
#include "port/port.h"
#include "rocksdb/statistics.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/thread_status.h"
#include "logging/logging.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {
class InstrumentedCondVar;

// A wrapper class for port::Mutex that provides additional layer
// for collecting stats and instrumentation.
class InstrumentedMutex {
 public:
  explicit InstrumentedMutex(bool adaptive = false)
      : mutex_(adaptive), stats_(nullptr), clock_(nullptr), stats_code_(0), info_log_(nullptr) {}

  explicit InstrumentedMutex(SystemClock* clock, bool adaptive = false)
      : mutex_(adaptive), stats_(nullptr), clock_(clock), stats_code_(0), info_log_(nullptr) {}

  InstrumentedMutex(Statistics* stats, SystemClock* clock, int stats_code,
                    bool adaptive, std::shared_ptr<Logger> info_log)
      : mutex_(adaptive),
        stats_(stats),
        clock_(clock),
        stats_code_(stats_code),
        info_log_(info_log) {}

  void Lock();

  void Unlock() {
    if (clock_ != nullptr && info_log_ != nullptr && lock_nanos_ != 0) {
      // Log if the lock was held for a significant time, 10ms or more
      uint64_t now = clock_->NowNanos();
      uint64_t elapsed = now - lock_nanos_;
      // uint64_t ten_ms = 10000000;  // 10 milliseconds in nanoseconds
      uint64_t two_ms = 2000000;  // 2 milliseconds in nanoseconds
      uint64_t one_ms = 1000000;  // 1 milliseconds in nanoseconds
      if (elapsed >= two_ms) {
        // Log backtrace
        void* callstack[32];
        int frames = backtrace(callstack, 32);
        char** strs = backtrace_symbols(callstack, frames);
        ROCKS_LOG_WARN(info_log_, "Backtrace:");
        // Join strs using "\n" and output in one log
        std::string trace;
        for (int i = 0; i < frames; ++i) {
          trace += strs[i];
          trace += "\\n";
        }
        ROCKS_LOG_WARN(info_log_, "dbg mutex held for a long time: %" PRIu64 " ms, backtrace: %s",
                        (elapsed / one_ms), trace.c_str());
        free(strs);
      }
    }
    lock_nanos_ = 0;
    mutex_.Unlock();
  }

  void AssertHeld() {
    mutex_.AssertHeld();
  }

 private:
  void LockInternal();
  friend class InstrumentedCondVar;
  port::Mutex mutex_;
  Statistics* stats_;
  SystemClock* clock_;
  int stats_code_;
  uint64_t lock_nanos_ = 0;
  std::shared_ptr<Logger> info_log_;
};

// RAII wrapper for InstrumentedMutex
class InstrumentedMutexLock {
 public:
  explicit InstrumentedMutexLock(InstrumentedMutex* mutex) : mutex_(mutex) {
    mutex_->Lock();
  }

  ~InstrumentedMutexLock() {
    mutex_->Unlock();
  }

 private:
  InstrumentedMutex* const mutex_;
  InstrumentedMutexLock(const InstrumentedMutexLock&) = delete;
  void operator=(const InstrumentedMutexLock&) = delete;
};

// RAII wrapper for temporary releasing InstrumentedMutex inside
// InstrumentedMutexLock
class InstrumentedMutexUnlock {
 public:
  explicit InstrumentedMutexUnlock(InstrumentedMutex* mutex) : mutex_(mutex) {
    mutex_->Unlock();
  }

  ~InstrumentedMutexUnlock() { mutex_->Lock(); }

 private:
  InstrumentedMutex* const mutex_;
  InstrumentedMutexUnlock(const InstrumentedMutexUnlock&) = delete;
  void operator=(const InstrumentedMutexUnlock&) = delete;
};

class InstrumentedCondVar {
 public:
  explicit InstrumentedCondVar(InstrumentedMutex* instrumented_mutex)
      : cond_(&(instrumented_mutex->mutex_)),
        stats_(instrumented_mutex->stats_),
        clock_(instrumented_mutex->clock_),
        stats_code_(instrumented_mutex->stats_code_) {}

  void Wait();

  bool TimedWait(uint64_t abs_time_us);

  void Signal() {
    cond_.Signal();
  }

  void SignalAll() {
    cond_.SignalAll();
  }

 private:
  void WaitInternal();
  bool TimedWaitInternal(uint64_t abs_time_us);
  port::CondVar cond_;
  Statistics* stats_;
  SystemClock* clock_;
  int stats_code_;
};

}  // namespace ROCKSDB_NAMESPACE
