#pragma once

#include "envoy/event/dispatcher.h"
#include "envoy/filesystem/filesystem.h"

#include "common/common/linked_object.h"
#include "common/common/logger.h"

namespace Filesystem {

/**
 * Implementation of Watcher that uses kqueue. kqueue API is way more elegant than inotify
 * and allows multiple files monitoring.
 */
class WatcherImpl : public Watcher, Logger::Loggable<Logger::Id::file> {
public:
  WatcherImpl(Event::Dispatcher& dispatcher);
  ~WatcherImpl();

  // Filesystem::Watcher
  void addWatch(const std::string& path, uint32_t events, OnChangedCb cb) override;

private:
  struct FileWatch : LinkedObject<FileWatch> {
    int fd_;
    uint32_t events_;
    std::string file_;
    OnChangedCb callback_;
  };

  typedef std::unique_ptr<FileWatch> FileWatchPtr;

  void onKqueueEvent();
  FileWatchPtr addWatch_(const std::string& path, uint32_t events, Watcher::OnChangedCb cb);

  int queue_;
  std::list<FileWatchPtr> watches_;
  Event::FileEventPtr kqueue_event_;
};

} // Filesystem
