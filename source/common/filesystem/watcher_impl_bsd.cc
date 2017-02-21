#include "watcher_impl_bsd.h"

#include "envoy/common/exception.h"
#include "envoy/event/dispatcher.h"
#include "envoy/event/file_event.h"

#include "common/common/assert.h"
#include "common/common/utility.h"

#include "event2/event.h"

#include <sys/fcntl.h>
#include <sys/types.h>
#include <sys/event.h>


namespace Filesystem {

WatcherImpl::WatcherImpl(Event::Dispatcher& dispatcher)
    : queue_(kqueue()),
      kqueue_event_(dispatcher.createFileEvent(queue_, [this](uint32_t events) -> void {
        if (events & Event::FileReadyType::Read) {
          onKqueueEvent();
        }
      }, Event::FileTriggerType::Edge)) {}

WatcherImpl::~WatcherImpl() {
  close(queue_);

  for (FileWatchPtr &file : watches_) {
    close(file->fd_);
    file->removeFromList(watches_);
  }
}

void WatcherImpl::addWatch(const std::string& path, uint32_t events, Watcher::OnChangedCb cb) {
  FileWatchPtr watch = addWatch_(path, events, cb);
  if (watch == nullptr) {
    throw EnvoyException(fmt::format("invalid watch path {}", path));
  }
  watch->moveIntoList(std::move(watch), watches_);
}

WatcherImpl::FileWatchPtr WatcherImpl::addWatch_(const std::string& path, uint32_t events,
                                                 Watcher::OnChangedCb cb) {
  int watch_fd = open(path.c_str(), O_SYMLINK);
  if (watch_fd == -1) {
    return nullptr;
  }

  FileWatchPtr watch(new FileWatch());
  watch->fd_ = watch_fd;
  watch->file_ = path;
  watch->events_ = events;
  watch->callback_ = cb;

  struct kevent event;
  EV_SET(&event, watch_fd, EVFILT_VNODE, EV_ADD | EV_CLEAR, NOTE_DELETE | NOTE_RENAME,
         0, watch.get());

  if (kevent(queue_, &event, 1, NULL, 0, NULL) == -1) {
    throw EnvoyException(
        fmt::format("unable to add filesystem watch for file {}: {}", path, strerror(errno)));
  }

  if (event.flags & EV_ERROR) {
    throw EnvoyException(
        fmt::format("unable to add filesystem watch for file {}: {}", path, strerror(event.data)));
  }

  log_debug("added watch for file: '{}' fd: {}", path, watch_fd);
  return watch;
}

void WatcherImpl::onKqueueEvent() {
  struct kevent event = {};
  timespec nullts = {0, 0};

  while (true) {
      int nevents = kevent(queue_, NULL, 0, &event, 1, &nullts);
      if (nevents < 1 || event.udata == nullptr) {
        return;
      }

      FileWatch* file = static_cast<FileWatch*>(event.udata);
      // kqueue doesn't seem to work well with NOTE_RENAME and O_SYMLINK, so instead if we
      // get a NOTE_DELETE on the symlink we check if there is another file with the same
      // name we assume a NOTE_RENAME and re-attach another event to the new file.
      if (event.fflags & NOTE_DELETE) {
        close(file->fd_);

        FileWatchPtr newFile = addWatch_(file->file_, file->events_, file->callback_);
        file->removeFromList(watches_);
        if (newFile == nullptr) {
          return;
        }

        event.fflags |= NOTE_RENAME;
        file = newFile.get();
        newFile->moveIntoList(std::move(newFile), watches_);
      }

      uint32_t events = 0;
      if (event.fflags & NOTE_RENAME) {
        events |= Events::MovedTo;
      }

      if (events & file->events_) {
        log_debug("matched callback: file: {}", file->file_);
        file->callback_(events);
      }

      log_debug("notification: fd: {} flags: {:x} file: {}", file->fd_, event.fflags, file->file_);
  }
}

} // Filesystem
