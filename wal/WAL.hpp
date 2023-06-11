#ifndef WAL_H
#define WAL_H
#include <boost/noncopyable.hpp>
#include <cstring>
#include <string>

#include "base/Error.hpp"
#include "base/Mutex.hpp"
#include "base/ThreadPool.hpp"
#include "wal/WALUtils.hpp"

namespace tsdb {
namespace wal {

std::string segment_name(const std::string &dir, int i);

class Page : boost::noncopyable {
 public:
  int alloc;
  int flushed;
  uint8_t buf_[32 * 1024];

  Page() { reset(true); }

  bool full() { return (alloc == PAGE_SIZE); }

  int remaining() { return (PAGE_SIZE - alloc); }

  void reset(bool real) {
    alloc = 0;
    flushed = 0;
    if (real) memset(buf_, 0, PAGE_SIZE);
  }
};

// Segment is one file.
class Segment : boost::noncopyable {
 public:
  std::string dir_;
  int index_;
  FILE *f;
  error::Error err_;
  bool closed;  // If the file closed.

 public:
  Segment(const std::string &dir, int index, bool write = true);  // Read/Write.
  Segment(const std::string &filename);                           // Read.
  Segment(const std::string &filename, const std::string &dir,
          int index);  // Read.

  std::string dir() { return dir_; }

  int index() { return index_; }

  bool get_close() { return closed; }
  void set_closed(bool c) { closed = c; }
  void close() {
    if (!closed) {
      fsync(fileno(f));
      fclose(f);
      closed = true;
    }
  }

  ~Segment() {
    // LOG_DEBUG << "Destruct Segment";
    close();
  }
};

class SegmentRef {
 public:
  std::string name;
  int index;
  SegmentRef(const std::string &name, int index) : name(name), index(index) {}
};

class SegmentRange {
 public:
  std::string dir;
  int first;
  int last;

  SegmentRange(const std::string &dir, int first, int last)
      : dir(dir), first(first), last(last) {}
};

class CorruptionError {
 public:
  std::string dir;
  int segment;
  uint64_t offset;
  error::Error err_;
  bool err;

  CorruptionError() : err(false){};
  CorruptionError(const std::string dir, int segment, uint64_t offset,
                  const error::Error &err_)
      : dir(dir), segment(segment), offset(offset), err_(err_), err(true) {}
  CorruptionError(int segment, uint64_t offset, const error::Error &err_)
      : segment(segment), offset(offset), err_(err_), err(true) {}
  operator bool() const { return err; }
  error::Error error() { return err_; }
};

std::pair<std::vector<SegmentRef>, error::Error> list_segments(
    const std::string &dir);

// TODO(Alec), add metrics to monitor.
class WAL : boost::noncopyable {
 private:
  std::string dir_;  // [...]/wal
  int segment_size;
  base::RWMutexLock mutex_;
  std::shared_ptr<Segment> segment;
  std::unique_ptr<Page> page;
  int done_pages;
  std::shared_ptr<base::ThreadPool> pool_;
  error::Error err_;

 public:
  WAL(const std::string &dir, const std::shared_ptr<base::ThreadPool> &pool_,
      int segment_size = SEGMENT_SIZE);
  std::string dir() { return dir_; }
  int pages_per_segment() { return segment_size / PAGE_SIZE; }
  std::pair<std::pair<int, int>, error::Error> segments(const std::string &dir);
  error::Error set_segment(const std::shared_ptr<Segment> &s);

  // truncate drops all segments before i.
  error::Error truncate(int i);

  // next_segment creates the next segment and closes the previous one.
  error::Error next_segment();

  // flush_page writes the new contents of the page to disk. If no more records
  // will fit into the page, the remaining bytes will be set to zero and a new
  // page will be started. If clear is true, this is enforced regardless of how
  // many bytes are left in the page.
  error::Error flush_page(bool clear);

  // log writes rec to the log and forces a flush of the current page if its
  // the final record of a batch, the record is bigger than the page size or
  // the current page is full.
  error::Error log(const std::vector<uint8_t> &rec, bool final = true);
  error::Error log(const std::vector<std::vector<uint8_t>> &recs);
  error::Error log(const uint8_t *p, int length, bool final = true);

  // Repair attempts to repair the WAL based on the error.
  // It discards all data after the corruption.
  error::Error repair(const CorruptionError &cerr);

  std::shared_ptr<base::ThreadPool> pool() { return pool_; }

  void sync();

  error::Error error() { return err_; }
  ~WAL();
};

error::Error validate_record(int i, RECORD_TYPE type);

class SegmentReader {
 private:
  std::vector<std::shared_ptr<Segment>> segments;
  int segment_index;
  int segment_offset;
  bool offset_reset;
  error::Error err_;

  RECORD_TYPE record_type;
  uint8_t buf[32 * 1024];
  int page_offset;

  std::vector<uint8_t> record_;
  bool eof;

 public:
  SegmentReader(const std::string &dir, bool directory = true);
  SegmentReader(const SegmentRef &ref);
  SegmentReader(const std::string &name, const std::string &dir, int index);
  SegmentReader(const std::deque<SegmentRange> &segs);

  // Read one page from current segment.
  // Pad zeros if page size < PAGE_SIZE.
  bool read_page();

  bool next();

  error::Error error();

  CorruptionError cerror();

  // <ptr, length>.
  std::pair<uint8_t *, int> record();

  int segment();

  int offset();

  // Close all Segments.
  void clear();
};

}  // namespace wal
}  // namespace tsdb

#endif