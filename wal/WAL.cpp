#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <algorithm>
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <chrono>
#include <cstdio>
#include <string>
// #include <boost/algorithm/string/predicate.hpp>
#include <iostream>
#include <vector>

#include "base/Checksum.hpp"
#include "base/Endian.hpp"
#include "base/Logging.hpp"
#include "base/TSDBException.hpp"
#include "tsdbutil/tsdbutils.hpp"
#include "wal/WAL.hpp"

namespace tsdb {
namespace wal {

std::string segment_name(const std::string &dir, int i) {
  return tsdbutil::filepath_join(dir, (boost::format("%08d") % i).str());
}

void close_segment(std::shared_ptr<Segment> segment) {
  auto t0 = std::chrono::high_resolution_clock::now();
  if (segment->get_close())
    LOG_WARN << "msg=\"segment " << segment->index() << " already closed\"";
  else {
    if (fsync(fileno(segment->f)) != 0)
      LOG_ERROR << "msg=\"sync previous segment " << segment->index()
                << "\" err=" << strerror(errno);
    if (fclose(segment->f) != 0)
      LOG_ERROR << "msg=\"close previous segment " << segment->index()
                << "\" err=" << strerror(errno);
    segment->set_closed(true);
  }
  LOG_INFO << "close segment: " << segment_name(segment->dir_, segment->index_)
           << ", duration: "
           << std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::high_resolution_clock::now() - t0)
                  .count()
           << "ms";
}

Segment::Segment(const std::string &dir, int index, bool write)
    : dir_(dir), index_(index), closed(true) {
  std::string name = segment_name(dir, index);
  if (write) {
    f = fopen(name.c_str(), "ab");
    if (!f) {
      err_.set("Cannot open segment file: " + name);
      return;
    }
    closed = false;

    // If the last page is torn, fill it with zeros.
    // In case it was torn after all records were written successfully, this
    // will just pad the page and everything will be fine.
    // If it was torn mid-record, a full read (which the caller should do anyway
    // to ensure integrity) will detect it as a corruption by the end.
    int size_ = ftell(f);
    if ((size_ = size_ % PAGE_SIZE) != 0) {
      LOG_WARN << "msg=\"last page of the wal is torn, filling it with zeros\""
               << " segment=",
          name;
      std::vector<char> pad_zero(PAGE_SIZE - size_, 0);
      if (fwrite(&(pad_zero[0]), 1, PAGE_SIZE - size_, f) !=
          PAGE_SIZE - size_) {
        fclose(f);
        err_.set("error zero-padding torn page");
        return;
      }
    }
  } else {
    f = fopen(name.c_str(), "rb");
    if (!f) {
      err_.set("Cannot open segment file: " + name);
      return;
    }
    closed = false;
  }
}

Segment::Segment(const std::string &filename) : closed(true) {
  boost::filesystem::path p(filename);
  if (!tsdbutil::is_number(p.filename().string())) {
    err_.set("invalid segment filename");
    return;
  }
  dir_ = p.parent_path().string();
  index_ = std::stoi(p.filename().string());
  f = fopen(filename.c_str(), "rb");
  if (!f) {
    err_.set("Cannot open segment file: " + filename);
    return;
  }
  closed = false;
}

Segment::Segment(const std::string &filename, const std::string &dir, int index)
    : dir_(dir), index_(index), closed(true) {
  f = fopen(filename.c_str(), "rb");
  if (!f) {
    err_.set("Cannot open segment file: " + filename);
    return;
  }
  closed = false;
}

bool compare_index(std::string &s1, std::string &s2) {
  if (s1.length() > s2.length()) {
    return false;
  } else if (s1.length() < s2.length()) {
    return true;
  } else {
    for (int i = 0; i < s1.length(); i++) {
      if (s1[i] < s2[i])
        return true;
      else if (s1[i] > s2[i])
        return false;
    }
    return true;
  }
}

std::pair<std::vector<SegmentRef>, error::Error> list_segments(
    const std::string &dir) {
  boost::filesystem::path p(dir);
  std::vector<SegmentRef> r;
  if (boost::filesystem::is_directory(p)) {
    boost::filesystem::directory_iterator end_itr;

    // cycle through the directory
    for (boost::filesystem::directory_iterator itr(p); itr != end_itr; ++itr) {
      // If it's not a directory, list it. If you want to list directories too,
      // just remove this check.
      if (boost::filesystem::is_regular_file(itr->path()) &&
          tsdbutil::is_number(itr->path().filename().string())) {
        // r.push_back(itr->path().filename().string());
        // LOG_INFO << itr->path().filename().string();
        // std::cerr << itr->path().string() <<  ", " <<
        // std::stoi(itr->path().filename().string()) << std::endl;
        r.emplace_back(itr->path().string(),
                       std::stoi(itr->path().filename().string()));
      }
    }
  } else {
    // LOG_ERROR << dir << " is not a directory";
    return {r, error::Error(dir + " is not a directory")};
  }
  std::sort(r.begin(), r.end(),
            [](const SegmentRef &lhs, const SegmentRef &rhs) {
              return lhs.index < rhs.index;
            });

  return {r, error::Error()};
}

WAL::WAL(const std::string &dir, const std::shared_ptr<base::ThreadPool> &pool_,
         int segment_size)
    : dir_(dir), pool_(pool_), segment_size(segment_size), done_pages(0) {
  if (!boost::filesystem::create_directories(dir)) {
    LOG_INFO << "WAL Directory existed: " << dir;
  } else {
    LOG_INFO << "create WAL Directory: " << dir;
  }

  std::pair<std::pair<int, int>, error::Error> rp = segments(dir);
  if (rp.second) {
    err_.set(rp.second);
    return;
  }

  page = std::unique_ptr<Page>(new Page());

  std::shared_ptr<Segment> segment_(new Segment(dir, rp.first.second));
  if (segment_->err_) {
    err_.set(segment_->err_);
    return;
  }

  err_.set(set_segment(segment_));
}

std::pair<std::pair<int, int>, error::Error> WAL::segments(
    const std::string &dir) {
  std::pair<std::vector<SegmentRef>, error::Error> segments_pair =
      list_segments(dir);
  if (segments_pair.second) return {{0, 0}, segments_pair.second};
  if (segments_pair.first.empty()) return {{0, 0}, segments_pair.second};
  return std::pair<std::pair<int, int>, error::Error>(
      std::pair<int, int>(segments_pair.first.front().index,
                          segments_pair.first.back().index),
      segments_pair.second);
}

error::Error WAL::set_segment(const std::shared_ptr<Segment> &s) {
  segment = s;
  if (fseek(s->f, 0L, SEEK_END) != 0) return error::Error("error seek");
  int size_ = ftell(s->f);
  if (fseek(s->f, 0L, SEEK_SET) != 0) return error::Error("error seek");
  done_pages = size_ / PAGE_SIZE;
  return error::Error();
}

// next_segment creates the next segment and closes the previous one.
error::Error WAL::next_segment() {
  // Only flush the current page if it actually holds data.
  if (page->alloc > 0) {
    error::Error err = flush_page(true);
    if (err) return err;
  }
  // std::cerr << "segment->index_ " << segment->index_ << std::endl;
  std::shared_ptr<Segment> next(new Segment(dir_, segment->index_ + 1));
  if (next->err_) return error::wrap(next->err_, "create new segment file");

  // Don't block further writes by fsyncing the last segment.
  pool_->run(boost::bind(&close_segment, segment));

  error::Error err = set_segment(next);
  if (err) return err;

  return error::Error();
}

// flush_page writes the new contents of the page to disk. If no more records
// will fit into the page, the remaining bytes will be set to zero and a new
// page will be started. If clear is true, this is enforced regardless of how
// many bytes are left in the page.
error::Error WAL::flush_page(bool clear) {
  clear = clear || page->full();

  // No more data will fit into the page. Enqueue and clear it.
  if (clear && page->alloc > 0) {
    page->alloc = PAGE_SIZE;  // Write till end of page.
  }
  int to_write = page->alloc - page->flushed;
  if (fwrite(page->buf_ + page->flushed, 1, to_write, segment->f) != to_write)
    return error::Error("error fwrite");
  page->flushed += to_write;

  // We flushed an entire page, prepare a new one.
  if (clear) {
    page->reset(true);
    ++done_pages;
  }
  return error::Error();
}

// log writes rec to the log and forces a flush of the current page if its
// the final record of a batch, the record is bigger than the page size or
// the current page is full.
error::Error WAL::log(const std::vector<uint8_t> &rec, bool final) {
  base::RWLockGuard lock(mutex_, 1);

  // If the record is too big to fit within the active page in the current
  // segment, terminate the active segment and advance to the next one.
  // This ensures that records do not cross segment boundaries.
  int available =
      page->remaining() - HEADER_SIZE +
      (PAGE_SIZE - HEADER_SIZE) * (segment_size / PAGE_SIZE - done_pages - 1);
  // std::cerr << "done_pages: " << done_pages << " page->remaining(): " <<
  // page->remaining() << " available: " << available << " rec.size(): " <<
  // rec.size() << std::endl; NOTICE(Alec), Here need to cast to int first
  // because available may be negative.
  if (static_cast<int>(rec.size()) > available) {
    error::Error err = next_segment();
    if (err) return err;
  }

  // Populate as many pages as necessary to fit the record.
  // Be careful to always do one pass to ensure we write zero-length records.
  int i = 0;
  int remaining = rec.size();
  const uint8_t *index = &(rec[0]);
  while (i == 0 || remaining > 0) {
    // If page cannot fit the whole header, continue next page.
    if ((PAGE_SIZE - page->alloc) - HEADER_SIZE <= 0) {
      error::Error err = flush_page(true);
      if (err) return err;
    }
    int l = std::min(remaining, (PAGE_SIZE - page->alloc) - HEADER_SIZE);
    RECORD_TYPE type;
    if (i == 0 && l == remaining)
      type = RECORD_FULL;
    else if (l == remaining)
      type = RECORD_LAST;
    else if (i == 0)
      type = RECORD_FIRST;
    else
      type = RECORD_MIDDLE;
    // LOG_INFO << "type: " << type << "remaining: " << remaining;
    page->buf_[page->alloc] = type;
    ++page->alloc;
    base::put_uint16_big_endian(page->buf_ + page->alloc, l);
    page->alloc += 2;
    uint32_t crc = base::GetCrc32(index, l);
    base::put_uint32_big_endian(page->buf_ + page->alloc, crc);
    page->alloc += 4;
    // LOG_DEBUG << "remaining:" << remaining << " page->alloc:" << page->alloc
    // << " l:" << l;
    memcpy(page->buf_ + page->alloc, index, l);
    // LOG_DEBUG << "after memcpy";
    index += l;
    remaining -= l;
    page->alloc += l;

    // By definition when a record is split it means its size is bigger than
    // the page boundary so the current page would be full and needs to be
    // flushed. On contrary if we wrote a full record, we can fit more records
    // of the batch into the page before flushing it.
    if (final || type != RECORD_FULL || page->full()) {
      error::Error err = flush_page(false);
      if (err) return err;
    }
    ++i;
  }
  return error::Error();
}

error::Error WAL::log(const std::vector<std::vector<uint8_t>> &recs) {
  for (int i = 0; i < recs.size(); i++) {
    error::Error err = log(recs[i], i == recs.size() - 1);
    if (err) return err;
  }
  return error::Error();
}

error::Error WAL::log(const uint8_t *p, int length, bool final) {
  base::RWLockGuard lock(mutex_, 1);

  // If the record is too big to fit within the active page in the current
  // segment, terminate the active segment and advance to the next one.
  // This ensures that records do not cross segment boundaries.
  int available =
      page->remaining() - HEADER_SIZE +
      (PAGE_SIZE - HEADER_SIZE) * (segment_size / PAGE_SIZE - done_pages - 1);
  if (length > available) {
    error::Error err = next_segment();
    if (err) return err;
  }

  // Populate as many pages as necessary to fit the record.
  // Be careful to always do one pass to ensure we write zero-length records.
  int i = 0;
  while (i == 0 || length > 0) {
    // If page cannot fit the whole header, continue next page.
    if ((PAGE_SIZE - page->alloc) - HEADER_SIZE <= 0) {
      error::Error err = flush_page(true);
      if (err) return err;
    }
    int l = std::min(length, (PAGE_SIZE - page->alloc) - HEADER_SIZE);
    RECORD_TYPE type;
    if (i == 0 && l == length)
      type = RECORD_FULL;
    else if (l == length)
      type = RECORD_LAST;
    else if (i == 0)
      type = RECORD_FIRST;
    else
      type = RECORD_MIDDLE;
    page->buf_[page->alloc] = type;
    ++page->alloc;
    base::put_uint16_big_endian(page->buf_ + page->alloc, l);
    page->alloc += 2;
    uint32_t crc = base::GetCrc32(p, l);
    base::put_uint32_big_endian(page->buf_ + page->alloc, crc);
    page->alloc += 4;
    memcpy(page->buf_ + page->alloc, p, l);
    p += l;
    length -= l;
    page->alloc += l;

    // By definition when a record is split it means its size is bigger than
    // the page boundary so the current page would be full and needs to be
    // flushed. On contrary if we wrote a full record, we can fit more records
    // of the batch into the page before flushing it.
    if (final || type != RECORD_FULL || page->full()) {
      error::Error err = flush_page(false);
      if (err) return err;
    }
    ++i;
  }
  return error::Error();
}

// Repair attempts to repair the WAL based on the error.
// It discards all data after the corruption.
// No lock here.
error::Error WAL::repair(const CorruptionError &cerr) {
  // We could probably have a mode that only discards torn records right around
  // the corruption to preserve as data much as possible.
  // But that's not generally applicable if the records have any kind of
  // causality. Maybe as an extra mode in the future if mid-WAL corruptions
  // become a frequent concern.
  if (cerr.segment == -1) return error::Error("cannot handle error");

  if (cerr.segment < 0)
    return error::Error("corruption error does not specify position");
  LOG_WARN << "msg=\"Starting corruption repair\" segment=" << cerr.segment
           << " offset=" << cerr.offset;

  // All segments after the corruption can no longer be used.
  std::pair<std::vector<SegmentRef>, error::Error> sp = list_segments(dir_);
  if (sp.second) return sp.second;

  for (auto const &ref : sp.first) {
    if (segment->index_ == ref.index) {
      flush_page(false);
      segment.reset();
    }
    if (ref.index <= cerr.segment) continue;
    if (std::remove(ref.name.c_str()) != 0)
      return error::Error("error remove " + ref.name);
    LOG_WARN << "WAL::repair remove " << ref.name;
  }
  // Regardless of the corruption offset, no record reaches into the previous
  // segment. So we can safely repair the WAL by removing the segment and
  // re-inserting all its records up to the corruption.
  std::string corrupted_file = segment_name(dir_, cerr.segment);
  if (!boost::filesystem::exists(corrupted_file)) return error::Error();
  LOG_WARN << "msg=\"rewrite corrupted segment\" segment=" << cerr.segment;

  std::string temp_file = corrupted_file + ".repair";
  boost::filesystem::rename(corrupted_file, temp_file);
  // Create a clean segment and make it the active one.
  std::shared_ptr<Segment> s(new Segment(dir_, cerr.segment));
  if (s->err_) return s->err_;
  error::Error err = set_segment(s);
  if (err) return err;

  page->reset(true);

  SegmentReader reader(temp_file, dir_, cerr.segment);
  if (reader.error())
    return error::wrap(reader.error(), "create SegmentReader");

  while (reader.next()) {
    // Add records only up to the where the error was.
    // LOG_DEBUG << "offset: " << reader.offset();
    if (reader.offset() >= cerr.offset) break;
    auto rec_pair = reader.record();
    // LOG_DEBUG << "rec length: " << rec_pair.second;
    err = log(rec_pair.first, rec_pair.second);
    if (err) {
      reader.clear();
      std::remove(temp_file.c_str());
      return error::wrap(err, "insert record");
    }
  }
  // We expect an error here from r.Err(), so nothing to handle.

  reader.clear();
  std::remove(temp_file.c_str());
  return error::Error();
}

// truncate drops all segments before i.
error::Error WAL::truncate(int i) {
  if (segment->index() < i) close_segment(segment);
  auto refs = list_segments(dir_);
  if (refs.second) return refs.second;
  for (auto const &ref : refs.first) {
    if (ref.index >= i) break;
    if (std::remove(ref.name.c_str()) != 0)
      return error::Error("error remove " + ref.name);
  }
  return error::Error();
}

void WAL::sync() {
  if (segment) {
    // flush data from user space to kernel space.
    fflush(segment->f);
    // flush data from kernel space to disk.
    fsync(fileno(segment->f));
  }
}

WAL::~WAL() {
  LOG_INFO << "close WAL: " << dir_;
  base::RWLockGuard lock(mutex_, 1);
  flush_page(true);
  close_segment(segment);
}

error::Error validate_record(int i, RECORD_TYPE type) {
  switch (type) {
    case 1:
      if (i != 0) return error::Error("unexpected full record");
      return error::Error();
    case 2:
      if (i != 0)
        return error::Error("unexpected first record, dropping buffer");
      return error::Error();
    case 3:
      if (i == 0)
        return error::Error("unexpected middle record, dropping buffer");
      return error::Error();
    case 4:
      if (i == 0)
        return error::Error("unexpected last record, dropping buffer");
      return error::Error();
    default:
      return error::Error("unexpected record type " + std::to_string(type));
  }
}

SegmentReader::SegmentReader(const std::string &dir, bool directory)
    : segment_index(0),
      segment_offset(0),
      offset_reset(false),
      page_offset(0),
      eof(false) {
  if (directory) {
    std::pair<std::vector<SegmentRef>, error::Error> sp = list_segments(dir);
    if (sp.second) {
      err_.set(sp.second);
      return;
    }
    segments.reserve(sp.first.size());
    for (auto const &s : sp.first) {
      segments.emplace_back(new Segment(s.name));
      if (segments.back()->err_) {
        err_.set(segments.back()->err_);
        return;
      }
    }
  } else {
    segments.emplace_back(new Segment(dir));
    if (segments.back()->err_) {
      err_.set(segments.back()->err_);
      return;
    }
  }
  if (!read_page()) {
    if (err_) {
      err_.set("read_page()");
      return;
    }
  }
  record_.reserve(PAGE_SIZE / 2);
}

SegmentReader::SegmentReader(const SegmentRef &ref)
    : segment_index(0),
      segment_offset(0),
      offset_reset(false),
      page_offset(0),
      eof(false) {
  segments.emplace_back(new Segment(ref.name));
  if (segments.back()->err_) {
    err_.set(segments.back()->err_);
    return;
  }
  if (!read_page()) {
    if (err_) {
      err_.set("read_page()");
      return;
    }
  }
  record_.reserve(PAGE_SIZE / 2);
}

SegmentReader::SegmentReader(const std::string &name, const std::string &dir,
                             int index)
    : segment_index(0),
      segment_offset(0),
      offset_reset(false),
      page_offset(0),
      eof(false) {
  segments.emplace_back(new Segment(name, dir, index));
  if (segments.back()->err_) {
    err_.set(segments.back()->err_);
    return;
  }
  if (!read_page()) {
    if (err_) {
      err_.set("read_page()");
      return;
    }
  }
  record_.reserve(PAGE_SIZE / 2);
}

SegmentReader::SegmentReader(const std::deque<SegmentRange> &segs)
    : segment_index(0),
      segment_offset(0),
      offset_reset(false),
      page_offset(0),
      eof(false) {
  for (const SegmentRange &seg : segs) {
    std::pair<std::vector<SegmentRef>, error::Error> sp =
        list_segments(seg.dir);
    if (sp.second) {
      err_.set(sp.second);
      return;
    }
    // std::cerr << "num: " << sp.first.size() << std::endl;
    for (const SegmentRef &r : sp.first) {
      if (seg.first >= 0 && r.index < seg.first) continue;
      if (seg.last >= 0 && r.index > seg.last) break;
      // std::cerr << r.name << std::endl;
      segments.emplace_back(new Segment(r.name));
      if (segments.back()->err_) {
        err_.set(segments.back()->err_);
        return;
      }
    }
    // std::cerr << "num: " << segments.size() << std::endl;
  }
  if (!read_page()) {
    if (err_) {
      err_.set("read_page()");
      return;
    }
  }
  record_.reserve(PAGE_SIZE / 2);
}

// Read one page from current segment.
// Pad zeros if page size < PAGE_SIZE.
bool SegmentReader::read_page() {
  if (segment_index == segments.size()) {
    eof = true;
    return false;
  }
  if (offset_reset) {
    offset_reset = false;
    segment_offset = 0;
  }
  int read = fread(buf, 1, PAGE_SIZE, segments[segment_index]->f);
  page_offset = 0;
  if (read != PAGE_SIZE) {
    if (feof(segments[segment_index]->f)) {
      offset_reset = true;
      ++segment_index;
      if (read == 0) return read_page();
      memset(buf + read, 0, PAGE_SIZE - read);
    } else {
      err_.set("error fread");
      return false;
    }
  }
  return true;
}

bool SegmentReader::next() {
  if (err_ || eof) return false;
  record_.clear();

  int i = 0;
  while (true) {
    // If the remaining space of the page cannot fit the whole header, continue
    // next page.
    if (page_offset + HEADER_SIZE >= PAGE_SIZE) {
      if (!read_page()) {  // EOF or error.
        if (err_) err_.set("read_page()");
        return false;
      }
    }

    record_type = buf[page_offset];
    ++page_offset;
    // std::cerr << "type: " << record_type << std::endl;
    if (record_type == RECORD_PAGE_TERM) {
      if (page_offset == PAGE_SIZE) continue;

      // We are pedantic and check whether the zeros are actually up
      // to a page boundary.
      // It's not strictly necessary but may catch sketchy state early.
      for (; page_offset < PAGE_SIZE; ++page_offset) {
        if (buf[page_offset] != 0) {
          err_.set("unexpected non-zero byte in padded page");
          return false;
        }
      }
      continue;
    }

    err_ = validate_record(i, record_type);
    if (err_) return false;

    int length = base::get_uint16_big_endian(buf + page_offset);
    page_offset += 2;
    uint32_t crc32 = base::get_uint32_big_endian(buf + page_offset);
    page_offset += 4;

    // fix error of empty record.
    if (length == 0) return next();
    if (length + page_offset > PAGE_SIZE) {
      err_.set("invalid record size " + std::to_string(length) + ", expected " +
               std::to_string(PAGE_SIZE - page_offset));
      return false;
    }

    uint32_t validate_crc32 = base::GetCrc32(buf + page_offset, length);
    if (crc32 != validate_crc32) {
      err_.set("invalid checksum " + std::to_string(validate_crc32) +
               ", expected " + std::to_string(crc32));
      return false;
    }
    for (int j = page_offset; j < page_offset + length; ++j)
      record_.push_back(buf[j]);
    page_offset += length;

    segment_offset += HEADER_SIZE + length;

    if (record_type == RECORD_FULL || record_type == RECORD_LAST) return true;
    // Only increment i for non-zero records since we use it
    // to determine valid content record sequences.
    ++i;
  }
}

error::Error SegmentReader::error() { return err_; }

CorruptionError SegmentReader::cerror() {
  if (err_)
    return CorruptionError(segments[segment_index]->dir_,
                           segments[segment_index]->index_, segment_offset,
                           err_);
  return CorruptionError();
}

// <ptr, length>.
std::pair<uint8_t *, int> SegmentReader::record() {
  return std::make_pair(&(record_[0]), record_.size());
}

int SegmentReader::segment() {
  if (segments.empty()) return -1;
  return segments[segment_index]->index_;
}

int SegmentReader::offset() { return segment_offset; }

// Close all Segments.
void SegmentReader::clear() { segments.clear(); }

}  // namespace wal
}  // namespace tsdb