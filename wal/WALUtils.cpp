#include "wal/WALUtils.hpp"

namespace tsdb {
namespace wal {

// WAL general record.
const RECORD_TYPE RECORD_PAGE_TERM = 0;
const RECORD_TYPE RECORD_FULL = 1;
const RECORD_TYPE RECORD_FIRST = 2;
const RECORD_TYPE RECORD_MIDDLE = 3;
const RECORD_TYPE RECORD_LAST = 4;
std::string record_type_string(RECORD_TYPE type_) {
  switch (type_) {
    case 0:
      return "zero";
    case 1:
      return "full";
    case 2:
      return "first";
    case 3:
      return "middle";
    case 4:
      return "last";
    default:
      return "<invalid>";
  }
}

// WAL meta.
const int SEGMENT_SIZE = 128 * 1024 * 1024;
const int PAGE_SIZE = 32 * 1024;
const int HEADER_SIZE = 7;

}  // namespace wal
}  // namespace tsdb