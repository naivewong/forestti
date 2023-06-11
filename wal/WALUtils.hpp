#ifndef WALUTILS_H
#define WALUTILS_H

#include <stdint.h>

#include <string>

namespace tsdb {
namespace wal {

// WAL general record.
typedef uint8_t RECORD_TYPE;
extern const RECORD_TYPE RECORD_PAGE_TERM;
extern const RECORD_TYPE RECORD_FULL;
extern const RECORD_TYPE RECORD_FIRST;
extern const RECORD_TYPE RECORD_MIDDLE;
extern const RECORD_TYPE RECORD_LAST;
std::string record_type_string(RECORD_TYPE type_);

// WAL meta.
extern const int SEGMENT_SIZE;
extern const int PAGE_SIZE;
extern const int HEADER_SIZE;

}  // namespace wal
}  // namespace tsdb

#endif