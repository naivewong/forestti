#include "chunk/ChunkWriter.hpp"

#include <boost/filesystem.hpp>

#include "base/Checksum.hpp"
#include "base/Logging.hpp"
#include "chunk/ChunkUtils.hpp"
#include "head/HeadUtils.hpp"
#include "leveldb/options.h"
#include "leveldb/slice.h"

namespace tsdb {
namespace chunk {

// Implicit construct from const char *
ChunkWriter::ChunkWriter(const std::string &dir)
    : dir(dir), pos(0), chunk_size(DEFAULT_CHUNK_SIZE) {
  boost::filesystem::path block_dir = boost::filesystem::path(dir);
  boost::filesystem::create_directories(block_dir);
  // if(!boost::filesystem::create_directories(block_dir)){
  //     LOG_INFO << "Block Directory existed: " << block_dir.string();
  // }
  // else{
  //     LOG_INFO << "Create Block Directory: " << block_dir.string();
  // }
}

FILE *ChunkWriter::tail() {
  if (files.empty()) return NULL;
  return files.back();
}

// finalize_tail writes all pending data to the current tail file and close it
void ChunkWriter::finalize_tail() {
  if (!files.empty()) {
    fflush(files.back());
    fclose(files.back());
  }
}

void ChunkWriter::cut() {
  // Sync current tail to disk and close.
  finalize_tail();
  auto p = next_sequence_file(dir);
  FILE *f = fopen(p.second.c_str(), "wb+");

  // Write header metadata for new file.
  uint8_t temp[8];
  base::put_uint32_big_endian(temp, MAGIC_CHUNK);
  base::put_uint32_big_endian(temp + 4, CHUNK_FORMAT_V1);
  fwrite(temp, 1, 8, f);
  files.push_back(f);
  file_names.push_back(p.second);
  seqs.push_back(p.first);
  pos = 8;
}

void ChunkWriter::write(const uint8_t *bytes, int size) {
  int written =
      fwrite(reinterpret_cast<const void *>(bytes), 1, size, files.back());
  pos += written;
}

void ChunkWriter::write_chunks(
    const std::deque<std::shared_ptr<ChunkMeta>> &chunks) {
  uint64_t max_len = 0;
  for (auto &ptr : chunks) {
    max_len += 5 + base::MAX_VARINT_LEN_32;
    max_len += static_cast<uint64_t>(ptr->chunk->size());
  }

  if (files.empty() || pos > chunk_size ||
      (pos + max_len > chunk_size && max_len <= chunk_size))
    cut();

  uint8_t b[base::MAX_VARINT_LEN_32];
  uint64_t sequence = (seq() << 32);
  for (auto &chk : chunks) {
    chk->ref = sequence | static_cast<uint64_t>(pos);

    // Write len of chk->chunk->bytes()
    int encoded = base::encode_unsigned_varint(
        b, static_cast<uint64_t>(chk->chunk->size()));
    write(b, encoded);

    // Write encoding
    b[0] = static_cast<uint8_t>(chk->chunk->encoding());
    write(b, 1);

    // Write data
    write(chk->chunk->bytes(), chk->chunk->size());

    // Write crc32
    base::put_uint32_big_endian(
        b, base::GetCrc32(chk->chunk->bytes(), chk->chunk->size()));
    write(b, 4);
  }
}

uint64_t ChunkWriter::seq() {
  // return static_cast<uint64_t>(files.size() - 1);
  return static_cast<uint64_t>(seqs.back());
}

void ChunkWriter::close() { finalize_tail(); }

ChunkWriter::~ChunkWriter() { close(); }

void LevelDBChunkWriter::write_chunks(
    const std::deque<std::shared_ptr<ChunkMeta>> &chunks) {
  for (auto &chk : chunks) {
    uint8_t *buf =
        new uint8_t[base::MAX_VARINT_LEN_32 + 5 + chk->chunk->size()];

    // Write len of chk->chunk->bytes()
    int encoded = base::encode_unsigned_varint(
        buf, static_cast<uint64_t>(chk->chunk->size()));

    // Write encoding
    buf[encoded] = static_cast<uint8_t>(chk->chunk->encoding());

    // Write data
    memcpy(buf + encoded + 1, chk->chunk->bytes(), chk->chunk->size());

    // Write crc32
    base::put_uint32_big_endian(
        buf + encoded + 1 + chk->chunk->size(),
        base::GetCrc32(chk->chunk->bytes(), chk->chunk->size()));

    // Generate LevelDB's key.
    // The left part of ref is the series ID.
    uint64_t sid, cid;
    std::tie(sid, cid) = head::unpack_chunk_id(chk->ref);
    chk->ref = sid;
    // printf("encoded:%d l:%lu ref:%lu min_time:%ld\n", encoded,
    // chk->chunk->size(), chk->ref, chk->min_time);
    uint8_t key[12];
    base::put_uint32_big_endian(key, sid);
    base::put_uint64_big_endian(key + 4, chk->min_time);

    // Write to LevelDB.
    db_->Put(leveldb::WriteOptions(),
             leveldb::Slice(reinterpret_cast<const char *>(key), 12),
             leveldb::Slice(reinterpret_cast<const char *>(buf),
                            encoded + 5 + chk->chunk->size()));

    delete[] buf;
  }
}

}  // namespace chunk
}  // namespace tsdb