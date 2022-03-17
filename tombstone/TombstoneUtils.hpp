#ifndef TOMBSTONEUTILS_H
#define TOMBSTONEUTILS_H

// #include <boost/core/ref.hpp>

#include "base/Checksum.hpp"
#include "tombstone/TombstoneReaderInterface.hpp"
#include "tsdbutil/EncBuf.hpp"

namespace tsdb {
namespace tombstone {

extern const uint32_t MAGIC_TOMBSTONE;
extern const uint8_t TOMBSTONE_FORMAT;

// NOTICE
// boost::bind will still make a copy for reference
void write_ts_helper(FILE *f, tsdbutil::EncBuf &enc_buf, base::CRC32 &crc32,
                     uint64_t ref, const Intervals &itvs);

bool write_tombstones(const std::string &dir, TombstoneReaderInterface *tr);

bool write_tombstones(const std::string &dir,
                      const std::shared_ptr<TombstoneReaderInterface> &tr);

// Return pair of MemTombstone and len of tombstone file.
// Check error by checking if pair.first is null.
std::pair<std::shared_ptr<TombstoneReaderInterface>, uint64_t> read_tombstones(
    const std::string &dir);

}  // namespace tombstone
}  // namespace tsdb

#endif