#include "tombstone/TombstoneUtils.hpp"

#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <cstdio>

#include "tombstone/Interval.hpp"
#include "tombstone/MemTombstones.hpp"
#include "tsdbutil/DecBuf.hpp"
#include "tsdbutil/MMapSlice.hpp"
#include "tsdbutil/tsdbutils.hpp"

namespace tsdb {
namespace tombstone {

const uint32_t MAGIC_TOMBSTONE = 0xA1ECA1EC;
const uint8_t TOMBSTONE_FORMAT = 1;

// NOTICE
// boost::bind will still make a copy for reference
void write_ts_helper(FILE *f, tsdbutil::EncBuf &enc_buf, base::CRC32 &crc32,
                     uint64_t ref, const Intervals &itvs) {
  for (const Interval &itvl : itvs) {
    enc_buf.put_unsigned_variant(ref);
    enc_buf.put_signed_variant(itvl.min_time);
    enc_buf.put_signed_variant(itvl.max_time);
    fwrite(enc_buf.get().first, 1, enc_buf.len(), f);
    crc32.process_bytes(enc_buf.get());
    enc_buf.reset();
  }
}

bool write_tombstones(const std::string &dir, TombstoneReaderInterface *tr) {
  std::string p = tsdbutil::filepath_join(dir, "tombstones");
  std::string tmp_path(p + std::string(".tmp"));
  FILE *f;
  if (!(f = fopen(tmp_path.c_str(), "wb"))) return false;

  // CRC32 object.
  base::CRC32 crc32;

  // Write header.
  tsdbutil::EncBuf enc_buf(3 * base::MAX_VARINT_LEN_64);
  enc_buf.put_BE_uint32(MAGIC_TOMBSTONE);
  enc_buf.put_byte(TOMBSTONE_FORMAT);
  fwrite(enc_buf.get().first, 1, 5, f);
  enc_buf.reset();

  // Iterate writing.
  if (tr != NULL)
    tr->iter(static_cast<boost::function<void(uint64_t, const Intervals &)>>(
        boost::bind(&write_ts_helper, f, boost::ref(enc_buf), boost::ref(crc32),
                    _1, _2)));

  // Write checksum.
  enc_buf.put_BE_uint32(crc32.checksum());

  fwrite(enc_buf.get().first, 1, 4, f);

  fclose(f);

  std::rename(tmp_path.c_str(), p.c_str());
  return true;
}

bool write_tombstones(const std::string &dir,
                      const std::shared_ptr<TombstoneReaderInterface> &tr) {
  std::string p = tsdbutil::filepath_join(dir, "tombstones");
  std::string tmp_path(p + std::string(".tmp"));
  FILE *f;
  if (!(f = fopen(tmp_path.c_str(), "wb"))) return false;

  // CRC32 object.
  base::CRC32 crc32;

  // Write header.
  tsdbutil::EncBuf enc_buf(3 * base::MAX_VARINT_LEN_64);
  enc_buf.put_BE_uint32(MAGIC_TOMBSTONE);
  enc_buf.put_byte(TOMBSTONE_FORMAT);
  fwrite(enc_buf.get().first, 1, 5, f);
  enc_buf.reset();

  // Iterate writing.
  tr->iter(static_cast<boost::function<void(uint64_t, const Intervals &)>>(
      boost::bind(&write_ts_helper, f, boost::ref(enc_buf), boost::ref(crc32),
                  _1, _2)));

  // Write checksum.
  enc_buf.put_BE_uint32(crc32.checksum());

  fwrite(enc_buf.get().first, 1, 4, f);

  fclose(f);

  std::rename(tmp_path.c_str(), p.c_str());
  return true;
}

// Return pair of MemTombstone and len of tombstone file.
// Check error by checking if pair.first is null.
std::pair<std::shared_ptr<TombstoneReaderInterface>, uint64_t> read_tombstones(
    const std::string &dir) {
  std::string p = tsdbutil::filepath_join(dir, "tombstones");

  if (!boost::filesystem::exists(p))
    return std::pair<std::shared_ptr<TombstoneReaderInterface>, uint64_t>(
        std::shared_ptr<TombstoneReaderInterface>(new MemTombstones()), 0);

  FILE *f;
  if (!(f = fopen(p.c_str(), "rb")))
    return std::make_pair(std::shared_ptr<TombstoneReaderInterface>(), 0);

  tsdbutil::MMapSlice bs(p);
  if (bs.len() < 9)
    return std::make_pair(std::shared_ptr<TombstoneReaderInterface>(), 0);
  std::pair<const uint8_t *, int> start = bs.range(0, bs.len() - 4);
  // Check magic
  if (base::get_uint32_big_endian(start.first) != MAGIC_TOMBSTONE)
    return std::make_pair(std::shared_ptr<TombstoneReaderInterface>(), 0);
  // Check version
  if (*(start.first + 4) != 1)
    return std::make_pair(std::shared_ptr<TombstoneReaderInterface>(), 0);
  // Check checksum
  uint32_t crc32 = base::get_uint32_big_endian(start.first + start.second);
  if (base::GetCrc32(start.first + 5, start.second - 5) != crc32)
    return std::make_pair(std::shared_ptr<TombstoneReaderInterface>(), 0);

  std::shared_ptr<TombstoneReaderInterface> stones =
      std::shared_ptr<TombstoneReaderInterface>(new MemTombstones());
  tsdbutil::DecBuf dec_buf(start.first + 5, start.second - 5);
  uint64_t ref;
  int64_t min_time, max_time;
  while (dec_buf.len() > 0) {
    ref = dec_buf.get_unsigned_variant();
    min_time = dec_buf.get_signed_variant();
    max_time = dec_buf.get_signed_variant();
    if (dec_buf.error() != tsdbutil::NO_ERR)
      return std::make_pair(std::shared_ptr<TombstoneReaderInterface>(), 0);
    stones->add_interval(ref, Interval(min_time, max_time));
  }
  return std::pair<std::shared_ptr<TombstoneReaderInterface>, uint64_t>(
      stones, bs.len());
}

}  // namespace tombstone
}  // namespace tsdb