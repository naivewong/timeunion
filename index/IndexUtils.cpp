#include "index/IndexUtils.hpp"

#include <limits>

namespace tsdb {
namespace index {

const uint32_t MAGIC_INDEX = 0x19960514;
const int HEADER_LEN = 5;
const std::string LABEL_NAME_SEPARATOR = "\xff";
const uint8_t INDEX_VERSION_V1 = 1;
const uint8_t INDEX_VERSION_V2 = 2;

const int SUCCEED = 0;
const int INVALID_STAGE = -1;
const int OUT_OF_ORDER = -2;
const int EXISTED = -3;
const int CANNOT_FIND = -4;
const int INVALID_LENGTH = -5;
const int RW_ERROR = -6;

std::string error_string(int err) {
  switch (err) {
    case INVALID_STAGE:
      return "invalid stage";
    case OUT_OF_ORDER:
      return "out of order";
    case EXISTED:
      return "existed";
    case CANNOT_FIND:
      return "cannot find";
    case INVALID_LENGTH:
      return "invalid length";
    case RW_ERROR:
      return "read write error";
    default:
      return "invalid error";
  }
}

const IndexWriterStage IDX_STAGE_NONE = 0;
const IndexWriterStage IDX_STAGE_SYMBOLS = 1;
const IndexWriterStage IDX_STAGE_SERIES = 2;
const IndexWriterStage IDX_STAGE_LABEL_INDEX = 3;
const IndexWriterStage IDX_STAGE_POSTINGS = 4;
const IndexWriterStage IDX_STAGE_GROUP_POSTINGS = 5;
const IndexWriterStage IDX_STAGE_SST = 6;
const IndexWriterStage IDX_STAGE_DONE = 7;

std::string stage_to_string(IndexWriterStage iws) {
  switch (iws) {
    case IDX_STAGE_NONE:
      return "none";
    case IDX_STAGE_SYMBOLS:
      return "symbols";
    case IDX_STAGE_SERIES:
      return "series";
    case IDX_STAGE_LABEL_INDEX:
      return "label index";
    case IDX_STAGE_POSTINGS:
      return "postings";
    case IDX_STAGE_GROUP_POSTINGS:
      return "group postings";
    case IDX_STAGE_SST:
      return "sst_table";
    case IDX_STAGE_DONE:
      return "done";
  }
  return "unknown";
}

uint64_t symbol_table_size(
    const std::shared_ptr<block::IndexReaderInterface> &indexr) {
  uint64_t size = 0;
  for (const std::string &s : indexr->symbols()) size += s.length() + 8;
  return size;
}

const uint64_t ALL_GROUP_POSTINGS = std::numeric_limits<uint64_t>::max();

}  // namespace index
}  // namespace tsdb