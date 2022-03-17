#ifndef INDEXUTILS_H
#define INDEXUTILS_H

#include <stdint.h>

#include <string>

#include "block/IndexReaderInterface.hpp"

namespace tsdb {
namespace index {

extern const uint32_t MAGIC_INDEX;
extern const int HEADER_LEN;
extern const std::string LABEL_NAME_SEPARATOR;
extern const uint8_t INDEX_VERSION_V1;  // Original index.
extern const uint8_t INDEX_VERSION_V2;  // Group version index.

extern const int SUCCEED;
extern const int INVALID_STAGE;
extern const int OUT_OF_ORDER;
extern const int EXISTED;
extern const int CANNOT_FIND;
extern const int INVALID_LENGTH;
extern const int RW_ERROR;
std::string error_string(int err);

typedef uint8_t IndexWriterStage;
extern const IndexWriterStage IDX_STAGE_NONE;
extern const IndexWriterStage IDX_STAGE_SYMBOLS;
extern const IndexWriterStage IDX_STAGE_SERIES;
extern const IndexWriterStage IDX_STAGE_LABEL_INDEX;
extern const IndexWriterStage IDX_STAGE_POSTINGS;
extern const IndexWriterStage IDX_STAGE_GROUP_POSTINGS;
extern const IndexWriterStage IDX_STAGE_SST;
extern const IndexWriterStage IDX_STAGE_DONE;
std::string stage_to_string(IndexWriterStage iws);

uint64_t symbol_table_size(
    const std::shared_ptr<block::IndexReaderInterface> &indexr);

extern const uint64_t ALL_GROUP_POSTINGS;

}  // namespace index
}  // namespace tsdb

#endif