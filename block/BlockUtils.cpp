#include "block/BlockUtils.hpp"

#include "base/Logging.hpp"
#include "third_party/rapidjson/document.h"
#include "third_party/rapidjson/prettywriter.h"
#include "third_party/rapidjson/rapidjson.h"
#include "third_party/rapidjson/stringbuffer.h"

namespace tsdb {
namespace block {

const std::string INDEX_FILE_NAME = "index";
const std::string META_FILE_NAME = "meta.json";

std::pair<BlockMeta, bool> read_block_meta(const std::string &dir) {
  boost::filesystem::path p =
      boost::filesystem::path(dir) / boost::filesystem::path(META_FILE_NAME);

  if (!boost::filesystem::exists(p)) {
    LOG_ERROR << p.string() << " not existed";
    return {BlockMeta(), false};
  }

  FILE *f = fopen(p.string().c_str(), "rb");
  if (f == NULL) {
    LOG_ERROR << "cannot open meta.json";
    return {BlockMeta(), false};
  }
  fseek(f, 0L, SEEK_END);
  long int size = ftell(f);
  fseek(f, 0L, SEEK_SET);

  char *content = new char[size + 1];
  if (fread(content, 1, size, f) != size) {
    fclose(f);
    LOG_ERROR << "error read meta.json";
    return {BlockMeta(), false};
  }
  // Very Important step!
  // Otherwise would cause WTF bug when parsing the document.
  content[size] = '\0';
  fclose(f);

  rapidjson::Document d;
  d.Parse(content);

  BlockMeta meta;

  if (d.HasMember("ulid")) {
    if (!d["ulid"].IsString()) {
      return {BlockMeta(), false};
    }
    meta.ulid_ = ulid::Unmarshal(d["ulid"].GetString());
  }
  if (d.HasMember("maxTime")) {
    if (!d["maxTime"].IsInt64()) {
      return {BlockMeta(), false};
    }
    meta.max_time = d["maxTime"].GetInt64();
  }
  if (d.HasMember("minTime")) {
    if (!d["minTime"].IsInt64()) {
      return {BlockMeta(), false};
    }
    meta.min_time = d["minTime"].GetInt64();
  }
  if (d.HasMember("version")) {
    if (!d["version"].IsInt()) {
      return {BlockMeta(), false};
    }
    meta.version = d["version"].GetInt();
  }

  if (d.HasMember("stats")) {
    if (d["stats"].HasMember("numSamples")) {
      if (!d["stats"]["numSamples"].IsUint64()) {
        return {BlockMeta(), false};
      }
      meta.stats.num_samples = d["stats"]["numSamples"].GetUint64();
    }
    if (d["stats"].HasMember("numSeries")) {
      if (!d["stats"]["numSeries"].IsUint64()) {
        return {BlockMeta(), false};
      }
      meta.stats.num_series = d["stats"]["numSeries"].GetUint64();
    }
    if (d["stats"].HasMember("numChunks")) {
      if (!d["stats"]["numChunks"].IsUint64()) {
        return {BlockMeta(), false};
      }
      meta.stats.num_chunks = d["stats"]["numChunks"].GetUint64();
    }
    if (d["stats"].HasMember("numTombstones")) {
      if (!d["stats"]["numTombstones"].IsUint64()) {
        return {BlockMeta(), false};
      }
      meta.stats.num_tombstones = d["stats"]["numTombstones"].GetUint64();
    }
    if (d["stats"].HasMember("numBytes")) {
      if (!d["stats"]["numBytes"].IsUint64()) {
        return {BlockMeta(), false};
      }
      meta.stats.num_bytes = d["stats"]["numBytes"].GetUint64();
    }
  }

  if (d.HasMember("compaction")) {
    if (d["compaction"].HasMember("level")) {
      if (!d["compaction"]["level"].IsInt()) {
        return {BlockMeta(), false};
      }
      meta.compaction.level = d["compaction"]["level"].GetInt();
    }
    if (d["compaction"].HasMember("deletable")) {
      if (!d["compaction"]["deletable"].IsBool()) {
        return {BlockMeta(), false};
      }
      meta.compaction.deletable = d["compaction"]["deletable"].GetBool();
    }
    if (d["compaction"].HasMember("failed")) {
      if (!d["compaction"]["failed"].IsBool()) {
        return {BlockMeta(), false};
      }
      meta.compaction.failed = d["compaction"]["failed"].GetBool();
    }

    if (d["compaction"].HasMember("sources")) {
      if (!d["compaction"]["sources"].IsArray()) {
        return {BlockMeta(), false};
      }
      for (rapidjson::SizeType i = 0; i < d["compaction"]["sources"].Size();
           i++) {
        if (!d["compaction"]["sources"][i].IsString()) {
          return {BlockMeta(), false};
        }
        meta.compaction.sources.push_back(
            ulid::Unmarshal(d["compaction"]["sources"][i].GetString()));
      }
    }

    if (d["compaction"].HasMember("parents")) {
      if (!d["compaction"]["parents"].IsArray()) {
        return {BlockMeta(), false};
      }
      for (rapidjson::SizeType i = 0; i < d["compaction"]["parents"].Size();
           i++) {
        BlockDesc temp;
        if (d["compaction"]["parents"][i].HasMember("ulid")) {
          if (!d["compaction"]["parents"][i]["ulid"].IsString()) {
            return {BlockMeta(), false};
          }
          temp.ulid_ = ulid::Unmarshal(
              d["compaction"]["parents"][i]["ulid"].GetString());
        }
        if (d["compaction"]["parents"][i].HasMember("minTime")) {
          if (!d["compaction"]["parents"][i]["minTime"].IsInt64()) {
            return {BlockMeta(), false};
          }
          temp.min_time = d["compaction"]["parents"][i]["minTime"].GetInt64();
        }
        if (d["compaction"]["parents"][i].HasMember("maxTime")) {
          if (!d["compaction"]["parents"][i]["maxTime"].IsInt64()) {
            return {BlockMeta(), false};
          }
          temp.max_time = d["compaction"]["parents"][i]["maxTime"].GetInt64();
        }
        meta.compaction.parents.emplace_back(temp);
      }
    }
  }

  return {meta, true};
}

bool write_block_meta(const std::string &dir, const BlockMeta &meta) {
  boost::filesystem::path p =
      boost::filesystem::path(dir) / boost::filesystem::path(META_FILE_NAME);

  FILE *f = fopen(p.string().c_str(), "wb");
  if (f == NULL) return false;

  rapidjson::Document d;
  d.SetObject();
  rapidjson::Value stats(rapidjson::kObjectType),
      compaction(rapidjson::kObjectType);

  d.AddMember(
      "ulid",
      rapidjson::Value(ulid::Marshal(meta.ulid_).data(), d.GetAllocator())
          .Move(),
      d.GetAllocator());
  d.AddMember("maxTime", meta.max_time, d.GetAllocator());
  d.AddMember("minTime", meta.min_time, d.GetAllocator());
  d.AddMember("version", meta.version, d.GetAllocator());

  stats.AddMember("numSamples", meta.stats.num_samples, d.GetAllocator());
  stats.AddMember("numSeries", meta.stats.num_series, d.GetAllocator());
  stats.AddMember("numChunks", meta.stats.num_chunks, d.GetAllocator());
  stats.AddMember("numTombstones", meta.stats.num_tombstones, d.GetAllocator());
  stats.AddMember("numBytes", meta.stats.num_bytes, d.GetAllocator());
  d.AddMember("stats", stats, d.GetAllocator());

  compaction.AddMember("level", meta.compaction.level, d.GetAllocator());
  compaction.AddMember("failed", meta.compaction.failed, d.GetAllocator());
  compaction.AddMember("deletable", meta.compaction.deletable,
                       d.GetAllocator());
  if (meta.compaction.sources.size() > 0) {
    rapidjson::Value sources(rapidjson::kArrayType);
    for (auto const &i : meta.compaction.sources)
      sources.PushBack(
          rapidjson::Value(ulid::Marshal(i).data(), d.GetAllocator()).Move(),
          d.GetAllocator());
    compaction.AddMember("sources", sources, d.GetAllocator());
  }
  if (meta.compaction.parents.size() > 0) {
    rapidjson::Value parents(rapidjson::kArrayType);
    for (auto const &i : meta.compaction.parents) {
      rapidjson::Value desc(rapidjson::kObjectType);
      desc.AddMember(
          "ulid",
          rapidjson::Value(ulid::Marshal(i.ulid_).data(), d.GetAllocator())
              .Move(),
          d.GetAllocator());
      desc.AddMember("maxTime", i.max_time, d.GetAllocator());
      desc.AddMember("minTime", i.min_time, d.GetAllocator());
      parents.PushBack(desc, d.GetAllocator());
    }
    compaction.AddMember("parents", parents, d.GetAllocator());
  }
  d.AddMember("compaction", compaction, d.GetAllocator());

  rapidjson::StringBuffer sb;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
  d.Accept(writer);

  int written = fwrite(sb.GetString(), 1, sb.GetLength(), f);
  fflush(f);
  int closed = fclose(f);
  if (written == sb.GetLength() && closed == 0)
    return true;
  else
    return false;
}

}  // namespace block
}  // namespace tsdb