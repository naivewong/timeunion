#include "db/HttpParser.hpp"

void NewTags(const tsdb::label::Labels& lset, Tags* tags) {
  for (const auto& l : lset) {
    Tag* tag = tags->add_tags();
    tag->set_name(l.label);
    tag->set_value(l.value);
  }
}

void NewTags(tsdb::label::Labels* lset, Tags* tags) {
  for (int i = 0; i < tags->tags_size(); i++) {
    lset->emplace_back(tags->tags(i).name(), tags->tags(i).value());
  }
}

/**********************************************
 *                InsertSamples               *
 **********************************************/
void Add(InsertSamples* samples, const tsdb::label::Labels& lset, int64_t t,
         double v) {
  InsertSample* tmp_sample = samples->add_samples();
  tmp_sample->set_type(InsertSample_InsertType_TS);
  Tags* tmp_lset = tmp_sample->mutable_lset();
  NewTags(lset, tmp_lset);
  tmp_sample->set_t(t);
  tmp_sample->add_v(v);
}

void Add(InsertSamples* samples, uint64_t id, int64_t t, double v) {
  InsertSample* tmp_sample = samples->add_samples();
  tmp_sample->set_type(InsertSample_InsertType_TS);
  tmp_sample->set_id(id);
  tmp_sample->set_t(t);
  tmp_sample->add_v(v);
}

void Add(InsertSamples* samples, const tsdb::label::Labels& group_lset,
         const std::vector<tsdb::label::Labels>& individual_lsets,
         int64_t timestamp, const std::vector<double>& values) {
  InsertSample* tmp_sample = samples->add_samples();
  tmp_sample->set_type(InsertSample_InsertType_GROUP);
  Tags* tmp_lset = tmp_sample->mutable_lset();
  NewTags(group_lset, tmp_lset);
  for (const auto& lset : individual_lsets) {
    tmp_lset = tmp_sample->add_lsets();
    NewTags(lset, tmp_lset);
  }
  tmp_sample->set_t(timestamp);
  for (double v : values) tmp_sample->add_v(v);
}

void Add(InsertSamples* samples, uint64_t ref, int64_t timestamp,
         const std::vector<double>& values) {
  InsertSample* tmp_sample = samples->add_samples();
  tmp_sample->set_type(InsertSample_InsertType_GROUP);
  tmp_sample->set_id(ref);
  tmp_sample->set_t(timestamp);
  for (double v : values) tmp_sample->add_v(v);
}

void Add(InsertSamples* samples, uint64_t ref, const std::vector<int>& slots,
         int64_t timestamp, const std::vector<double>& values) {
  InsertSample* tmp_sample = samples->add_samples();
  tmp_sample->set_type(InsertSample_InsertType_GROUP);
  tmp_sample->set_id(ref);
  for (int slot : slots) tmp_sample->add_slots(slot);
  tmp_sample->set_t(timestamp);
  for (double v : values) tmp_sample->add_v(v);
}

/**********************************************
 *                InsertResult                *
 **********************************************/
void Add(InsertResults* results, uint64_t id, const std::vector<int>& slots) {
  InsertResult* result = results->add_results();
  result->set_id(id);
  for (int i : slots) result->add_slots(i);
}

/**********************************************
 *                QueryRequest                *
 **********************************************/
void Add(QueryRequest* request, bool return_metric,
         const tsdb::label::Labels& matchers, int64_t mint, int64_t maxt) {
  request->set_return_metric(return_metric);
  Tags* tags = request->mutable_lset();
  NewTags(matchers, tags);
  request->set_mint(mint);
  request->set_maxt(maxt);
}

/**********************************************
 *                QueryResults                *
 **********************************************/
void Add(QueryResults* results, const tsdb::label::Labels& lset, uint64_t id,
         const std::vector<int64_t>& t, const std::vector<double>& v) {
  QueryResult* result = results->add_results();
  if (!lset.empty()) {
    Tags* tags = result->mutable_metric();
    NewTags(lset, tags);
  }
  result->set_id(id);
  for (size_t i = 0; i < t.size(); i++) {
    QuerySample* sample = result->add_values();
    sample->set_t(t[i]);
    sample->set_v(v[i]);
  }
}

void Add(QueryResults* results, const tsdb::label::Labels& lset,
         const std::vector<tsdb::label::Labels>& lsets, uint64_t id,
         const std::vector<std::vector<int64_t>>& t,
         const std::vector<std::vector<double>>& v) {
  QueryResult* result = results->add_results();
  if (!lset.empty()) {
    Tags* tags = result->mutable_metric();
    NewTags(lset, tags);
  }
  result->set_id(id);
  for (size_t i = 0; i < t.size(); i++) {
    QueryGroupSample* sample = result->add_series();
    if (!lsets.empty()) {
      Tags* tags = sample->mutable_metric();
      NewTags(lsets[i], tags);
    }
    for (size_t j = 0; j < t[i].size(); j++) {
      QuerySample* sample2 = sample->add_values();
      sample2->set_t(t[i][j]);
      sample2->set_v(v[i][j]);
    }
  }
}

void Parse(QueryResults* results, int idx, tsdb::label::Labels* lset,
           uint64_t* id, std::vector<int64_t>* t, std::vector<double>* v) {
  Parse(results->mutable_results(idx), lset, id, t, v);
}

void Parse(QueryResults* results, int idx, tsdb::label::Labels* lset,
           std::vector<tsdb::label::Labels>* lsets, uint64_t* id,
           std::vector<std::vector<int64_t>>* t,
           std::vector<std::vector<double>>* v) {
  Parse(results->mutable_results(idx), lset, lsets, id, t, v);
}

void Parse(QueryResult* result, tsdb::label::Labels* lset, uint64_t* id,
           std::vector<int64_t>* t, std::vector<double>* v) {
  Tags* tags = result->mutable_metric();
  NewTags(lset, tags);
  *id = result->id();
  for (int i = 0; i < result->values_size(); i++) {
    t->push_back(result->values(i).t());
    v->push_back(result->values(i).v());
  }
}

void Parse(QueryResult* result, tsdb::label::Labels* lset,
           std::vector<tsdb::label::Labels>* lsets, uint64_t* id,
           std::vector<std::vector<int64_t>>* t,
           std::vector<std::vector<double>>* v) {
  Tags* tags = result->mutable_metric();
  NewTags(lset, tags);
  *id = result->id();
  for (int i = 0; i < result->series_size(); i++) {
    QueryGroupSample* sample = result->mutable_series(i);
    tags = sample->mutable_metric();
    if (tags->tags_size() > 0) {
      tsdb::label::Labels l;
      NewTags(&l, tags);
      lsets->push_back(std::move(l));
    }
    std::vector<int64_t> tmp_t;
    std::vector<double> tmp_v;
    for (int j = 0; j < sample->values_size(); j++) {
      tmp_t.push_back(sample->values(j).t());
      tmp_v.push_back(sample->values(j).v());
    }
    t->push_back(std::move(tmp_t));
    v->push_back(std::move(tmp_v));
  }
}

namespace tsdb {
namespace db {

void InsertEncoder::add(const label::Labels& lset, int64_t t, double v) {
  writer_.StartObject();
  writer_.Key("type");
  writer_.Int(kTS);
  writer_.Key("lset");
  writer_.StartObject();
  for (const label::Label& l : lset) {
    writer_.Key(l.label.c_str());
    writer_.String(l.value.c_str());
  }
  writer_.EndObject();
  writer_.Key("t");
  writer_.Int64(t);
  writer_.Key("v");
  writer_.StartArray();
  writer_.Double(v);
  writer_.EndArray();
  writer_.EndObject();
}

void InsertEncoder::add(uint64_t id, int64_t t, double v) {
  writer_.StartObject();
  writer_.Key("type");
  writer_.Int(kTS);
  writer_.Key("id");
  writer_.Uint64(id);
  writer_.Key("t");
  writer_.Int64(t);
  writer_.Key("v");
  writer_.StartArray();
  writer_.Double(v);
  writer_.EndArray();
  writer_.EndObject();
}

void InsertEncoder::add(const label::Labels& group_lset,
                        const std::vector<label::Labels>& individual_lsets,
                        int64_t timestamp, const std::vector<double>& values) {
  writer_.StartObject();
  writer_.Key("type");
  writer_.Int(kGroup);
  writer_.Key("lset");
  writer_.StartObject();
  for (const label::Label& l : group_lset) {
    writer_.Key(l.label.c_str());
    writer_.String(l.value.c_str());
  }
  writer_.EndObject();
  writer_.Key("lsets");
  writer_.StartArray();
  for (const auto& lset : individual_lsets) {
    writer_.StartObject();
    for (const label::Label& l : lset) {
      writer_.Key(l.label.c_str());
      writer_.String(l.value.c_str());
    }
    writer_.EndObject();
  }
  writer_.EndArray();
  writer_.Key("t");
  writer_.Int64(timestamp);
  writer_.Key("v");
  writer_.StartArray();
  for (double v : values) writer_.Double(v);
  writer_.EndArray();
  writer_.EndObject();
}

void InsertEncoder::add(uint64_t ref, int64_t timestamp,
                        const std::vector<double>& values) {
  writer_.StartObject();
  writer_.Key("type");
  writer_.Int(kGroup);
  writer_.Key("id");
  writer_.Uint64(ref);
  writer_.Key("t");
  writer_.Int64(timestamp);
  writer_.Key("v");
  writer_.StartArray();
  for (double v : values) writer_.Double(v);
  writer_.EndArray();
  writer_.EndObject();
}

InsertParser::InsertParser(const std::string& body) : idx_(0), size_(0) {
  d_.Parse(body.c_str());
  if (d_.HasParseError() || !d_.IsObject() || !d_.HasMember("samples") ||
      !d_["samples"].IsArray()) {
    err_.wrap("cannot parse current line");
    return;
  }
  size_ = d_["samples"].GetArray().Size();
}

bool InsertParser::next() {
  if (err_ || idx_ >= size_) return false;

  lset_.clear();
  lsets_.clear();
  slots_.clear();
  v_.clear();

  const auto& obj = d_["samples"].GetArray()[idx_];

  if (obj.HasMember("type")) {
    type_ = (InsertType)(obj["type"].GetInt());
  } else {
    err_.wrap("no type specified");
    return false;
  }

  if (obj.HasMember("lset")) {
    if (!obj["lset"].IsObject()) {
      err_.wrap("cannot parse lset");
      return false;
    }
    for (auto& item : obj["lset"].GetObject())
      lset_.emplace_back(item.name.GetString(), item.value.GetString());
  } else {
    if (obj.HasMember("id")) {
      id_ = obj["id"].GetUint64();
    } else {
      err_.wrap("no lset & no id");
      return false;
    }
  }

  if (obj.HasMember("lsets")) {
    if (!obj["lsets"].IsArray()) {
      err_.wrap("lsets is not array");
      return false;
    }
    const auto& arr = obj["lsets"].GetArray();
    for (size_t i = 0; i < arr.Size(); i++) {
      if (!arr[i].IsObject()) {
        err_.wrap("cannot parse lsets");
        return false;
      }
      label::Labels tmp_lset;
      for (auto& l : arr[i].GetObject())
        tmp_lset.emplace_back(l.name.GetString(), l.value.GetString());
      lsets_.push_back(std::move(tmp_lset));
    }
  }

  if (obj.HasMember("slots")) {
    if (!obj["slots"].IsArray()) {
      err_.wrap("cannot parse slots");
      return false;
    }
    const auto& arr = obj["slots"].GetArray();
    for (size_t i = 0; i < arr.Size(); i++) slots_.push_back(arr[i].GetInt());
  }

  if (obj.HasMember("t")) {
    t_ = obj["t"].GetInt64();
  } else {
    err_.wrap("no timestamp specified");
    return false;
  }

  if (obj.HasMember("v")) {
    if (!obj["v"].IsArray()) {
      err_.wrap("cannot parse values");
      return false;
    }
    const auto& arr = obj["v"].GetArray();
    for (size_t i = 0; i < arr.Size(); i++) v_.push_back(arr[i].GetDouble());
  } else {
    err_.wrap("no values specified");
    return false;
  }

  idx_++;
  return true;
}

InsertResultEncoder::InsertResultEncoder() : writer_(s_) {
  writer_.StartObject();
  writer_.Key("result");
  writer_.StartArray();
}

void InsertResultEncoder::add(uint64_t id, const std::vector<int>& slots) {
  writer_.StartObject();

  writer_.Key("id");
  writer_.Uint64(id);

  if (!slots.empty()) {
    writer_.Key("slots");
    writer_.StartArray();
    for (int slot : slots) writer_.Int(slot);
    writer_.EndArray();
  }

  writer_.EndObject();
}

void InsertResultEncoder::close() {
  writer_.EndArray();
  writer_.EndObject();
}

InsertResultParser::InsertResultParser(const std::string& body)
    : idx_(0), size_(0) {
  d_.Parse(body.c_str());
  if (d_.HasParseError() || !d_.IsObject()) {
    err_.wrap("cannot parse insert result");
    return;
  }

  if (!d_.HasMember("result")) {
    err_.wrap("no insert result");
    return;
  }
  size_ = d_["result"].GetArray().Size();
}

bool InsertResultParser::next() {
  if (err_ || idx_ >= size_) return false;

  const auto& obj = d_["result"].GetArray()[idx_];
  slots_.clear();
  if (obj.HasMember("id")) id_ = obj["id"].GetUint64();

  if (obj.HasMember("slots")) {
    for (const auto& slot : obj["slots"].GetArray())
      slots_.push_back(slot.GetUint64());
  }

  idx_++;
  return true;
}

QueryEncoder::QueryEncoder(int metric, const label::Labels& matchers,
                           int64_t mint, int64_t maxt)
    : writer_(s_) {
  writer_.StartObject();
  writer_.Key("metric");
  writer_.Int(metric);
  writer_.Key("matchers");
  writer_.StartObject();
  for (const label::Label& l : matchers) {
    writer_.Key(l.label.c_str());
    writer_.String(l.value.c_str());
  }
  writer_.EndObject();
  writer_.Key("mint");
  writer_.Int64(mint);
  writer_.Key("maxt");
  writer_.Int64(maxt);
  writer_.EndObject();
}

QueryParser::QueryParser(const std::string& body) {
  rapidjson::Document d;
  d.Parse(body.c_str());
  if (d.HasParseError() || !d.IsObject()) {
    err_.wrap("cannot parse current line");
    return;
  }

  if (d.HasMember("matchers")) {
    for (auto& item : d["matchers"].GetObject())
      lset_.emplace_back(item.name.GetString(), item.value.GetString());
  } else {
    err_.wrap("matchers not set");
    return;
  }

  if (d.HasMember("mint")) {
    mint_ = d["mint"].GetInt64();
  } else {
    err_.wrap("mint not set");
    return;
  }

  if (d.HasMember("maxt")) {
    maxt_ = d["maxt"].GetInt64();
  } else {
    err_.wrap("maxt not set");
    return;
  }

  if (d.HasMember("metric")) {
    metric_ = d["metric"].GetInt();
  } else
    metric_ = 1;
}

void QueryResultEncoder::write(const label::Labels& lset, uint64_t id,
                               const std::vector<int64_t>& t,
                               const std::vector<double>& v) {
  assert(t.size() == v.size());
  writer_.StartObject();

  writer_.Key("id");
  writer_.Uint64(id);

  writer_.Key("metric");
  writer_.StartObject();
  for (const label::Label& l : lset) {
    writer_.Key(l.label.c_str());
    writer_.String(l.value.c_str());
  }
  writer_.EndObject();

  writer_.Key("values");
  writer_.StartArray();
  for (size_t i = 0; i < t.size(); i++) {
    writer_.StartArray();
    writer_.Double((double)(t[i]) / 1000);
    writer_.Double(v[i]);
    writer_.EndArray();
  }
  writer_.EndArray();

  writer_.EndObject();
}

void QueryResultEncoder::write(const label::Labels& lset,
                               const std::vector<label::Labels>& lsets,
                               uint64_t id, const std::vector<int>& slots,
                               const std::vector<std::vector<int64_t>>& t,
                               const std::vector<std::vector<double>>& v) {
  assert(lsets.size() == v.size());
  assert(slots.empty() || slots.size() == lsets.size());
  writer_.StartObject();

  writer_.Key("id");
  writer_.Uint64(id);

  writer_.Key("metric");
  writer_.StartObject();
  for (const label::Label& l : lset) {
    writer_.Key(l.label.c_str());
    writer_.String(l.value.c_str());
  }
  writer_.EndObject();

  writer_.Key("series");
  writer_.StartArray();
  for (size_t i = 0; i < lsets.size(); i++) {
    writer_.StartObject();

    if (!slots.empty()) {
      writer_.Key("slot");
      writer_.Uint64(slots[i]);
    }

    writer_.Key("metric");
    writer_.StartObject();
    for (const label::Label& l : lsets[i]) {
      writer_.Key(l.label.c_str());
      writer_.String(l.value.c_str());
    }
    writer_.EndObject();
    writer_.Key("values");
    writer_.StartArray();
    for (size_t j = 0; j < t[i].size(); j++) {
      writer_.StartArray();
      writer_.Double((double)(t[i][j]) / 1000);
      writer_.Double(v[i][j]);
      writer_.EndArray();
    }
    writer_.EndArray();

    writer_.EndObject();
  }
  writer_.EndArray();

  writer_.EndObject();
}

QueryResultParser::QueryResultParser(const std::string& body)
    : idx_(0), size_(0) {
  d_.Parse(body.c_str());
  if (d_.HasParseError() || !d_.IsObject() || !d_.HasMember("result") ||
      !d_["result"].IsArray()) {
    err_.wrap("cannot parse result");
    return;
  }
  size_ = d_["result"].GetArray().Size();
}

bool QueryResultParser::next() {
  if (err_ || idx_ >= size_) return false;

  lset_.clear();
  lsets_.clear();
  timestamps_.clear();
  values_.clear();

  const auto& obj = d_["result"].GetArray()[idx_];
  if (!obj.IsObject()) {
    err_.wrap("cannot parse current sample");
    return false;
  }

  if (obj.HasMember("id")) id_ = obj["id"].GetUint64();

  if (obj.HasMember("metric")) {
    if (!obj["metric"].IsObject()) {
      err_.wrap("cannot parse metric");
      return false;
    }
    for (auto& item : obj["metric"].GetObject())
      lset_.emplace_back(item.name.GetString(), item.value.GetString());
  }

  if (obj.HasMember("values")) {
    std::vector<int64_t> timestamps;
    std::vector<double> values;
    for (const auto& value : obj["values"].GetArray()) {
      timestamps.push_back(value.GetArray()[0].GetDouble() * 1000);
      values.push_back(value.GetArray()[1].GetDouble());
    }
    timestamps_.push_back(std::move(timestamps));
    values_.push_back(std::move(values));
  }

  if (obj.HasMember("series")) {
    for (const auto& series : obj["series"].GetArray()) {
      if (series.HasMember("metric")) {
        label::Labels lset;
        for (auto& item : series["metric"].GetObject())
          lset.emplace_back(item.name.GetString(), item.value.GetString());
        lsets_.push_back(std::move(lset));
      }

      std::vector<int64_t> timestamps;
      std::vector<double> values;
      for (const auto& value : series["values"].GetArray()) {
        timestamps.push_back(value.GetArray()[0].GetDouble() * 1000);
        values.push_back(value.GetArray()[1].GetDouble());
      }
      timestamps_.push_back(std::move(timestamps));
      values_.push_back(std::move(values));
    }
  }

  idx_++;
  return true;
}

}  // namespace db
}  // namespace tsdb