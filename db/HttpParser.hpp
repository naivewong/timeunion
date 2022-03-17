#pragma once

#include <iostream>

#include "base/Error.hpp"
#include "db/DB.pb.h"
#include "label/Label.hpp"
#include "third_party/rapidjson/document.h"
#include "third_party/rapidjson/rapidjson.h"
#include "third_party/rapidjson/stringbuffer.h"
#include "third_party/rapidjson/writer.h"

void NewTags(const tsdb::label::Labels& lset, Tags* tags);
void NewTags(tsdb::label::Labels* lset, Tags* tags);

/**********************************************
 *                InsertSamples               *
 **********************************************/
void Add(InsertSamples* samples, const tsdb::label::Labels& lset, int64_t t,
         double v);
void Add(InsertSamples* samples, uint64_t id, int64_t t, double v);
void Add(InsertSamples* samples, const tsdb::label::Labels& group_lset,
         const std::vector<tsdb::label::Labels>& individual_lsets,
         int64_t timestamp, const std::vector<double>& values);
void Add(InsertSamples* samples, uint64_t ref, int64_t timestamp,
         const std::vector<double>& values);
void Add(InsertSamples* samples, uint64_t ref, const std::vector<int>& slots,
         int64_t timestamp, const std::vector<double>& values);

/**********************************************
 *                InsertResults                *
 **********************************************/
void Add(InsertResults* results, uint64_t id, const std::vector<int>& slots);

/**********************************************
 *                QueryRequest                *
 **********************************************/
void Add(QueryRequest* request, bool return_metric,
         const tsdb::label::Labels& matchers, int64_t mint, int64_t maxt);

/**********************************************
 *                QueryResults                *
 **********************************************/
void Add(QueryResults* results, const tsdb::label::Labels& lset, uint64_t id,
         const std::vector<int64_t>& t, const std::vector<double>& v);
void Add(QueryResults* results, const tsdb::label::Labels& lset,
         const std::vector<tsdb::label::Labels>& lsets, uint64_t id,
         const std::vector<std::vector<int64_t>>& t,
         const std::vector<std::vector<double>>& v);
void Parse(QueryResult* result, tsdb::label::Labels* lset, uint64_t* id,
           std::vector<int64_t>* t, std::vector<double>* v);
void Parse(QueryResult* result, tsdb::label::Labels* lset,
           std::vector<tsdb::label::Labels>* lsets, uint64_t* id,
           std::vector<std::vector<int64_t>>* t,
           std::vector<std::vector<double>>* v);
void Parse(QueryResults* results, int idx, tsdb::label::Labels* lset,
           uint64_t* id, std::vector<int64_t>* t, std::vector<double>* v);
void Parse(QueryResults* results, int idx, tsdb::label::Labels* lset,
           std::vector<tsdb::label::Labels>* lsets, uint64_t* id,
           std::vector<std::vector<int64_t>>* t,
           std::vector<std::vector<double>>* v);

namespace tsdb {
namespace db {

enum InsertType { kTS, kGroup };

/*
{
  "samples": [
    {
      "type":  1,
      "id":    100,
      "lset":  {"label1":"l1", "label2":"l2"},
      "lsets": [
        {"TS":"t1"},
        {"TS":"t2"}
      ],
      "t":     1625387863000,
      "v":     [0.1, 0.2]
    },
    {
      "type":  0,
      "id":    99,
      "lset":  {"label1":"l1", "label2":"l2"},
      "t":     1625387863000,
      "v":     [0.1]
    }
  ]
}
*/
class InsertEncoder {
 private:
  rapidjson::StringBuffer s_;
  rapidjson::Writer<rapidjson::StringBuffer> writer_;

 public:
  InsertEncoder() : writer_(s_) {
    writer_.StartObject();
    writer_.Key("samples");
    writer_.StartArray();
  }

  void add(const label::Labels& lset, int64_t t, double v);
  void add(uint64_t id, int64_t t, double v);
  void add(const label::Labels& group_lset,
           const std::vector<label::Labels>& individual_lsets,
           int64_t timestamp, const std::vector<double>& values);
  void add(uint64_t ref, int64_t timestamp, const std::vector<double>& values);

  void close() {
    writer_.EndArray();
    writer_.EndObject();
  }

  std::string str() { return s_.GetString(); }
};

class InsertParser {
 private:
  rapidjson::Document d_;
  InsertType type_;
  uint64_t id_;
  label::Labels lset_;
  std::vector<label::Labels> lsets_;
  std::vector<int> slots_;
  int64_t t_;
  std::vector<double> v_;

  int idx_;
  int size_;

  error::Error err_;

 public:
  explicit InsertParser(const std::string& body);

  error::Error error() { return err_; }

  InsertType type() { return type_; }
  uint64_t id() { return id_; }
  label::Labels& lset() { return lset_; }
  std::vector<label::Labels>& lsets() { return lsets_; }
  std::vector<int>& slots() { return slots_; }
  int64_t t() { return t_; }
  std::vector<double>& v() { return v_; }

  bool next();
};

class InsertResultEncoder {
 private:
  rapidjson::StringBuffer s_;
  rapidjson::Writer<rapidjson::StringBuffer> writer_;

 public:
  InsertResultEncoder();

  void add(uint64_t id, const std::vector<int>& slots);
  void close();
  std::string str() { return s_.GetString(); }
};

class InsertResultParser {
 private:
  rapidjson::Document d_;
  uint64_t id_;
  std::vector<int> slots_;
  error::Error err_;

  int idx_;
  int size_;

 public:
  explicit InsertResultParser(const std::string& body);

  bool next();
  uint64_t id() { return id_; }
  std::vector<int>& slots() { return slots_; }
  error::Error error() { return err_; }
};

/*
{
  "metric":   1,
  "matchers": {"label1":"l1", "label2":"l2"},
  "mint":     1625387863000,
  "maxt":     1625387963000
}
*/
class QueryEncoder {
 private:
  rapidjson::StringBuffer s_;
  rapidjson::Writer<rapidjson::StringBuffer> writer_;

 public:
  QueryEncoder(int metric, const label::Labels& matchers, int64_t mint,
               int64_t maxt);
  std::string str() { return s_.GetString(); }
};

class QueryParser {
 private:
  label::Labels lset_;
  int64_t mint_;
  int64_t maxt_;
  int metric_;

  error::Error err_;

 public:
  explicit QueryParser(const std::string& body);

  error::Error error() { return err_; }
  label::Labels matchers() { return lset_; }
  int64_t mint() { return mint_; }
  int64_t maxt() { return maxt_; }
  int metric() { return metric_; }
};

/*
{
  "result" : [
    {
      "metric" : {
        "__name__" : "up",
        "job" : "prometheus",
        "instance" : "localhost:9090"
      },
      "values" : [
        [ 1435781430.781, "1" ],
        [ 1435781445.781, "1" ],
        [ 1435781460.781, "1" ]
      ]
    },
    {
      "id" : 3,
      "values" : [
        [ 1435781430.781, "1" ],
        [ 1435781445.781, "1" ],
        [ 1435781460.781, "1" ]
      ]
    },
    {
      "metric" : {
        "__name__" : "up",
        "job" : "node",
      },
      "series" : [
        {
          "metric" : {
             "instance" : "localhost:9091"
          },
          "values" : [
             [ 1435781430.781, "1" ],
             [ 1435781445.781, "1" ],
             [ 1435781460.781, "1" ]
          ]
        },
        {
          "metric" : {
             "instance" : "localhost:9092"
          },
          "values" : [
             [ 1435781430.781, "1" ],
             [ 1435781445.781, "1" ],
             [ 1435781460.781, "1" ]
          ]
        }
      ]
    },
    {
      "id" : 13,
      "series" : [
        {
          "slot" : 0,
          "values" : [
             [ 1435781430.781, "1" ],
             [ 1435781445.781, "1" ],
             [ 1435781460.781, "1" ]
          ]
        },
        {
          "slot" : 1,
          "values" : [
             [ 1435781430.781, "1" ],
             [ 1435781445.781, "1" ],
             [ 1435781460.781, "1" ]
          ]
        }
      ]
    }
  ]
}
*/
class QueryResultEncoder {
 private:
  rapidjson::StringBuffer s_;
  rapidjson::Writer<rapidjson::StringBuffer> writer_;

 public:
  QueryResultEncoder() : writer_(s_) {
    writer_.StartObject();
    writer_.Key("result");
    writer_.StartArray();
  }

  void write(const label::Labels& lset, uint64_t id,
             const std::vector<int64_t>& t, const std::vector<double>& v);

  void write(const label::Labels& lset, const std::vector<label::Labels>& lsets,
             uint64_t id, const std::vector<int>& slots,
             const std::vector<std::vector<int64_t>>& t,
             const std::vector<std::vector<double>>& v);

  std::string str() { return s_.GetString(); }

  void close() {
    writer_.EndArray();
    writer_.EndObject();
  }
};

class QueryResultParser {
 private:
  rapidjson::Document d_;
  uint64_t id_;
  label::Labels lset_;
  std::vector<label::Labels> lsets_;
  std::vector<std::vector<int64_t>> timestamps_;
  std::vector<std::vector<double>> values_;

  int idx_;
  int size_;
  error::Error err_;

 public:
  explicit QueryResultParser(const std::string& body);

  uint64_t id() { return id_; }
  label::Labels& lset() { return lset_; }
  std::vector<label::Labels>& lsets() { return lsets_; }
  std::vector<std::vector<int64_t>>& timestamps() { return timestamps_; }
  std::vector<std::vector<double>>& values() { return values_; }

  bool next();
};

}  // namespace db
}  // namespace tsdb