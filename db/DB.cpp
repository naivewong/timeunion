#include "db/DB.hpp"

#include <snappy.h>

#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <limits>

#include "base/Logging.hpp"
#include "base/TimeStamp.hpp"
#include "base/WaitGroup.hpp"
#include "db/DB.pb.h"
#include "db/HttpParser.hpp"
#include "db/db_querier.h"
#include "label/EqualMatcher.hpp"
#include "querier/SeriesSetInterface.hpp"
#include "tsdbutil/tsdbutils.hpp"

namespace tsdb {
namespace db {

DB::DB(const std::string& dir, leveldb::DB* db)
    : dir_(dir),
      db_(db),
      head_(new head::HeadType(0, dir, dir, db)),
      cached_querier_(nullptr) {
#if USE_PROTOBUF
  init_http_proto_server();
#else
  init_http_json_server();
#endif
}

void DB::init_http_proto_server() {
  server_.Post("/insert", [this](const httplib::Request& req,
                                 httplib::Response& res) {
    if (!this->cached_appender_) this->cached_appender_ = this->appender();
    std::string data;
    snappy::Uncompress(req.body.data(), req.body.size(), &data);
    InsertSamples samples;
    InsertResults results;
    samples.ParseFromString(data);
    for (int i = 0; i < samples.samples_size(); i++) {
      InsertSample* sample = samples.mutable_samples(i);
      if (sample->type() == InsertSample_InsertType_TS) {
        if (sample->mutable_lset()->tags_size() == 0) {
          leveldb::Status s = this->cached_appender_->add_fast(
              sample->id(), sample->t(), sample->v(0));
          if (!s.ok())
            Add(&results, 0, {});
          else
            Add(&results, sample->id(), {});
        } else {
          label::Labels lset;
          NewTags(&lset, sample->mutable_lset());
          std::pair<uint64_t, leveldb::Status> p =
              this->cached_appender_->add(lset, sample->t(), sample->v(0));
          Add(&results, p.first, {});
        }
      } else if (sample->type() == InsertSample_InsertType_GROUP) {
        std::vector<double> vals;
        for (int j = 0; j < sample->v_size(); j++) vals.push_back(sample->v(j));
        if (sample->mutable_lset()->tags_size() == 0) {
          if (sample->slots_size() == 0) {
            leveldb::Status s =
                this->cached_appender_->add(sample->id(), sample->t(), vals);
            if (!s.ok())
              Add(&results, 0, {});
            else
              Add(&results, sample->id(), {});
          } else {
            std::vector<int> slots;
            for (int j = 0; j < sample->slots_size(); j++)
              vals.push_back(sample->slots(j));
            this->cached_appender_->add(sample->id(), slots, sample->t(), vals);
          }
        } else {
          uint64_t id;
          std::vector<int> slots;
          label::Labels lset;
          NewTags(&lset, sample->mutable_lset());
          std::vector<label::Labels> lsets;
          for (int j = 0; j < sample->lsets_size(); j++) {
            label::Labels l;
            NewTags(&l, sample->mutable_lsets(j));
            lsets.push_back(std::move(l));
          }
          this->cached_appender_->add(lset, lsets, sample->t(), vals, &id,
                                      &slots);
          Add(&results, id, slots);
        }
      }
    }

    this->cached_appender_->commit();
    data.clear();
    results.SerializeToString(&data);
    res.set_content(data, "text/plain");
  });

  server_.Post(
      "/query", [this](const httplib::Request& req, httplib::Response& res) {
        QueryRequest request;
        request.ParseFromString(req.body);

        if (!this->cached_querier_)
          this->cached_querier_ = this->querier(request.mint(), request.maxt());
        if (this->cached_querier_->mint() != request.mint() ||
            this->cached_querier_->maxt() != request.maxt()) {
          delete this->cached_querier_;
          this->cached_querier_ = querier(request.mint(), request.maxt());
        }
        std::vector<::tsdb::label::MatcherInterface*> matchers;
        for (int i = 0; i < request.lset().tags_size(); i++)
          matchers.push_back(new ::tsdb::label::EqualMatcher(
              request.lset().tags(i).name(), request.lset().tags(i).value()));

        QueryResults results;
        uint64_t id;

        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
            this->cached_querier_->select(matchers);
        while (ss->next()) {
          std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();
          id = ss->current_tsid();

          if (series->type() == tsdb::querier::kTypeSeries) {
            QueryResult* result = results.add_results();
            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->iterator();
            while (it->next()) {
              QuerySample* sample = result->add_values();
              sample->set_t(it->at().first);
              sample->set_v(it->at().second);
            }

            if (request.return_metric()) {
              Tags* tags = result->mutable_metric();
              NewTags(series->labels(), tags);
            }
            result->set_id(id);
          } else if (series->type() == tsdb::querier::kTypeGroup) {
            QueryResult* result = results.add_results();
            result->set_id(id);
            while (series->next()) {
              QueryGroupSample* sample = result->add_series();
              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->iterator();
              while (it->next()) {
                QuerySample* sample2 = sample->add_values();
                sample2->set_t(it->at().first);
                sample2->set_v(it->at().second);
              }

              if (request.return_metric()) {
                label::Labels lset;
                series->labels(lset);
                Tags* tags = sample->mutable_metric();
                NewTags(lset, tags);
              }
            }
          }
        }

        for (size_t i = 0; i < matchers.size(); i++) delete matchers[i];

        std::string data, compressed_data;
        results.SerializeToString(&data);
        snappy::Compress(data.data(), data.size(), &compressed_data);
        res.set_content(compressed_data, "text/plain");
      });

  std::thread t([this]() { this->server_.listen("127.0.0.1", 9966); });
  t.detach();
}

void DB::init_http_json_server() {
  server_.Post("/insert", [this](const httplib::Request& req,
                                 httplib::Response& res) {
    if (!this->cached_appender_) this->cached_appender_ = this->appender();
    InsertParser parser(req.body);
    InsertResultEncoder encoder;
    while (parser.next()) {
      if (parser.type() == kTS) {
        if (parser.lset().empty())
          this->cached_appender_->add_fast(parser.id(), parser.t(),
                                           parser.v()[0]);
        else {
          std::pair<uint64_t, leveldb::Status> p = this->cached_appender_->add(
              parser.lset(), parser.t(), parser.v()[0]);
          encoder.add(p.first, {});
        }
      } else if (parser.type() == kGroup) {
        if (parser.lset().empty()) {
          if (parser.slots().empty())
            this->cached_appender_->add(parser.id(), parser.t(), parser.v());
          else
            this->cached_appender_->add(parser.id(), parser.slots(), parser.t(),
                                        parser.v());
        } else {
          uint64_t id;
          std::vector<int> slots;
          this->cached_appender_->add(parser.lset(), parser.lsets(), parser.t(),
                                      parser.v(), &id, &slots);
          encoder.add(id, slots);
        }
      }
    }
    this->cached_appender_->commit();
    encoder.close();
    res.set_content(encoder.str(), "text/plain");
  });

  server_.Post(
      "/query", [this](const httplib::Request& req, httplib::Response& res) {
        QueryParser parser(req.body);
        if (parser.error()) {
          res.set_content(parser.error().error(), "text/plain");
          return;
        }
        if (!this->cached_querier_)
          this->cached_querier_ = this->querier(parser.mint(), parser.maxt());
        if (this->cached_querier_->mint() != parser.mint() &&
            this->cached_querier_->maxt() != parser.maxt()) {
          delete this->cached_querier_;
          this->cached_querier_ = querier(parser.mint(), parser.maxt());
        }
        std::vector<::tsdb::label::MatcherInterface*> matchers;
        for (const auto& l : parser.matchers())
          matchers.push_back(new ::tsdb::label::EqualMatcher(l.label, l.value));

        QueryResultEncoder encoder;
        uint64_t id;

        std::unique_ptr<::tsdb::querier::SeriesSetInterface> ss =
            this->cached_querier_->select(matchers);
        while (ss->next()) {
          std::unique_ptr<::tsdb::querier::SeriesInterface> series = ss->at();
          id = ss->current_tsid();

          if (series->type() == tsdb::querier::kTypeSeries) {
            std::vector<int64_t> timestamps;
            std::vector<double> values;
            std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                series->iterator();
            while (it->next()) {
              timestamps.push_back(it->at().first);
              values.push_back(it->at().second);
            }

            if (parser.metric() == 1)
              encoder.write(series->labels(), id, timestamps, values);
            else
              encoder.write({}, id, timestamps, values);
          } else if (series->type() == tsdb::querier::kTypeGroup) {
            std::vector<std::vector<int64_t>> timestamps;
            std::vector<std::vector<double>> values;
            std::vector<label::Labels> lsets;
            while (series->next()) {
              std::vector<int64_t> tmp_times;
              std::vector<double> tmp_values;
              std::unique_ptr<::tsdb::querier::SeriesIteratorInterface> it =
                  series->iterator();
              while (it->next()) {
                tmp_times.push_back(it->at().first);
                tmp_values.push_back(it->at().second);
              }

              if (parser.metric() == 1) {
                label::Labels lset;
                series->labels(lset);
                lsets.push_back(std::move(lset));
              }
              timestamps.push_back(std::move(tmp_times));
              values.push_back(std::move(tmp_values));
            }
            if (parser.metric() == 1)
              encoder.write({}, lsets, id, {}, timestamps, values);
            else
              encoder.write({}, {}, id, {}, timestamps, values);
          }
        }

        for (size_t i = 0; i < matchers.size(); i++) delete matchers[i];

        encoder.close();
        res.set_content(encoder.str(), "text/plain");
      });

  std::thread t([this]() { this->server_.listen("127.0.0.1", 9966); });
  t.detach();
}

DB::~DB() {
  if (cached_querier_) delete cached_querier_;
  delete db_;
  server_.stop();
}

void DB::purge_time(int64_t timestamp) {
  db_->PurgeTime(timestamp);
  head_->purge_time(timestamp);
}

}  // namespace db
}  // namespace tsdb