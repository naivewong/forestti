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
#include "label/EqualMatcher.hpp"
#include "querier/SeriesSetInterface.hpp"
#include "tsdbutil/tsdbutils.hpp"

namespace tsdb {
namespace db {

DB::DB(const std::string& dir, leveldb::DB* db)
    : dir_(dir),
      db_(db),
      head_(new head::Head(dir, "", db, true)),
      cached_querier_(nullptr) {
  init_http_proto_server();
}

void DB::init_http_proto_server() {
  server_.Post(
      "/insert", [this](const httplib::Request& req, httplib::Response& res) {
        if (!this->cached_appender_) this->cached_appender_ = this->appender();
        std::string data;
        snappy::Uncompress(req.body.data(), req.body.size(), &data);
        InsertSamples samples;
        InsertResults results;
        samples.ParseFromString(data);
        for (int i = 0; i < samples.samples_size(); i++) {
          InsertSample* sample = samples.mutable_samples(i);
          if (sample->mutable_lset()->tags_size() == 0) {
            leveldb::Status s = this->cached_appender_->add_fast(
                sample->id(), sample->t(), sample->v());
            if (!s.ok())
              Add(&results, 0);
            else
              Add(&results, sample->id());
          } else {
            label::Labels lset;
            NewTags(&lset, sample->mutable_lset());
            std::pair<uint64_t, leveldb::Status> p =
                this->cached_appender_->add(lset, sample->t(), sample->v());
            Add(&results, p.first);
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

DB::~DB() {
  if (cached_querier_) delete cached_querier_;
  delete db_;
  server_.stop();
}

}  // namespace db
}  // namespace tsdb