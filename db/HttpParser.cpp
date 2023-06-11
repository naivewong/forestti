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
  Tags* tmp_lset = tmp_sample->mutable_lset();
  NewTags(lset, tmp_lset);
  tmp_sample->set_t(t);
  tmp_sample->set_v(v);
}

void Add(InsertSamples* samples, uint64_t id, int64_t t, double v) {
  InsertSample* tmp_sample = samples->add_samples();
  tmp_sample->set_id(id);
  tmp_sample->set_t(t);
  tmp_sample->set_v(v);
}

/**********************************************
 *                InsertResult                *
 **********************************************/
void Add(InsertResults* results, uint64_t id) {
  InsertResult* result = results->add_results();
  result->set_id(id);
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

void Parse(QueryResults* results, int idx, tsdb::label::Labels* lset,
           uint64_t* id, std::vector<int64_t>* t, std::vector<double>* v) {
  Parse(results->mutable_results(idx), lset, id, t, v);
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
