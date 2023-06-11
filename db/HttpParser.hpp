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

/**********************************************
 *                InsertResults                *
 **********************************************/
void Add(InsertResults* results, uint64_t id);

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
void Parse(QueryResult* result, tsdb::label::Labels* lset, uint64_t* id,
           std::vector<int64_t>* t, std::vector<double>* v);
void Parse(QueryResults* results, int idx, tsdb::label::Labels* lset,
           uint64_t* id, std::vector<int64_t>* t, std::vector<double>* v);
