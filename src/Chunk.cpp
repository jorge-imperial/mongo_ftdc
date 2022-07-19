//
// Created by jorge on 11/14/20.
//

#include "Chunk.h"

// Zlib decompressor
#include <zlib.h>
#include <boost/log/trivial.hpp>
#include <bson/bson.h>
#include <sstream>
#include <map>
#include <boost/token_functions.hpp>

#include "ChunkMetric.h"
#include "ConstDataRangeCursor.h"

static
void
write_log(const std::vector<std::string>  pref, const char *key, const bson_t *document) {
#if DEBUG_VISIT
    size_t documentLength = 0;
    char *str = bson_as_canonical_extended_json (document, &documentLength);
    BOOST_LOG_TRIVIAL(info) << "LOG-JSON " << key << " " << str;
    bson_free (str);
#endif
}

Chunk::Chunk (const uint8_t *data, size_t size, int64_t id=-1) : deltasInChunk(0), metricsInChunk(0) {
    compressed = new uint8_t[size];
    compressed_size = size;

    this->id = id/1000; //TODO: Why divide?

    // We know where this chunk starts, but not where it ends
    this->start = id;
    this->end = INVALID_TIMESTAMP;

    if (!memcpy(compressed, data, size)) {
        BOOST_LOG_TRIVIAL(fatal) << "Could not allocate " << size << " while parsing chunkVector.";
    }
}

size_t
Chunk::getMetricNames(std::vector<std::string> & metricNames) {

    for (auto &m : metrics) {
        metricNames.emplace_back(m->getName());
    }
    return metricNames.size();
}

std::string
fullname (const char *key, std::vector<std::string> & docs) {
    std::ostringstream s;
    for (auto & doc : docs)
        s << doc << ".";

    s << key;
    return s.str();
}


//
// From StackOverflow https://stackoverflow.com/questions/4901842/in-memory-decompression-with-zlib
//
int
Chunk::inflate(const void *src, int srcLen, void *dst, int dstLen) {
    z_stream strm = {nullptr};
    strm.total_in = strm.avail_in = srcLen;
    strm.total_out = strm.avail_out = dstLen;
    strm.next_in = (Bytef *) src;
    strm.next_out = (Bytef *) dst;

    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;

    int err;
    int ret = -1;

    err = inflateInit2(&strm, (15 + 32)); //15 window bits, and the +32 tells zlib to to detect if using gzip or zlib
    if (err == Z_OK) {
        err = ::inflate(&strm, Z_FINISH);
        if (err == Z_STREAM_END) {
            ret = strm.total_out;
        } else {
            inflateEnd(&strm);
            return err;
        }
    } else {
        inflateEnd(&strm);
        return err;
    }

    inflateEnd(&strm);
    return ret;
}

uint64_t
unpack(ConstDataRangeCursor *cdc) {
    uint64_t i = 0;
    uint8_t b;
    uint s = 0;

    while(true) {
        b   = cdc->ReadByte();
        i |= uint64_t(b & 0x7f) << s;
        s += 7;
        if ((b & 0x80) == 0) {
            return i;
        }
    }
}

int
Chunk::ReadVariableSizedInts() {
    int count = 0;

    const uint8_t *p = this->decompressed; // data;
    auto docSize = *(uint32_t*) this->decompressed;

    auto metricsSize = metrics.size();
    if (metricsSize != metricsInChunk) {
        BOOST_LOG_TRIVIAL(fatal) << "Chunks in document and in values do not match " << metrics.size() << " != " << metricsInChunk;
        //return -1;
    }

    auto offset = docSize + 8;
    const uint8_t *q = p +  offset;

    auto dataRangeSize = this->decompressed_size-offset;

    //if (dataRangeSize <= 0) return 0;

    ConstDataRangeCursor dataRangeCursor(q,dataRangeSize);

    uint64_t nZeroes = 0;

    for (int m=0; m < metrics.size(); ++m) {

        auto values = metrics[m]->getValues(); //
        auto value = values[0];

        for (int s = 0; s < deltasInChunk; ++s) {

            uint64_t delta;

            if (nZeroes!=0) {
                delta = 0;
                --nZeroes;
            } else {
                delta = unpack(&dataRangeCursor);
                if (delta == 0) {
                    nZeroes = unpack(&dataRangeCursor);
                }
            }
            value += delta;
            values[s + 1] = value;
        }
        metrics[m]->setSampleCount(deltasInChunk);
    }
    return count;
}

int
Chunk::Decompress() {
    decompressed = new uint8_t[CHUNK_MAX_SIZE];
    decompressed_size = inflate(compressed+4, compressed_size, decompressed, CHUNK_MAX_SIZE);
    return decompressed_size;
}

typedef struct {
    int visited;
    bson_visitor_t *visit_table;
    std::vector<std::string> name_stack;
    std::vector<ChunkMetric *> *metrics;
    bool logNames;
} visitResults ;

static bool
visit_before (const bson_iter_t *iter, const char *key, void *data)
{
    auto *v = (visitResults *) data;
    v->visited++;
    return false;// returning true stops further iteration of the document
}

static bool
visit_array(const bson_iter_t *, const char *key, const bson_t *v_array, void *data) {
    auto *v = (visitResults *) data;

    bson_iter_t child;
    if (bson_iter_init(&child, v_array)) {
        v->name_stack.emplace_back(key);
        ::bson_iter_visit_all(&child, v->visit_table, data);
        v->name_stack.pop_back();
    }
    return false;
}

static bool
visit_document(const bson_iter_t *, const char *key, const bson_t *v_document, void *data){
    auto *v = (visitResults *) data;
    bson_iter_t child;
    if (bson_iter_init(&child, v_document)) {

        ::write_log(v->name_stack, key, v_document);

        v->name_stack.emplace_back(key);
        ::bson_iter_visit_all(&child, v->visit_table, data);
        v->name_stack.pop_back();
    }
    return false;
}

static bool
visit_int32(const bson_iter_t *, const char *key, int32_t v_int32, void *data) {
    auto *v = (visitResults *) data;
    v->metrics->emplace_back( new ChunkMetric(fullname(key, v->name_stack),  BSON_TYPE_INT32, v_int32));

    if (v->logNames)
       BOOST_LOG_TRIVIAL(debug) << "Metric: " << fullname(key, v->name_stack) << " = " << v_int32;
    return false;
}

static bool
visit_int64(const bson_iter_t *, const char *key, int64_t v_int64, void *data) {
    auto *v = (visitResults *) data;
    v->metrics->emplace_back( new ChunkMetric(fullname(key, v->name_stack),  BSON_TYPE_INT64, v_int64));
    if (v->logNames)
       BOOST_LOG_TRIVIAL(debug) << "Metric: " << fullname(key, v->name_stack) << " = " << v_int64;
    return false;
}

static bool
visit_bool(const bson_iter_t *, const char *key, bool v_bool, void *data) {
    auto *v = (visitResults *) data;
    v->metrics->emplace_back( new ChunkMetric(fullname(key, v->name_stack),  BSON_TYPE_BOOL, v_bool));
    if (v->logNames)
        BOOST_LOG_TRIVIAL(debug) << "Metric: " << fullname(key, v->name_stack) << " = " << v_bool;
    return false;
}

static bool
visit_double(const bson_iter_t *, const char *key, double v_double, void *data){
    auto *v = (visitResults *) data;
    v->metrics->emplace_back( new ChunkMetric(fullname(key, v->name_stack),  BSON_TYPE_DOUBLE, v_double));
    if (v->logNames)
        BOOST_LOG_TRIVIAL(debug) << "Metric: " << fullname(key, v->name_stack) << " = " << v_double;
    return false;
}

static bool
visit_timestamp(const bson_iter_t *, const char *key, uint32_t t1, uint32_t t2, void *data){
    auto *v = (visitResults *) data;
    std::string s1 =  std::string(key) + "_t";
    v->metrics->emplace_back( new ChunkMetric(fullname( s1.c_str(), v->name_stack),  BSON_TYPE_INT32, t1));
    std::string s2 =  std::string(key) + "_i";
    v->metrics->emplace_back( new ChunkMetric(fullname( s2.c_str(), v->name_stack),  BSON_TYPE_INT32, t2));

    if (v->logNames) {
        BOOST_LOG_TRIVIAL(debug) << "Metric: " << fullname(key, v->name_stack) << " = " << t1;
        BOOST_LOG_TRIVIAL(debug) << "Metric: " << fullname(key, v->name_stack) << " = " << t2;
    }
    return false;
}

static bool
visit_date_time(const bson_iter_t *, const char *key, int64_t v_datetime, void *data){
    auto *v = (visitResults *) data;
    v->metrics->emplace_back( new ChunkMetric(fullname(key, v->name_stack),  BSON_TYPE_DATE_TIME, v_datetime));
    if (v->logNames)
        BOOST_LOG_TRIVIAL(debug) << "Metric: " << fullname(key, v->name_stack) << " = " << v_datetime;
    return false;
}

static bool
visit_utf8(const bson_iter_t *, const char *key, size_t, const char *s, void *data){
    auto *v = (visitResults *) data;
    if (v->logNames)
        BOOST_LOG_TRIVIAL(debug) << "Metric: " << fullname(key, v->name_stack) << " = " << s;
    return false;
}

//TODO:: Add?
static bool
visit_oid(const bson_iter_t *, const char *, const bson_oid_t *, void *data){
    return false;
}

static bool
visit_null(const bson_iter_t *, const char *, void *data){
    return false;
}

static bool
visit_binary(const bson_iter_t *, const char *, bson_subtype_t subtype, size_t , const uint8_t *, void *data){
    return false;
}

int
Chunk::ConstructMetrics(const uint8_t* data ) {

    bson_iter_t iter;
    auto *dataSize = (int32_t*) data;

    // Keys are variable between chunks and so are deltas.
    // Assume makes an ass of u and me.
    auto pp = data+(*dataSize);
    metricsInChunk = *(uint32_t*)pp;
    deltasInChunk = *(uint32_t*)(pp + 4);

    // This is the number of structures needed
    BOOST_LOG_TRIVIAL(debug) << "Metrics in chunk to reserve: " << metricsInChunk ;
    metrics.reserve(metricsInChunk);

    if ((bsonStruct = bson_new_from_data (data, (size_t)(*dataSize)))) {
        if (bson_iter_init (&iter, bsonStruct)) {

            bson_visitor_t vt{};
            visitResults v{};

            vt.visit_before = visit_before;
            vt.visit_array = visit_array;
            vt.visit_document = visit_document;
            vt.visit_int32 = visit_int32;
            vt.visit_int64 = visit_int64;
            vt.visit_bool = visit_bool;
            vt.visit_double = visit_double;
            vt.visit_date_time = visit_date_time;
            vt.visit_timestamp = visit_timestamp;
            // Only for display
            vt.visit_utf8 = visit_utf8;

            v.visit_table = &vt;
            v.metrics = &metrics;

            ::bson_iter_visit_all(&iter, &vt, &v);

            //BOOST_LOG_TRIVIAL(debug) << "Visited: " << v.visited << "  Metrics: " << v.metrics->size();
            if (metricsInChunk != metrics.size())
                BOOST_LOG_TRIVIAL(error) << "Wrong number of metrics: " << v.metrics->size() << "!=" << metricsInChunk;
        }
    }

    // Get actual values
    ReadVariableSizedInts();
    setTimestampLimits();

    return metrics.size();
}

void
Chunk::setTimestampLimits() {
    start = metrics[0]->getValue(0);
    end = metrics[0]->getValue( getSamplesCount()-1);
}

bool Chunk::Consume() {

    if (Decompress() > 0) {
        auto data = getUncompressedData();

        // We construct the metrics. These are name and first value only since deltasInChunk have not been read.
        ConstructMetrics(data);

        // Get actual values
        ReadVariableSizedInts();
        setTimestampLimits();
        return true;
    }
    else
        return false;
}



typedef struct {

    bson_t *parent;
    bson_t *doc;
    std::string name;
} SubDocs;

std::string
Chunk::getJsonAtPosition(size_t position) {

    bson_iter_t iter;
    bson_iter_t baz;

    auto newBson = bson_copy(bsonStruct);

    if (bson_iter_init (&iter, newBson)) {

        for (int i=0;i<metrics.size(); ++i) {
            if (bson_iter_find_descendant(&iter, metrics[i]->getName().c_str(), &baz)) {

                auto t = bson_iter_type(&baz);

                switch (t) {
                    case BSON_TYPE_INT64:
                        bson_iter_overwrite_int64(&baz, metrics[i]->getValue(position));
                        break;
                    case BSON_TYPE_INT32:
                        bson_iter_overwrite_int32(&baz, metrics[i]->getValue( position ));
                        break;
                    case BSON_TYPE_DATE_TIME:
                        bson_iter_overwrite_date_time(&baz, metrics[i]->getValue( position));
                        break;
                    case BSON_TYPE_DOUBLE:
                        bson_iter_overwrite_double(&baz, metrics[i]->getValue( position));
                        break;
                    case BSON_TYPE_BOOL:
                        bson_iter_overwrite_bool(&baz, metrics[i]->getValue( position));
                        break;
                    case BSON_TYPE_UTF8:
                    case  BSON_TYPE_EOD:
                    case BSON_TYPE_DOCUMENT:
                    case BSON_TYPE_ARRAY:
                    case BSON_TYPE_DECIMAL128:
                    case BSON_TYPE_MAXKEY:
                    case BSON_TYPE_MINKEY :
                    case BSON_TYPE_BINARY:
                    case BSON_TYPE_UNDEFINED:
                    case BSON_TYPE_OID:
                    case BSON_TYPE_NULL:
                    case BSON_TYPE_REGEX:
                    case BSON_TYPE_TIMESTAMP:
                    case BSON_TYPE_DBPOINTER:

                    default:
                        break;
                }
            }
        }
    }
    auto jsonStr = bson_as_json(newBson, NULL);
    std::string ret = std::string(jsonStr);
    bson_free(jsonStr);
    bson_destroy(newBson);

    return ret;

}


std::string
Chunk::getJsonFromTimestamp(Timestamp ts) {

    for (size_t i=0;i<metrics.size(); ++i) {
        if (metrics[0]->getValue(i) == ts) {
            return getJsonAtPosition(i);
        }
    }

    return "{}";
}

std::string
Chunk::getCsvAtPosition(size_t pos) {

    std::string ret;
    for (auto m : metrics) {
        ret += std::to_string(m->getValue(pos));
        ret += ",";
    }
    return ret;
}

size_t Chunk::getRawValuesAtPosition(uint64_t *values, size_t pos) {
    size_t count = 0;
    for (auto m : metrics)
        values[count++] = m->getValue(pos);

    return count;
}


