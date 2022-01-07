//
// Created by jorge on 12/15/20.
//

#ifndef FTDCPARSER_BSON_VALUE_H
#define FTDCPARSER_BSON_VALUE_H

#include <bson.h>

BSON_ALIGNED_BEGIN (8)
typedef struct  {

    union {
        bson_oid_t v_oid;
        int64_t v_int64;
        int32_t v_int32;
        int8_t v_int8;
        double v_double;
        bool v_bool;
        int64_t v_datetime;
        struct {
            uint32_t timestamp;
            uint32_t increment;
        } v_timestamp;
        struct {
            char *str;
            uint32_t len;
        } v_utf8;
        struct {
            uint8_t *data;
            uint32_t data_len;
        } v_doc;
        struct {
            uint8_t *data;
            uint32_t data_len;
            bson_subtype_t subtype;
        } v_binary;
        struct {
            char *regex;
            char *options;
        } v_regex;
        struct {
            char *collection;
            uint32_t collection_len;
            bson_oid_t oid;
        } v_dbpointer;
        struct {
            char *code;
            uint32_t code_len;
        } v_code;
        struct {
            char *code;
            uint8_t *scope_data;
            uint32_t code_len;
            uint32_t scope_len;
        } v_codewscope;
        struct {
            char *symbol;
            uint32_t len;
        } v_symbol;
        bson_decimal128_t v_decimal128;
    } value;
    int32_t padding;
} bson_value BSON_ALIGNED_END (8);



#endif //FTDCPARSER_BSON_VALUE_H
