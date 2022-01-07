//
// Created by jorge on 3/1/21.
//

#ifndef FTDCPARSER_PARSERTASK_H
#define FTDCPARSER_PARSERTASK_H


#include <cstdint>
#include <cstdio>

class ParserTask {

public:
    ParserTask(const uint8_t *data, size_t len,  uint64_t id) {
        this->data = data;
        this->length = len;
        this->id = id;
    }

    const uint8_t *getData() {
        return data;
    }

     size_t getDataSize() {
        return length;
    }

     int64_t getId() {
        return id;
    }

private:
    const uint8_t *data;
    size_t length;
    uint64_t id;
};


#endif //FTDCPARSER_PARSERTASK_H
