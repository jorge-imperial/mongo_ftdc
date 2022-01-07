//
// Created by jorge on 1/7/21.
//

#ifndef FTDCPARSER_CONSTDATARANGECURSOR_H
#define FTDCPARSER_CONSTDATARANGECURSOR_H

#include <cstdint>
#include <cstdio>

class ConstDataRangeCursor {

public:
    ConstDataRangeCursor(const uint8_t* data, size_t length) {
        this->data = data;
        this->length = length;
        //
        this->at = this->start = this->data;
        this->end = this->start + length;
    };


    uint8_t ReadByte();

private:
    const uint8_t *start;
    const uint8_t *end;
    const uint8_t* at;
    //
    const uint8_t* data;
    size_t length;
};


#endif //FTDCPARSER_CONSTDATARANGECURSOR_H
