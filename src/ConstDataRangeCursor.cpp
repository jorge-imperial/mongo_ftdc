//
// Created by jorge on 1/7/21.
//


#include "include/ConstDataRangeCursor.h"


uint8_t ConstDataRangeCursor::ReadByte() {
    uint8_t val = *at;
    ++at;
    return val;
}
