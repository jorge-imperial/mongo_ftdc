//
// Created by jorge on 12/28/21.
//

#ifndef PYFTDC_TIMESTAMP_H
#define PYFTDC_TIMESTAMP_H

namespace ftdcparser {

#include <cstdint>

    typedef uint64_t Timestamp;

    static const uint64_t INVALID_TIMESTAMP = UINT64_MAX;
}

#endif //PYFTDC_TIMESTAMP_H
