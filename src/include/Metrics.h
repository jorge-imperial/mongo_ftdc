//
// Created by jorge on 12/28/21.
//

#ifndef PYFTDC_METRICS_H
#define PYFTDC_METRICS_H

#include <vector>
#include <cstdint>

namespace ftdcparser {
    typedef std::vector<uint64_t> Metrics;
    typedef std::vector<uint64_t> *MetricsPtr;
}

#endif //PYFTDC_METRICS_H
