//
// Created by jorge on 12/24/21.
//

#ifndef PYFTDC_JSONWRITER_H
#define PYFTDC_JSONWRITER_H


#include <cstddef>
#include "Dataset.h"

#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>


class JSONWriter {

public:


      size_t dumpTimestamps(Dataset *dataset,
                          std::string outputFile,
                          Timestamp start = INVALID_TIMESTAMP, Timestamp end=INVALID_TIMESTAMP,
                          bool rated=false );


};


#endif //PYFTDC_JSONWRITER_H
