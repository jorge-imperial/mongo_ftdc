//
// Created by Jorge Imperial-Sosa on 1/16/21.
//

#ifndef FTDCPARSER_CSVWRITER_H
#define FTDCPARSER_CSVWRITER_H

#include <cstddef>
#include "Dataset.h"

class CSVWriter {

public:


    size_t dumpCSVTimestamps(Dataset *pDataset, std::string outputPath, Timestamp start, Timestamp end, bool rated);
};

#endif //FTDCPARSER_CSVWRITER_H

