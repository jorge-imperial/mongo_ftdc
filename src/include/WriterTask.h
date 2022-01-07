//
// Created by jorge on 12/24/21.
//

#ifndef PYFTDC_WRITERTASK_H
#define PYFTDC_WRITERTASK_H

#include "Dataset.h"
#include "Timestamp.h"


class WriterTask {

public:
    void setTimestamp(ftdcparser::Timestamp ts) {  timestamp = ts;   }
    ftdcparser::Timestamp getTimestamp() const { return timestamp; }

private:
    ftdcparser::Timestamp timestamp;
};


#endif //PYFTDC_WRITERTASK_H
