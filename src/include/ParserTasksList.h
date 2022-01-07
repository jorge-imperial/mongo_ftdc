//
// Created by Jorge on 2/16/21.
//

#ifndef FTDCPARSER_PARSERTASKSLIST_H
#define FTDCPARSER_PARSERTASKSLIST_H

#include <bson/bson.h>
#include <queue>
#include <Dataset.h>
#include <ParserTask.h>

class ParserTasksList {

public:
    ParserTasksList() {};

    void push(const uint8_t *data, size_t i, int64_t id);
    ParserTask *pop();
    bool empty();
private:
    boost::mutex mu;
    std::queue< ParserTask *> parserTasks;

};


#endif //FTDCPARSER_PARSERTASKSLIST_H
