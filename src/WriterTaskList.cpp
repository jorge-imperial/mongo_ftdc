//
// Created by jorge on 12/19/21.
//

#include "WriterTaskList.h"
#include "WriterTask.h"
#include "Dataset.h"

WriterTaskList::WriterTaskList(const  Timestamp start, const Timestamp end, const unsigned long metricCount) {

    this->start = start;
    this->end = end;

    // create tasks
    taskList.resize(metricCount);

    if (start == INVALID_TIMESTAMP || end == INVALID_TIMESTAMP) throw  (-1);
}


WriterTask
WriterTaskList::get()   {
    mu.lock();

    auto t = taskList.front();
    taskList.pop_front();

    mu.unlock();
    return t;
}

size_t
WriterTaskList::put(WriterTask &task)   {

    taskList.push_back(task);
    return taskList.size();
}
