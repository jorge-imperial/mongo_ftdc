//
// Created by Jorge on 2/16/21.
//

#include <ParserTasksList.h>
#include <ParserTask.h>

void
ParserTasksList::push(const uint8_t *data, size_t size, int64_t id) {
    mu.lock();

    ParserTask *t = new ParserTask(data, size, id);

    parserTasks.push(t);

    mu.unlock();
}

ParserTask *
ParserTasksList::pop( ) {
    mu.lock();
    auto p = parserTasks.front();
    parserTasks.pop();
    mu.unlock();
    return p;
}

bool
ParserTasksList::empty() {
    mu.lock();
    auto e = parserTasks.empty();
    mu.unlock();
    return e;
}