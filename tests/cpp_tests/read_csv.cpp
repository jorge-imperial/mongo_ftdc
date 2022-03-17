//
// Created by jorge on 3/16/22.
#include <cstddef>
#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include "ChunkMetric.h"

size_t ReadMetricsFromCSV(std::string csvTestFile, std::vector<ChunkMetric *> * metrics) {

    metrics->clear();
    metrics->reserve(2000);

    //
    std::ifstream f;
    f.open(csvTestFile, std::ios::in);
    std::string line;
    while (getline(f, line)) { //read data from file object and put it into string.

        std::stringstream ss(line);
        std::string token;
        ChunkMetric *pM = nullptr;
        int field = 0;
        do {
            std::getline(ss, token, ';');
            if (!pM) {
                pM = new ChunkMetric(token, BSON_TYPE_INT64, 0);
            } else {
                auto val = pM->getValues();
                val[field++] = atol(token.c_str());
            }

        } while (field < 300);
        metrics->emplace_back(pM);
    }
    f.close();

    return metrics->size();
}
