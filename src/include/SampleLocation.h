//
// Created by Jorge Imperial-Sosa on 11/8/21.
//

#ifndef PYFTDC_SAMPLELOCATION_H
#define PYFTDC_SAMPLELOCATION_H


#include <cstddef>

class SampleLocation  {

public:
    SampleLocation(int chunkPos, int samplePos) { this->samplePos = samplePos; this->chunkPos = chunkPos; }

    size_t getChunkLoc() {  return chunkPos;  }
    size_t getSampleLoc() { return samplePos; }

private:
    size_t chunkPos;
    size_t samplePos;
};


#endif //PYFTDC_SAMPLELOCATION_H
