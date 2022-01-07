//
// Created by jorge on 11/11/21.
//

#ifndef PYFTDC_FILEPARSEDDATA_H
#define PYFTDC_FILEPARSEDDATA_H


#include <string>
#include <filesystem>

class FileParsedData {

public:
    FileParsedData(const char *file, uint64_t start, uint64_t end, size_t samplesCount) {
        this->filePath =  file;
        this->start = start;
        this->end = end;
        this->samplesInFile = samplesCount;
    }

    std::string getFile() { return filePath; }
    uint64_t getStart() { return  start; }
    uint64_t getEnd() { return  end; }
    size_t getSamplesCount() {return samplesInFile;}
    std::string getFileAbsolute() {
        auto absPath = std::filesystem::absolute( filePath );
        return absPath.string();
    }
private:
    std::string filePath;
    uint64_t start;
    uint64_t end;
    size_t   samplesInFile;
};


#endif //PYFTDC_FILEPARSEDDATA_H
