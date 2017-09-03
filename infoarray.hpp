#ifndef _INFOARRAY_HPP_
#define _INFOARRAY_HPP_

#include <stdint.h>

#define ARRAY_START_SIZE 32

struct transactionInfo{
    uint64_t    tid;
    uint32_t    jOffset;

    friend bool operator<(const transactionInfo &l, const transactionInfo &r){
        return l.tid < r.tid;
    }
    friend bool operator>(const transactionInfo &l, const transactionInfo &r){
        return l.tid > r.tid;
    }
    friend bool operator==(const transactionInfo &l, const transactionInfo &r){
        return l.tid == r.tid;
    }
};

struct infoArray{
    uint64_t        index;
    uint64_t        size;
    transactionInfo *array;

    infoArray():index(0), size(ARRAY_START_SIZE), array(NULL){};
};

void initInfoArray(infoArray*);

void doubleInfoArray(infoArray *info);

void reduceInfoArray(infoArray *info, uint64_t filled, uint64_t pos);

void infoArrayForget(infoArray *info, uint64_t tid, int64_t index);

bool findOffset(infoArray *info, uint64_t from, uint64_t to, uint32_t *offset);

bool findOffsetLeft(infoArray *info, uint64_t from, uint64_t to,
                    uint32_t *offset);

#endif
