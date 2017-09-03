#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "infoarray.hpp"
#include "bsearch.hpp"

void initInfoArray(infoArray* arr){

	arr->array = new transactionInfo[ARRAY_START_SIZE];
}


void doubleInfoArray(infoArray *info){
    transactionInfo *newArray;
    uint64_t        newSize = info->size * 2;

    newArray = new transactionInfo[newSize];
    memmove(newArray, info->array, info->index * sizeof(transactionInfo));
    delete[] info->array;
    info->array = newArray;
    info->size = newSize;
}

void reduceInfoArray(infoArray *info, uint64_t filled, uint64_t pos){
    transactionInfo *newArray;
    uint64_t        newSize;

    if(info->size > ARRAY_START_SIZE){
        if(info->index){
            filled = info->index;
            filled--;
            filled |= filled >> 1;
            filled |= filled >> 2;
            filled |= filled >> 4;
            filled |= filled >> 8;
            filled |= filled >> 16;
            filled++;
            newSize = filled;
            if(newSize < ARRAY_START_SIZE)
                newSize = ARRAY_START_SIZE;
        }else{
            newSize = ARRAY_START_SIZE;
        }

        newArray = new transactionInfo[newSize];
        if(info->index){
            memmove(newArray, &(info->array[pos]), (info->index - pos) *
                    sizeof(transactionInfo));
            info->index -= pos;
        }
        delete[] info->array;
        info->array = newArray;
        info->size = newSize;
    }
}

void infoArrayForget(infoArray *info, uint64_t tid, int64_t index){
    if(info->index > 0 && info->array[0].tid <= tid){
        if(info->index - 1 < (uint64_t) index){
            info->index = 0;
            reduceInfoArray(info, 0, 0);
            return;
        }

        if(info->index - index < info->index / 2){
            reduceInfoArray(info, info->index, index);
        }else{
            memmove(&(info->array[0]), &(info->array[index]), (info->index -
                    index) * sizeof(transactionInfo));
            info->index -= index;
        }
    }
}

bool findOffset(infoArray *info, uint64_t from, uint64_t to, uint32_t *offset){
    int64_t         index;
    transactionInfo temp;

    temp.tid = to;
    index = binarySearch<transactionInfo, int64_t>(info->array, temp, 0,
                                                   info->index - 1);
    if(index < 0)
        return false;
    if((uint64_t) index > info->index - 1)
        index = info->index - 1;

    while(index != 0 && info->array[index].tid > to)
        index--;

    *offset = info->array[index + 1].jOffset;
    return true;
}

bool findOffsetLeft(infoArray *info, uint64_t from, uint64_t to,
                    uint32_t *offset){
    int64_t         index;
    transactionInfo temp;

    temp.tid = from;
    index = binarySearch<transactionInfo, int64_t>(info->array, temp, 0,
                                                   info->index - 1);
    if(index < 0)
        index = 0;
    if((uint64_t) index > info->index - 1 || info->array[index].tid > to)
        return false;

    *offset = info->array[index].jOffset;
    return true;
}
