#include <cstdio>
#include <cstring>
#include "rarray.hpp"

RangeArray::RangeArray(){
    entries = new RAEntry[SIZE_STEP];
    entryNo = 0;
    size = SIZE_STEP;

    for(int i = 0; i < SIZE_STEP; i++){
        entries[i].transactionId = 0;
        entries[i].flags[0] = 0;
        entries[i].flags[1] = 0;
        entries[i].offset[0] = 0;
        entries[i].offset[1] = 0;
    }
}

/* Typical destructor. */
RangeArray::~RangeArray(){
    if(entries != NULL){
        delete[] entries;
    }
}

/* Inserts a new entry to the RangeArray. Checks to see if there is already the
   same transaction id in the array and if it is, adds the offset to it's offset
   array. Else adds it to a new entry at the action position of the array. */
int RangeArray::insertEntry(uint64_t tid, uint32_t offset, int action){
    if(entryNo == size && entries[entryNo - 1].transactionId != tid){
        if(doubleArray()){
            return -1;
        }
    }

    if(entryNo > 0 && entries[entryNo - 1].transactionId == tid){
        entries[entryNo - 1].offset[INSERT] = offset;
        entries[entryNo - 1].flags[INSERT] = 1;
    }else{
        entries[entryNo].transactionId = tid;
        entries[entryNo].offset[action] = offset;
        entries[entryNo].flags[action] = 1;
        entries[entryNo].flags[(action + 1) % 2] = 0;
        entryNo++;

    }

    return 0;
}

/* Returns a List with the RAEntries in the requested range */
RAEntry_ptr RangeArray::getEntries(uint64_t range_start, uint64_t range_end){
    RAEntry_ptr     toReturn;
    RAEntry         temp;

    temp.transactionId = range_start;
    toReturn.array = entries;
    toReturn.limit = entryNo;
    toReturn.pos = binarySearch<RAEntry, int64_t>(entries, temp, 0, entryNo - 1);
    if(toReturn.pos < 0 || toReturn.pos >= entryNo ||
       entries[toReturn.pos].transactionId < range_start ||
       entries[toReturn.pos].transactionId > range_end){
        toReturn.pos = -1;
    }

    return toReturn;
}

/* Doubles the size of the array */
int RangeArray::doubleArray(){
    RAEntry     *newArray;

    newArray = new RAEntry[size * 2];
    memmove(newArray, entries, size * sizeof(RAEntry));
    size *= 2;
    delete[] entries;
    entries = newArray;

    return 0;
}

void RangeArray::print_array(){
	for(uint32_t i=0; i<entryNo; i++){
		printf("%lu, ",entries[i].transactionId);
	}
	printf("\n");
}
