#ifndef _HASH_HPP_
#define _HASH_HPP_

#include <stdint.h>
#include <cstdio>
#include <cstring>
#include "list.hpp"
#include "bsearch.hpp"
/* Class for the hash map.
   g_depth = global depth of the map
   size = size of the index (2^g_depth)
   recsPerBucket = maximum allowed entries per bucket
   buckets = the array of index entries
   bucketsPerDepth = an array that holds the number of buckets that have
                     the corresponding depth */
template<typename T>
class Hash{
private:
    /* Struct with the value of the key of the entry as well as a pointer to the
       entry's data */
    struct entry{
        uint64_t    key;
        T           *data;

        friend bool operator<(const entry &l, const entry &r){
            return l.key < r.key;
        }
        friend bool operator>(const entry &l, const entry &r){
            return l.key > r.key;
        }
        friend bool operator==(const entry &l, const entry &r){
            return l.key == r.key;
        }
        friend bool operator<=(const entry &l, const entry &r){
            return l.key < r.key || l.key == r.key;
        }

        entry():key(0), data(NULL){};
    };
    struct entryArray{
        uint32_t    index;
        entry       *records;
        uint64_t    forgot;

        entryArray():index(0), records(NULL), forgot(0){};
    };
    /* Struct of the bucket.
       l_depth = local depth of the bucket
       records = an array of entrys */
    struct bucket{
        uint32_t    l_depth;
        entryArray  *arrayptr;

        bucket():l_depth(0), arrayptr(NULL){};
    };
    uint32_t    g_depth;
    uint64_t    size;
    uint8_t     recsPerBucket;
    bucket      *buckets;
    uint64_t    bucketsPerDepth[64];
    /* Necessary operations that are not available to users. Only called from
       class functions. */
    uint64_t findPos(uint64_t key);
    uint64_t findPos(uint64_t key, uint32_t l_depth);
    void splitBucket(uint64_t startPos, uint32_t amount, uint32_t step);
    void mergeBuckets(uint64_t pos, uint64_t step, uint64_t amount);
    void doubleIndex();
    void halveIndex();
public:
    /* Constructors, destructors and helper functions. */
    void init(uint32_t, uint8_t);
    ~Hash();
    void printHash();
    /* Operations that are relevant to the user */
    void insertHashRecord(uint64_t key, T *newT);
    void deleteHashRecord(uint64_t pos, uint32_t index);
    void deleteValidationEntry(uint64_t key);
    T* getHashRecord(uint64_t key);
    template <typename T1>
    T1 getHashRecords(uint64_t key, uint64_t range_start, uint64_t range_end);
    void forget(uint64_t transactionId, infoArray* info, int64_t index);
};

/* Init is called after the construction of the hash map and it initializes the
   array of indexes as well as allocating an empty bucket to which all the indexes
   point to */
template<typename T>
void Hash<T>::init(uint32_t depth, uint8_t rpb){
    uint64_t    s = 2;
    entryArray  *newptr;

    g_depth = depth;
    recsPerBucket = rpb;

    switch(g_depth){
        case 0:
        buckets = new bucket[1];
        size = 1;
        break;
        case 1:
        buckets = new bucket[2];
        size = 2;
        break;
        default:
        s <<= g_depth - 1;
        size = s;
        buckets = new bucket[s];
    }

    newptr = new entryArray;
    newptr->records = new entry[rpb];

    for(int i = 0; i < size; i++){
        buckets[i].arrayptr = newptr;
    }

    memset(bucketsPerDepth, 0, 64 * 8);
}
/* The destructor checks for multiple pointers to the same bucket in order to
   avoid double free's */
template<typename T>
Hash<T>::~Hash(){
    entryArray  *cur;
    uint32_t    step, amount, index;

    if(buckets != NULL){
        for(uint64_t i = 0; i < size; i++){
            cur = buckets[i].arrayptr;
            if(cur != NULL){
                step = amount = 1;
                if(g_depth != buckets[i].l_depth){
                    step <<= buckets[i].l_depth;
                    amount <<= g_depth - buckets[i].l_depth;
                    for(uint64_t j = 0; j < amount; j++){
                        buckets[i + j * step].arrayptr = NULL;
                    }
                }
                if(cur->records != NULL){
                    index = cur->index;
                    for(uint32_t j = 0; j < index; j++){
                        delete cur->records[j].data;
                    }
                    delete[] cur->records;
                }
                delete cur;
            }
        }
        delete[] buckets;
    }
}

/* Prints hash in a somewhat printable format to help with debugging */
template <typename T>
void Hash<T>::printHash(){
    for(uint64_t i = 0; i < size; i++){
        fprintf(stderr, "i = %lu depth = %lu -->%p\t", i, buckets[i].l_depth,
                buckets[i].arrayptr);
        if(buckets[i].arrayptr->index > 0){
            fprintf(stderr, "[");
            for(uint32_t j = 0; j < buckets[i].arrayptr->index; j++){
                fprintf(stderr, " %lu", buckets[i].arrayptr->records[j].key);
            }
            fprintf(stderr, " ]\n");
        }else{
            fprintf(stderr, "NULL\n");
        }
    }
}

/* Recursive function that inserts a new hash record after potentionally splitting
   the target bucket or doubling the index. It requires a pointer to malloc'ed
   data to be given as an argument */
template<typename T>
void Hash<T>::insertHashRecord(uint64_t key, T* newT){
    uint64_t    pos = findPos(key), startPos;
    uint32_t    curLDepth, step = 1, amount = 2, index;
    int32_t     searchPos = 0;
    entry       temp;

    temp.key = key;
    index = buckets[pos].arrayptr->index;
    if(index < recsPerBucket){
        if(buckets[pos].arrayptr->records == NULL){
            buckets[pos].arrayptr->records = new entry[recsPerBucket];
        }else{
            searchPos =
                binarySearch<entry, int32_t>(buckets[pos].arrayptr->records,
                                             temp, 0, index - 1);
        }
        if(recsPerBucket > 1 && buckets[pos].arrayptr->index != 0 &&
           (searchPos =
            binarySearch<entry, int32_t>(buckets[pos].arrayptr->records,
                                         temp, 0, index - 1)) < index){
            if(searchPos < 0) searchPos = 0;
            memmove(&(buckets[pos].arrayptr->records[searchPos + 1]),
                    &(buckets[pos].arrayptr->records[searchPos]),
                    (index - searchPos) * sizeof(entry));
            buckets[pos].arrayptr->records[searchPos].key = key;
            buckets[pos].arrayptr->records[searchPos].data = newT;
        }else{
            buckets[pos].arrayptr->records[index].key = key;
            buckets[pos].arrayptr->records[index].data = newT;
        }
        buckets[pos].arrayptr->index++;
    }else{
        if(buckets[pos].l_depth == g_depth){
            doubleIndex();
        }
        curLDepth = buckets[pos].l_depth;
        startPos = findPos(key, curLDepth);
        amount <<= g_depth - (curLDepth + 1);
        step <<= curLDepth;
        splitBucket(startPos, amount, step);
        insertHashRecord(key, newT);
    }
}

/* Deletes a hash record together with it's data. After doing so it checks if
   a merging of buckets or halving of the index is needed */
template <typename T>
void Hash<T>::deleteHashRecord(uint64_t pos, uint32_t index){
    buckets[pos].arrayptr->index--;
    delete buckets[pos].arrayptr->records[index].data;
    if(buckets[pos].arrayptr->index != 0){
        if(buckets[pos].arrayptr->index > index)
            memmove(&(buckets[pos].arrayptr->records[index]),
                    &(buckets[pos].arrayptr->records[index + 1]),
                    (buckets[pos].arrayptr->index - index) * sizeof(entry));
    }
}

/* Returns a pointer to the data of the required hash record. If it is not found
   NULL is returned */
template <typename T>
T* Hash<T>::getHashRecord(uint64_t key){
    T           *toReturn = NULL;
    uint64_t    pos = findPos(key);
    int32_t     searchPos = 0;
    entry       temp;

    if(buckets[pos].arrayptr->records != NULL){
        if(recsPerBucket > 1){
            temp.key = key;
            searchPos =
                binarySearch<entry, int32_t>(buckets[pos].arrayptr->records,
                                             temp, 0,
                                             buckets[pos].arrayptr->index - 1);
        }
        if(searchPos >= 0 && searchPos < buckets[pos].arrayptr->index &&
           buckets[pos].arrayptr->records[searchPos].data != NULL &&
           buckets[pos].arrayptr->records[searchPos].key == key){
            toReturn = buckets[pos].arrayptr->records[searchPos].data;
        }
    }

    return toReturn;
}

/* Returns a List of data from the corresponding bucket or NULL if none is found
   in the required range. */
template <typename T>
template <typename T1>
T1 Hash<T>::getHashRecords(uint64_t key, uint64_t range_start,
                           uint64_t range_end){
    T1          empty;
    uint64_t    pos = findPos(key);

    if(buckets[pos].arrayptr->records == NULL ||
       buckets[pos].arrayptr->records[0].data == NULL ||
       buckets[pos].arrayptr->records[0].key != key){
        return empty;
    }else{
        return buckets[pos].arrayptr->records[0].data->getEntries(range_start,
                                                                  range_end);
    }
}

template <typename T>
void Hash<T>::forget(uint64_t transactionId, infoArray* info, int64_t index){
    uint64_t    i;
    int32_t     j;

    for(i = 0; i < size; i++){
        if(buckets[i].arrayptr->records != NULL &&
           buckets[i].arrayptr->forgot != transactionId){
            for(j = 0; j < buckets[i].arrayptr->index; j++){
                buckets[i].arrayptr->records[j].data->forget(transactionId,
                                                                info, index);
            }
        }
        buckets[i].arrayptr->forgot = transactionId;
    }
}

template <typename T>
void Hash<T>::deleteValidationEntry(uint64_t key){
    uint32_t    searchPos, ldepth, indexSum;
    uint64_t    pos = findPos(key), pos1, pos2, step, amount;
    entry       temp;

    temp.key = key;
    searchPos = binarySearch<entry, int32_t>(buckets[pos].arrayptr->records,
                                             temp, 0,
                                             buckets[pos].arrayptr->index - 1);
    deleteHashRecord(pos, searchPos);
    if(buckets[pos].l_depth == 0) return;
    ldepth = buckets[pos].l_depth - 1;
    step = 1 << ldepth;
    pos1 = findPos(key, ldepth);
    pos2 = pos1 + step;

    indexSum = buckets[pos1].arrayptr->index +
               buckets[pos2].arrayptr->index;
    while(step != 0 && buckets[pos1].l_depth == buckets[pos2].l_depth &&
          indexSum < recsPerBucket){
        amount = 1 << (g_depth - ldepth);
        mergeBuckets(pos1, step, amount);
        ldepth--;
        step >>= 1;
        if(step != 0){
            pos1 = findPos(key, ldepth);
            pos2 = pos1 + step;
            indexSum = buckets[pos1].arrayptr->index +
                       buckets[pos2].arrayptr->index;
        }
    }

    while(!bucketsPerDepth[g_depth - 1]){
        halveIndex();
    }
}

template <typename T>
void Hash<T>::doubleIndex(){
    uint64_t    oldSize = size;
    bucket      *newIndex;

    size *= 2;
    g_depth++;
    newIndex = new bucket[size];
    memcpy(newIndex, buckets, oldSize * sizeof(bucket));
    memmove(&(newIndex[oldSize]), buckets, oldSize * sizeof(bucket));
    delete[] buckets;
    buckets = newIndex;
}

template <typename T>
void Hash<T>::halveIndex(){
    uint64_t    newSize = size / 2;
    bucket      *newIndex;

    newIndex = new bucket[newSize];
    memmove(newIndex, buckets, newSize * sizeof(bucket));
    delete[] buckets;
    buckets = newIndex;
    size = newSize;
    g_depth--;
}


template <typename T>
void Hash<T>::splitBucket(uint64_t startPos, uint32_t amount, uint32_t step){
    uint32_t    i, index, depth;
    uint64_t    cur, oldKey[recsPerBucket];
    entryArray  *newptr, *oldptr;
    T           *oldData[recsPerBucket];

    index = buckets[startPos].arrayptr->index;
    depth = buckets[startPos].l_depth;
    for(i = 0; i < index; i++){
        oldData[i] = buckets[startPos].arrayptr->records[i].data;
        buckets[startPos].arrayptr->records[i].data = NULL;
        oldKey[i] = buckets[startPos].arrayptr->records[i].key;
    }
    oldptr = buckets[startPos].arrayptr;
    oldptr->index = 0;
    newptr = new entryArray;

    for(uint64_t i = 0; i < amount; i++){
        cur = startPos + i * step;

        buckets[cur].l_depth++;
        if(i % 2 == 0){
            buckets[cur].arrayptr = oldptr;
        }else{
            buckets[cur].arrayptr = newptr;
        }
    }

    if(depth && bucketsPerDepth[depth - 1]){
        bucketsPerDepth[depth - 1]--;
    }
    bucketsPerDepth[depth]++;

    for(i = 0; i < index; i++){
        insertHashRecord(oldKey[i], oldData[i]);
    }
}

template <typename T>
void Hash<T>::mergeBuckets(uint64_t pos, uint64_t step, uint64_t amount){
    uint32_t depth = buckets[pos].l_depth;

    if(buckets[pos].arrayptr->records == NULL &&
       buckets[pos + step].arrayptr->records != NULL){
        buckets[pos].arrayptr = buckets[pos + step].arrayptr;
        buckets[pos + step].arrayptr = buckets[pos].arrayptr;
    }
    if(buckets[pos + step].arrayptr->records != NULL){
        memmove(&(buckets[pos].arrayptr->records[buckets[pos].arrayptr->index]),
                buckets[pos + step].arrayptr->records,
                buckets[pos + step].arrayptr->index * sizeof(entry));
        quickSort<entry>(buckets[pos].arrayptr->records, 0,
                         buckets[pos].arrayptr->index +
                         buckets[pos + step].arrayptr->index - 1);
    }
    buckets[pos].arrayptr->index += buckets[pos + step].arrayptr->index;
    if(buckets[pos + step].arrayptr->records != NULL)
        delete[] buckets[pos + step].arrayptr->records;
    delete buckets[pos + step].arrayptr;

    for(uint64_t i = 0; i < amount; i++){
        buckets[pos + i * step].l_depth--;
        if(buckets[pos + i * step].arrayptr != buckets[pos].arrayptr){
            buckets[pos + i * step].arrayptr = buckets[pos].arrayptr;
        }
    }

    if(bucketsPerDepth[depth - 1]){
        bucketsPerDepth[depth - 1]--;
    }
    if(depth - 1){
        bucketsPerDepth[depth - 2]++;
    }
}

/* Finds position according to global depth */
template <typename T>
uint64_t Hash<T>::findPos(uint64_t key){
    uint64_t mask = 0xFFFFFFFFFFFFFFFFULL;

    if(g_depth == 0)
    	mask = 0;
    else
    	mask >>= (64 - g_depth);

    return key & mask;
}

/* Finds position according to the local depth given as an argument */
template <typename T>
uint64_t Hash<T>::findPos(uint64_t key, uint32_t l_depth){
    uint64_t mask = 0xFFFFFFFFFFFFFFFFULL;

    if(l_depth == 0)
    	mask = 0;
    else
    	mask >>= (64 - l_depth);

    return key & mask;
}

#endif
