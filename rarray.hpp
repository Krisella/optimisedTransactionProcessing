#ifndef _RARRAY_HPP_
#define _RARRAY_HPP_

#include <stdint.h>
#include <cstring>
#include "list.hpp"
#include "bsearch.hpp"

#define SIZE_STEP 4
#define INSERT 1
#define DELETE 0

/* Struct for the entries of the RangeArray
   offset = positions in the Journal for the transaction id
   flags = 0 if the corresponding position in offset is not used, else 1 */
struct RAEntry{
    uint64_t    transactionId;
    uint32_t    offset[2];
    uint8_t     flags[2];
    /* Overload operators for use with binary search */
    friend bool operator<(const RAEntry &l, const RAEntry &r){
        return l.transactionId < r.transactionId;
    }
    friend bool operator>(const RAEntry &l, const RAEntry &r){
        return l.transactionId > r.transactionId;
    }
    friend bool operator==(const RAEntry &l, const RAEntry &r){
        return l.transactionId == r.transactionId;
    }
};

struct RAEntry_ptr{
    RAEntry     *array;
    uint32_t    limit;
    int64_t     pos;

    RAEntry_ptr():array(NULL), pos(-1){}
};

/* Class for RangeArray.
   entries = array of the entries
   entryNo = current # of entries
   size = current size of the array */
class RangeArray{
private:
    RAEntry     *entries;
    uint32_t    entryNo;
    uint32_t    size;
    /* Necessary operations that are not available to users. Only called from
       class functions. */
    int doubleArray();
public:
    /* Constructors, destructors and helper functions. */
    RangeArray();
    ~RangeArray();
    /* Operations that are relevant to the user */
    int insertEntry(uint64_t tid, uint32_t offset, int action);
    RAEntry_ptr getEntries(uint64_t range_start, uint64_t range_end);
    bool isInsert(){return entries[entryNo - 1].flags[INSERT] == 1;};
    uint32_t getLastInsert(){return entries[entryNo - 1].offset[INSERT];};
    bool is_empty(){return size==0;};
    void print_array();
};

#endif
