#ifndef _VENTRY_HPP_
#define _VENTRY_HPP_

#include <stdint.h>
#include "structs.hpp"
#include "Journal.hpp"
#include "infoarray.hpp"
#include "list.hpp"

#define BITSET_START_SIZE 128

enum{LEFT, RIGHT, START};

class ValidationEntry{
public:
    uint64_t                    range_start;
    uint64_t                    range_end;
    Column_t                    condition;
    unsigned char               *bitSet;
    uint32_t                    bitSetSize;
    uint32_t                    bitSetEntries;
    uint64_t                    journalOffset;
    List<ValidationQueries_t*>  vList;

    void doubleBitset();
    void reduceBitset(uint32_t filled);
    void shiftBitset(uint32_t shiftamount);

    ValidationEntry(uint64_t rs, uint64_t re, Column_t cond);
    ~ValidationEntry();

    void addValidation(ValidationQueries_t* v){vList.push_back(v);};

    void removeValidation(){ValidationQueries_t *v; vList.pop(&v);};

    int inRange(uint64_t rs, uint64_t re);

    void addRange(uint64_t limit, Journal *j, uint32_t offset, int side);

    uint64_t* getConflicts(uint32_t*, uint32_t*, uint64_t, uint64_t, uint32_t*,
                           uint32_t, uint32_t);

    void forget(uint64_t tid, infoArray *info, uint64_t index);
};

#endif
