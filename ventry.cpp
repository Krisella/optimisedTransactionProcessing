#include "ventry.hpp"

ValidationEntry::ValidationEntry(uint64_t rs, uint64_t re, Column_t cond)
                    :range_start(rs), range_end(rs), condition(cond),
                     bitSet(NULL), bitSetSize(BITSET_START_SIZE),
                     bitSetEntries(0){
}

ValidationEntry::~ValidationEntry(){
    if(bitSet != NULL) delete[] bitSet;
    vList.deleteList();
}

void ValidationEntry::shiftBitset(uint32_t shiftamount){
    uint32_t indexshift = shiftamount / 64,
             bitshift = shiftamount % 64,
             startIndex;
    uint64_t mask, *castedBitset;

    startIndex = bitSetEntries / 64;

    while((startIndex + indexshift + 1) * 8 >= bitSetSize){
        doubleBitset();
    }
    castedBitset = (uint64_t*) bitSet;

    for(int i = startIndex; i >= 0; i--){
        mask = 0xFFFFFFFFFFFFFFFFULL;
        if(indexshift){
            castedBitset[i + indexshift] = castedBitset[i];
            castedBitset[i] = 0;
        }
        if(bitshift){
            mask <<= (64 - bitshift);
            mask &= castedBitset[i + indexshift];
            mask >>= (64 - bitshift);
            castedBitset[i + indexshift + 1] |= mask;
            castedBitset[i + indexshift] <<= bitshift;
        }
    }
}

void ValidationEntry::doubleBitset(){
    unsigned char   *newBitset;
    uint32_t        newBitsetSize = 2 * bitSetSize;

    newBitset = new unsigned char[newBitsetSize];
    memset(newBitset, 0, newBitsetSize);
    memmove(newBitset, bitSet, bitSetSize);
    delete[] bitSet;
    bitSet = newBitset;
    bitSetSize = newBitsetSize;
}

void ValidationEntry::reduceBitset(uint32_t filled){
    unsigned char   *newBitset;
    uint32_t        newBitsetSize;

    if(bitSetSize > BITSET_START_SIZE){
        if(filled){
            filled--;
            filled |= filled >> 1;
            filled |= filled >> 2;
            filled |= filled >> 4;
            filled |= filled >> 8;
            filled |= filled >> 16;
            filled++;
            newBitsetSize = filled;
            if(newBitsetSize < BITSET_START_SIZE)
                newBitsetSize = BITSET_START_SIZE;
        }else{
            newBitsetSize = BITSET_START_SIZE;
        }
        newBitset = new unsigned char[newBitsetSize];
        memset(newBitset, 0, newBitsetSize);
        memmove(newBitset, bitSet, newBitsetSize);
        delete[] bitSet;
        bitSet = newBitset;
        bitSetSize = newBitsetSize;
    }
}

int ValidationEntry::inRange(uint64_t rs, uint64_t re){
    int returnValue;

    returnValue = 0;
    if(bitSet == NULL){
        bitSet = new unsigned char[BITSET_START_SIZE];
        bitSetSize = BITSET_START_SIZE;
        memset(bitSet, 0, bitSetSize);
        if(rs != range_start){
            range_start = rs;
            range_end = rs;
        }
        returnValue = 6;
    }else if(rs > range_end){
        returnValue = 1;
    }else if(re < range_start){
        returnValue = 2;
    }else if(rs < range_start && re <= range_end){
        returnValue = 3;
    }else if(rs >= range_start && re > range_end){
        returnValue = 4;
    }else if(rs < range_start && re > range_end){
        returnValue = 5;
    }

    return returnValue;
}

void ValidationEntry::addRange(uint64_t limit, Journal *j, uint32_t offset,
                               int side){
    uint64_t            toInsertValues, bitSetIndex, temp = 0;
    uint64_t            *castedBitset = (uint64_t*) bitSet;
    JournalArrayEntry   *journal, entry;
    int                 conflict, bit;
    bool                flag = false;

    journal = j->get_journal_array_entry();

    if(side == LEFT){
        uint32_t        shiftamount = journalOffset - offset,
                        startOffset = offset;

        if(journalOffset == 0){
            range_start = limit;
            return;
        }

        if(bitSetEntries != 0){
            shiftBitset(shiftamount);
            offset = journalOffset - 1;
        }
        castedBitset = (uint64_t*) bitSet;
        journalOffset = startOffset;
        entry = journal[offset];
        if(entry.tid < limit){
            range_start = limit;
            return;
        }

        bitSetIndex = (shiftamount - 1) / 64;
        bit = (shiftamount - 1) % 64;
        toInsertValues = 1;

        while(!flag){
            conflict = calculateOp_t(condition.op, condition.value,
                                     entry.rec_array[condition.column]);
            if(conflict && entry.tid >= limit){
                toInsertValues <<= bit;
                temp |= toInsertValues;
                toInsertValues = 1;
            }
            bit--;
            offset--;
            if(offset) entry = journal[offset];

            if(bit < 0 || !offset || entry.tid < limit){
                castedBitset[bitSetIndex] |= temp;
                temp = 0;
                if(!offset || entry.tid < limit){
                    flag = true;
                }else{
                    bitSetIndex--;
                    bit = 63;
                }
            }
        }
        bitSetEntries += shiftamount;
        range_start = limit;
    }else{
        uint32_t journalLastIndex = j->get_last_index();
        uint64_t bitSetSize64 = bitSetSize / 8;
        if(side == RIGHT && bitSetEntries != 0){
            offset = bitSetEntries + journalOffset;
        }else{
            journalOffset = offset;
        }
        if(offset > journalLastIndex){
            range_end = limit;
            return;
        }

        entry = journal[offset];
        if(entry.tid > limit){
            range_end = limit;
            return;
        }

        if(side == RIGHT && bitSetEntries != 0){
            bitSetIndex = bitSetEntries / 64;
            bit = bitSetEntries % 64;
            if(bitSetIndex >= bitSetSize64){
                doubleBitset();
                bitSetSize64 <<= 1;
                castedBitset = (uint64_t*) bitSet;
            }
        }else{
            bitSetIndex = 0;
            bit = 0;
        }
        toInsertValues = 1;

        while(!flag && entry.tid <= limit){
            conflict = calculateOp_t(condition.op, condition.value,
                                     entry.rec_array[condition.column]);
            if(conflict){
                toInsertValues <<= bit;
                temp |= toInsertValues;
                toInsertValues = 1;
            }
            bit++;
            offset++;
            if(offset <= journalLastIndex) entry = journal[offset];

            if(bit > 63 || offset > journalLastIndex || entry.tid > limit){
                castedBitset[bitSetIndex] |= temp;
                temp = 0;
                if(offset > journalLastIndex || entry.tid > limit){
                    flag = true;
                }else{
                    bit = 0;
                    bitSetIndex++;
                    if(bitSetIndex >= bitSetSize64){
                        doubleBitset();
                        bitSetSize64 <<= 1;
                        castedBitset = (uint64_t*) bitSet;
                    }
                }
            }
        }
        bitSetEntries = offset - journalOffset;
        range_end = limit;
    }
}

uint64_t* ValidationEntry::getConflicts(uint32_t *num_of_bits, uint32_t *offset,
                                        uint64_t rs, uint64_t re, uint32_t* size,
                                        uint32_t offset1, uint32_t offset2){
    *offset = offset1 - journalOffset;

	*num_of_bits = offset2 - offset1 + 1;

	*size = bitSetSize;

	return (uint64_t*) bitSet;
}

void ValidationEntry::forget(uint64_t tid, infoArray *info, uint64_t index){
	uint32_t    num_of_bits, offset, to_be_removed;
    uint32_t    bitset_index, filled;
    uint64_t    *castedBitset;
    uint32_t    joffset;

    if(info->array[0].tid <= tid && bitSetEntries != 0){

        if(index >= info->index || (info->array[index].jOffset > journalOffset &&
           info->array[index].jOffset - journalOffset >= bitSetEntries - 1)){
            delete[] bitSet;
            bitSet = NULL;
            range_start = tid;
            range_end = tid;
            bitSetEntries = 0;
            return;
        }

        joffset = info->array[index].jOffset;
        if(range_start <= tid){
			num_of_bits = bitSetEntries - (joffset - journalOffset);
			to_be_removed = joffset - journalOffset;
			offset = (joffset - journalOffset) % 64;

			castedBitset = (uint64_t*) bitSet;
			bitset_index = (joffset - journalOffset)/64;

			uint32_t cur_sum = 0;
			int k = 0;
			uint64_t cur_bitfield, temp_bitfield;

			do{
				cur_bitfield = castedBitset[bitset_index + k];
				if(offset!=0){
					cur_bitfield >>= offset;
					if(cur_sum + 64 - offset != num_of_bits &&
					   bitset_index + k + 1 < (bitSetSize / 8)){
						temp_bitfield = castedBitset[bitset_index + k + 1];
						temp_bitfield <<= (64 - offset);
						cur_bitfield |= temp_bitfield;
					}
				}

				if(cur_sum+64 > num_of_bits){
					uint64_t end_mask = 0xFFFFFFFFFFFFFFFFULL;
					int num_of_shifts =64 - (num_of_bits % 64);
					end_mask = end_mask >> num_of_shifts;
					cur_bitfield = cur_bitfield & end_mask;
				}
				castedBitset[k] = cur_bitfield;

				cur_sum+=64;
				k++;
			}while(cur_sum < num_of_bits);

			if(bitSetSize > (unsigned int)k * 8)
				memset(bitSet + k * 8, 0, bitSetSize - k * 8);
			range_start = info->array[index].tid;
			filled = bitSetEntries / 8;
			if(bitSetEntries % 8 != 0)
				++filled;
			if(filled < bitSetSize / 2)
				reduceBitset(filled);
			journalOffset += to_be_removed;
            bitSetEntries -= to_be_removed;
            range_start = tid;
		}
    }
}
