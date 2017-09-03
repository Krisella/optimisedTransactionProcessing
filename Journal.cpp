/*
 * Journal.cpp
 *
 *  Created on: Nov 23, 2015
 *      Author: krisella
 */

#include "Journal.hpp"

#define INCREASE_STEP 100

int Journal::insertJournalRecord(uint64_t* rec,uint64_t tid)
{
	if(last_index==rows-1)
		increaseJournal();

	last_index++;
	journal[last_index].rec_array=rec;
	journal[last_index].tid=tid;
	return 0;
}

JournalArrayEntry* Journal::getJournalRecords(uint64_t range_start, int32_t* pos)
{
	JournalArrayEntry temp;
	temp.tid=range_start;
	int32_t index=binarySearch<JournalArrayEntry,int32_t>(journal,temp,0,last_index);
	*pos=index;

	return journal;
}

int Journal::increaseJournal()
{
	JournalArrayEntry* new_journal;
	new_journal = new JournalArrayEntry[rows*2];
	memcpy(new_journal,journal,rows*sizeof(JournalArrayEntry));
	delete [] journal;
	journal=new_journal;
	rows *= 2;
	return 0;
}


