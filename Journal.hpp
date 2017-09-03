/*
 * Journal.h
 *
 *  Created on: Nov 23, 2015
 *      Author: krisella
 */

#ifndef JOURNAL_H_
#define JOURNAL_H_

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include "list.hpp"
#include "bsearch.hpp"

struct JournalArrayEntry{
	uint64_t tid;
	uint64_t* rec_array;

	friend bool operator<(const JournalArrayEntry &l, const JournalArrayEntry &r){
		return l.tid < r.tid;
	}
	friend bool operator>(const JournalArrayEntry &l, const JournalArrayEntry &r){
		return l.tid > r.tid;
	}
	friend bool operator==(const JournalArrayEntry &l,const JournalArrayEntry &r){
		return l.tid==r.tid;
	}
	JournalArrayEntry():tid(-1),rec_array(NULL){}
};

class Journal{

private:
	uint32_t rows;
	uint32_t columns;
	uint32_t last_index;
	JournalArrayEntry* journal;

public:
	Journal(uint32_t c):rows(100),columns(c),last_index(-1){
		journal = new JournalArrayEntry[rows];
	};
	~Journal(){
		for(uint32_t i=0;i<rows;i++)
			if(i <= last_index)
				delete [] journal[i].rec_array;
		delete [] journal;
	};

	int insertJournalRecord(uint64_t*,uint64_t);
	int increaseJournal();
	JournalArrayEntry* getJournalRecords(uint64_t,int32_t*);
	uint32_t get_col_count(){return columns;};
	uint32_t get_rows(){return rows;};
	uint64_t* get_record(uint32_t index){return journal[index].rec_array;};
	JournalArrayEntry get_arrayEntry(uint32_t index){return journal[index];};
	uint32_t get_last_index(){return last_index;};
	JournalArrayEntry* get_journal_array_entry(){return journal;};
	uint64_t get_tid(uint32_t offset){return journal[offset].tid;}

	void print_journal(){for(uint32_t i=0;i<=last_index;i++){
							printf("tid: %lu",journal[i].tid);
							for(uint32_t j=0;j<columns;j++)
								printf(" %lu",journal[i].rec_array[j]);
							printf("\n");}
							}
};
class JournalsArray{

private:
	uint32_t relationsCount;
	Journal** journalArr;

public:
	JournalsArray(uint32_t relation_count, uint32_t* columnCounts ):relationsCount(relation_count){
		journalArr = new Journal*[relation_count];
		for(uint32_t i=0;i<relation_count;i++)
			journalArr[i]=new Journal(columnCounts[i]);
	};
	~JournalsArray(){
		for(uint32_t i=0;i<relationsCount;i++)
			delete journalArr[i];
		delete [] journalArr;

	};
	uint32_t get_column_count(int index){return journalArr[index]->get_col_count();};
	uint32_t get_relation_rows(int relation){return journalArr[relation]->get_rows();};
	int insert_journal_rec(int index, uint64_t* rec, uint64_t tid){journalArr[index]->insertJournalRecord(rec,tid);return 0;};
	uint64_t* get_journal_record(uint32_t journal_index,uint32_t relation){return journalArr[relation]->get_record(journal_index);};
	JournalArrayEntry get_journalArrayEntry(uint32_t journal_index,uint32_t relation){return journalArr[relation]->get_arrayEntry(journal_index);};
	uint32_t get_last_index(uint32_t relationID){return journalArr[relationID]->get_last_index();}
	JournalArrayEntry* getJournalRecords(uint32_t relation,uint64_t from,int32_t* index){return journalArr[relation]->getJournalRecords(from,index);};
	uint32_t get_num_of_relations(){return relationsCount;};
	Journal* get_journal(uint32_t relation){return journalArr[relation];};

	void print_journal(uint32_t rel){journalArr[rel]->print_journal();};
	void print_journals(){for(uint32_t i=0;i<relationsCount;i++){
							printf("Journal Number %d \n",i);
							journalArr[i]->print_journal();}}
};
#endif /* JOURNAL_H_ */
