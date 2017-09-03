/*
 * Inspired from SIGMOD Programming Contest 2015.
 * http://db.in.tum.de/sigmod15contest/task.html
 * Simple requests parsing and reporting.
**/

#include "ventry.hpp"
#include "bsearch.hpp"
#include "hash.hpp"
#include "rarray.hpp"
#include "infoarray.hpp"
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <stdint.h>
#include <time.h>
#include <pthread.h>
#include "structs.hpp"
#include "SpookyV2.h"
#include "city.h"
#include "jobscheduler.hpp"


#define GLOBAL_DEPTH 10
enum{LEVEL1, LEVEL2, LEVEL3};

static 						uint32_t* schema = NULL;

int 						chosenLevel = LEVEL1;
bool 						useTransactionHash = false;
uint64_t					forgotId;
JournalsArray 				*journals_arr;
Hash<RangeArray>			*hash_array;
Hash<uint32_t>				*tHash;
Hash<ValidationEntry>  		*vEntryHash;
List<ValidationQueries_t*> 	validations_list;
infoArray 					*infoarrays;
JobScheduler				js;

int 						rounds = 3;
int 						curRound = 0;
uint32_t					num_of_threads = 1;
uint64_t 					lastFlush;
pthread_t 					threads[8];
List<jsValidationQueries_t> jsValidations;
uint64_t					finishedJobs;
pthread_mutex_t 			listMtx = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t 				listCv = PTHREAD_COND_INITIALIZER;
pthread_mutex_t				finishedMtx = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t 				finishedCv = PTHREAD_COND_INITIALIZER;

int calcByHash(Query_t *q, RAEntry entry, uint32_t c0_pos){
	int 			result = 1;
	Column_t 		*columns = q->columns;
	uint64_t 		*record;

	for(int i = 0; i < 2; i++){
		if(entry.flags[i] == 1){
			record = journals_arr->get_journal_record(entry.offset[i],
														q->relationId);
			for(uint32_t j = 0; j < q->columnCount; j++){
				if(j == c0_pos) continue;
				result = calculateOp_t(columns[j].op,columns[j].value,
								  record[columns[j].column]);
				if(result == 0) break;
			}
			if(result == 1) return 1;
		}
	}
	return 0;
}

int executeValidation(ValidationQueries_t *v){
	uint64_t 			from = v->from, to = v->to;
	uint32_t			*tOffset, queryCount, columnCount;
	int64_t 			raPos;
	uint32_t 			i, j, relationId, c0_pos;
	int 				flag;
	RAEntry_ptr			ptr;
	char				*query_reader = v->queries;
	JournalArrayEntry 	jaentry;

	query_reader = v->queries;
	queryCount = v->queryCount;

	for(i = 0; i < queryCount; i++){

		int column_result = 1;
		Query_t* q = (Query_t*) query_reader;
		relationId = q->relationId;
		columnCount = q->columnCount;

		flag = -1;
		for(j = 0; j < columnCount; j++){
			if(q->columns[j].column == 0){
				if(q->columns[j].op == Equal){
					flag = 0;
					c0_pos = j;
				}
				break;
			}
		}

		if(!flag){
			ptr = hash_array[relationId].
				  getHashRecords<RAEntry_ptr>(q->columns[c0_pos].value, from,to);
			if(ptr.pos == -1 || ptr.array == NULL){
				query_reader += sizeof(Query_t) + (sizeof(Column_t) *
												   columnCount);
				continue;
			}else{
				raPos = ptr.pos;
				while(raPos >= 0 && ptr.array[raPos].transactionId >= from &&
					  ptr.array[raPos].transactionId <= to){
					if(calcByHash(q, ptr.array[raPos], c0_pos)){
						return 1;
					}
					raPos--;
				}
				raPos = ptr.pos + 1;
				while(raPos < ptr.limit && ptr.array[raPos].transactionId >= from
					  && ptr.array[raPos].transactionId <= to){
					if(calcByHash(q, ptr.array[raPos], c0_pos)){
						return 1;
					}
					raPos++;
				}
			}
		}else{
			if(useTransactionHash)
				tOffset = tHash[q->relationId].getHashRecord(from);
			if(useTransactionHash && tOffset != NULL){
				uint32_t 			lIndex;
				JournalArrayEntry 	*journal;
				j = *tOffset;
				journal = journals_arr->get_journal(relationId)->get_journal_array_entry();
				lIndex = journals_arr->get_last_index(relationId);
				while(journal[j].tid <= to){
					for(uint32_t k = 0; k < columnCount; k++){
						column_result = calculateOp_t(q->columns[k].op,q->columns[k].value,
							journal[j].rec_array[q->columns[k].column]);
						if(column_result == 0)
							break;
					}
					if(column_result == 1){
						return 1;
					}
					j++;
					if(j > lIndex) break;
				}
			}else{
				JournalArrayEntry* journal;
				int32_t pos;
				uint32_t rows=journals_arr->get_last_index(relationId);
				flag=0;
				journal=journals_arr->getJournalRecords(relationId,from,&pos);

				if(pos>=0){
					if(pos!=0)
						for(int32_t i=pos-1;i>=0;i--)
						{
							if(journal[i].tid<from || journal[i].tid>to)
								break;

							flag=1;
							for(j = 0; j < columnCount; ++j){
								column_result=calculateOp_t(q->columns[j].op,q->columns[j].value,journal[i].rec_array[q->columns[j].column]);
								if(column_result==0)
									break;
							}
							if(column_result==1){
								return 1;
							}

						}
						if(columnCount==0 && flag==1){
							return 1;
						}

						for(uint32_t i=pos;i<=rows;i++){
							if(journal[i].tid<=to && journal[i].tid >= from){
								flag=1;
								for(j=0;j<columnCount;j++){
									column_result=calculateOp_t(q->columns[j].op,q->columns[j].value,journal[i].rec_array[q->columns[j].column]);
									if(column_result==0)
										break;
								}
								if(column_result==1){
									return 1;
								}
							}
							else
								break;
						}
						if(columnCount==0 && flag==1){
							return 1;
						}
					}
			}
		}
		query_reader += sizeof(Query_t) + (sizeof(Column_t) * columnCount);
	}

	return 0;
}

void *workerThread(void *arg){
	jsValidationQueries_t  	*job;
	bool					exiting = false;

	pthread_detach(pthread_self());

	while(!exiting){
		pthread_mutex_lock(&listMtx);
		while(!js.gotJobs()){
			pthread_cond_wait(&listCv, &listMtx);
		}

		job = js.getJob();

		pthread_mutex_unlock(&listMtx);
		if(job->result != -1){
			job->result = executeValidation(job->v);
		}else{
			delete job;
			exiting = true;
		}

		pthread_mutex_lock(&finishedMtx);

		++finishedJobs;
		if(finishedJobs == js.getJobsAmount()){
			pthread_cond_signal(&finishedCv);
		}
		pthread_mutex_unlock(&finishedMtx);
	}

	pthread_exit(NULL);
}

static void processDefineSchema(DefineSchema_t *s){
	uint32_t i;

	if(schema != NULL) free(schema);
	schema = (uint32_t*) malloc(sizeof(uint32_t) * s->relationCount);

	for(i = 0; i < s->relationCount; i++){
		schema[i] = s->columnCounts[i];
	}

	journals_arr = new JournalsArray(s->relationCount, s->columnCounts);
	hash_array = new Hash<RangeArray>[s->relationCount];
	tHash = new Hash<uint32_t>[s->relationCount];
	vEntryHash = new Hash<ValidationEntry>[s->relationCount];
	infoarrays = new infoArray[s->relationCount];
	for(i = 0; i < s->relationCount; i++){
		hash_array[i].init(GLOBAL_DEPTH, 1);
		tHash[i].init(GLOBAL_DEPTH, 1);
		vEntryHash[i].init(GLOBAL_DEPTH, 32);
		initInfoArray(&(infoarrays[i]));
	}

	if(chosenLevel == LEVEL3){
		for(i = 0; i < num_of_threads; ++i){
			pthread_create(&(threads[i]), NULL, workerThread, NULL);
		}
	}
}

static void processTransaction(Transaction_t *t){
	uint32_t 		i;
	RangeArray 		*rarray;
	uint64_t 		*new_rec, *to_delete_rec, transactionId;
	uint32_t 		journal_index, startIndex, relationId;
	uint32_t 		*tOffset = NULL, rowCount;
	int 			flag;
	const char 		*reader = t->operations;

	transactionId = t->transactionId;

	for(i=0; i < t->deleteCount; i++){
		flag = -1;
		const TransactionOperationDelete_t* o =
				(TransactionOperationDelete_t*) reader;
		relationId = o->relationId;
		rowCount = o->rowCount;

		journal_index = journals_arr->get_last_index(relationId) + 1;
		startIndex = journal_index;
		for(uint32_t k = 0; k < rowCount; k++){
			rarray = hash_array[relationId].getHashRecord(o->keys[k]);
			if(rarray != NULL){
				if(!rarray->isInsert())
					continue;
				if(flag) flag = 0;
				journal_index = rarray->getLastInsert();
				new_rec = new uint64_t[schema[relationId]];
				to_delete_rec = journals_arr->get_journal_record(journal_index,
																   relationId);
				for(uint32_t j = 0; j < schema[relationId]; j++)
					new_rec[j] = to_delete_rec[j];
				journals_arr->insert_journal_rec(relationId, new_rec,
												   transactionId);
				rarray->insertEntry(transactionId,
									journals_arr->get_last_index(relationId),
																   DELETE);
			}
		}
		if((useTransactionHash || chosenLevel == LEVEL2) && !flag){
			if(useTransactionHash){
		    	tOffset = tHash[relationId].getHashRecord(transactionId);
		    }
		    if(!useTransactionHash || tOffset == NULL){
		    	if(useTransactionHash){
			    	tOffset = new uint32_t;
			    	*tOffset = startIndex;
			    	tHash[relationId].insertHashRecord(transactionId, tOffset);
			    }
		    	if(chosenLevel == LEVEL2){
			    	infoarrays[relationId].array[infoarrays[relationId].index].tid =
			    		transactionId;
			    	infoarrays[relationId].array[infoarrays[relationId].index].jOffset =
			    		startIndex;
			    	infoarrays[relationId].index++;
			    	if(infoarrays[relationId].index >= infoarrays[relationId].size)
			    		doubleInfoArray(&(infoarrays[relationId]));
			    }
		    }
		}
		reader += sizeof(TransactionOperationDelete_t) + (sizeof(uint64_t) *
													  rowCount);
	}

	for(i = 0; i < t->insertCount; i++){
		const TransactionOperationInsert_t* o = (TransactionOperationInsert_t*) reader;
		relationId = o->relationId;
		rowCount = o->rowCount;

		if(chosenLevel == LEVEL2 || useTransactionHash){
			if(useTransactionHash){
				tOffset = tHash[relationId].getHashRecord(transactionId);
			}
			if(!useTransactionHash || tOffset == NULL){
				if(useTransactionHash){
					tOffset = new uint32_t;
					*tOffset = journals_arr->get_last_index(relationId) + 1;
					tHash[relationId].insertHashRecord(transactionId, tOffset);
				}
				if(chosenLevel == LEVEL2){
					infoarrays[relationId].array[infoarrays[relationId].index].tid =
			    		transactionId;
			    	infoarrays[relationId].array[infoarrays[relationId].index].jOffset =
			    		*tOffset;
			    	infoarrays[relationId].index++;
			    	if(infoarrays[relationId].index >= infoarrays[relationId].size)
						doubleInfoArray(&(infoarrays[relationId]));
				}
			}
		}
		for(uint32_t k = 0; k < rowCount; k++){
			new_rec = new uint64_t[schema[relationId]];
			for(uint32_t j = 0; j < schema[relationId]; j++)
				new_rec[j] = o->values[k * schema[relationId] + j];
			journals_arr->insert_journal_rec(relationId, new_rec,
											   transactionId);
			rarray = hash_array[relationId].getHashRecord(new_rec[0]);
			if(rarray == NULL){
				rarray = new RangeArray;
				hash_array[relationId].insertHashRecord(new_rec[0],
															rarray);
			}
			rarray->insertEntry(transactionId,
								journals_arr->get_last_index(relationId),
															   INSERT);
		}
		reader += sizeof(TransactionOperationInsert_t) + (sizeof(uint64_t) *
														  rowCount *
														  schema[relationId]);
	}
}

static void processValidationQueries(ValidationQueries_t **v){

	ValidationQueries_t 	*validation = *v, *temp;
	char					*query_reader = validation->queries;
	uint64_t 				key, from = validation->from, to = validation->to;
	uint32_t 				relationId, queryCount, columnCount;
	ValidationEntry 		*ventry;
	jsValidationQueries_t 	jsValidation;

	query_reader = validation->queries;
	*v = NULL;
	if(chosenLevel != LEVEL3){
		validations_list.push_back(validation);
	}else{
		jsValidation.v = validation;
		jsValidations.push_back(jsValidation);
	}

	queryCount = validation->queryCount;

	if(chosenLevel == LEVEL2){
		for(uint32_t i = 0; i < queryCount; ++i){
			Query_t* q = (Query_t*) query_reader;
			relationId = q->relationId;
			columnCount = q->columnCount;

			for(uint32_t j = 0; j < columnCount; ++j){
				key = CityHash64((char*)(&(q->columns[j])), sizeof(Column_t));

				ventry = vEntryHash[relationId].getHashRecord(key);
				if(ventry == NULL){
					ventry = new ValidationEntry(from, to, q->columns[j]);
					vEntryHash[relationId].insertHashRecord(key, ventry);
				}
				if(ventry->vList.peek(&temp) == -1 || temp->validationId !=
				   validation->validationId){
					ventry->addValidation(validation);
				}
			}
			query_reader += sizeof(Query_t) + (sizeof(Column_t) * columnCount);
		}
	}
}

uint32_t find_first_occurence_in_journal(uint64_t from, uint64_t to, Journal* journal){

	int32_t pos, i;
	JournalArrayEntry* array_entry;

	array_entry = journal->getJournalRecords(from, &pos);

	if(pos!=0){
		for( i=pos-1;i>=0;i--)
		{
			if(array_entry[i].tid<from || array_entry[i].tid>to){
				i++;
				break;
			}
		}
		return i;
	}else{
		return pos;
	}
}

void executeValidation2(ValidationQueries_t *v){
	uint64_t 				from = v->from, to = v->to, lastId;
	uint64_t 				key, temp;
	uint32_t 				relationId, i, j, offset, offset2, *tentry,
							queryCount, columnCount, num_of_bits = 0;
	char					*query_reader = v->queries;
	ValidationEntry 		*ventry;
	Journal					*journal;
	int						result;
	int						no_tid_flag = 0, flag;
	uint32_t 				c0_pos;
	RAEntry_ptr 			ptr;
	int64_t 				raPos;

	if(from <= forgotId) from = forgotId;
	query_reader = v->queries;
	queryCount = v->queryCount;

	for(i = 0; i < queryCount; ++i){

		Query_t* q = (Query_t*) query_reader;
		relationId = q->relationId;
		columnCount = q->columnCount;

		journal = journals_arr->get_journal(relationId);
		lastId = journal->get_arrayEntry(journal->get_last_index()).tid;

		if(columnCount == 0){
			offset = find_first_occurence_in_journal(from, to, journal);
			temp = journal->get_tid(offset);
			if(temp >= from && temp <= to){
				printf("1");
				return;
			}
			query_reader += sizeof(Query_t) + (sizeof(Column_t) * columnCount);
			continue;
		}

		//1st level hash
		flag = -1;
		for(j = 0; j < columnCount; j++){
			if(q->columns[j].column == 0){
				if(q->columns[j].op == Equal){
					flag = 0;
					c0_pos = j;
				}
				break;
			}
		}

		if(!flag){
			ptr = hash_array[relationId].
				  getHashRecords<RAEntry_ptr>(q->columns[c0_pos].value, from,to);
			if(ptr.pos == -1 || ptr.array == NULL){
				query_reader += sizeof(Query_t) + (sizeof(Column_t) * columnCount);
				continue;
			}else{
				raPos = ptr.pos;
				while(raPos >= 0 && ptr.array[raPos].transactionId >= from &&
					  ptr.array[raPos].transactionId <= to){
					if(calcByHash(q, ptr.array[raPos], c0_pos)){
						printf("1");
						return;
					}
					raPos--;
				}
				raPos = ptr.pos + 1;
				while(raPos < ptr.limit && ptr.array[raPos].transactionId >= from
					  && ptr.array[raPos].transactionId <= to){
					if(calcByHash(q, ptr.array[raPos], c0_pos)){
						printf("1");
						return;
					}
					raPos++;
				}
			}
		}else{
			if(useTransactionHash){
				tentry = tHash[relationId].getHashRecord(from);
			}
			if(!useTransactionHash || tentry == NULL){
				if(!findOffsetLeft(&(infoarrays[relationId]), from, to,
								   &offset)){
					no_tid_flag = 1;
				}
			}else{
				offset = *tentry;
			}
			if(!no_tid_flag){
				if(to >= lastId){
					offset2 = journal->get_last_index();
				}else{
					if(useTransactionHash){
						tentry = tHash[relationId].getHashRecord(to + 1);
					}
					if(!useTransactionHash || tentry == NULL){
						findOffset(&(infoarrays[relationId]), from, to, &offset2);
					}else{
						offset2 = *tentry;
					}
					--offset2;
				}
			}

			uint64_t 		*bitfields[columnCount];
			uint32_t		bitSetSizes[columnCount];
			uint32_t  		bitOffsets[columnCount];

			for(j = 0; j < columnCount; ++j){
				if(no_tid_flag) break;
				//key = SpookyHash::Hash64(&(q->columns[j]), sizeof(Column_t), 544897486465797897);
				key = CityHash64((char*)(&(q->columns[j])), sizeof(Column_t));

				ventry = vEntryHash[relationId].getHashRecord(key);
				result = ventry->inRange(from, to);
				if(result!=0){
					if(result == 1 || result == 4){
						ventry->addRange(to, journal, offset, RIGHT);
					}else if(result == 2 || result == 3){
						ventry->addRange(from, journal, offset, LEFT);
					}else if(result == 5){
						ventry->addRange(from, journal, offset, LEFT);
						ventry->addRange(to, journal, offset, RIGHT);
					}else if(result == 6){
						ventry->addRange(to, journal, offset, START);
					}
				}

				bitfields[j] = ventry->getConflicts(&num_of_bits, &(bitOffsets[j]),
													from, to, &(bitSetSizes[j]),
													offset, offset2);
			}

			if(no_tid_flag == 1){
				no_tid_flag = 0;
				query_reader += sizeof(Query_t) + (sizeof(Column_t) * columnCount);
				continue;
			}

			uint32_t 	cur_sum=0;
			uint64_t 	res;
			uint64_t 	cur_bitfield;
			uint64_t 	temp_bitfield;
			uint32_t 	k=0;

			do{
				for(j = 0; j < columnCount; ++j){
					uint32_t 	bitset_index = bitOffsets[j] / 64;
					offset = bitOffsets[j] % 64;

					cur_bitfield = bitfields[j][bitset_index + k];
					if(offset != 0){
						cur_bitfield >>= offset;
						if(cur_sum + 64 - offset != num_of_bits &&
						   bitset_index + k + 1 < (bitSetSizes[j] / 8)){
							temp_bitfield = bitfields[j][bitset_index + k + 1];
							temp_bitfield <<= (64 - offset);
							cur_bitfield |= temp_bitfield;
						}
					}

					if(j == 0)
						res = cur_bitfield;
					else
						res = res & cur_bitfield;

					if(cur_sum+64 > num_of_bits && j == 0){
						uint64_t end_mask = 0xFFFFFFFFFFFFFFFFULL;
						uint32_t num_of_shifts = 64 - (num_of_bits % 64);
						end_mask = end_mask >> num_of_shifts;
						res = res & end_mask;
					}
					if(res == 0)
						break;
				}
				if(res!=0){
					printf("1");
					return;
				}
				cur_sum += 64;
				k++;
			}while(cur_sum < num_of_bits);
		}
		query_reader += sizeof(Query_t) + (sizeof(Column_t) * columnCount);
	}
	printf("0");
}



static void processFlush(Flush_t *fl){
	ValidationQueries_t 	*v, *tempV;
	ValidationEntry 		*ventry;
	char					*query_reader;
	int 					size = validations_list.get_size();
	uint64_t				key, validationId;
	uint32_t				relationId, queryCount, columnCount;

	if(size!=0){
		validations_list.pop(&v);
		if(v->validationId > fl->validationId){
			validations_list.push_front(v);
			return;
		}
		while(v->validationId <= fl->validationId){
			if(chosenLevel == LEVEL1){
				printf("%d", executeValidation(v));
			}else if(chosenLevel == LEVEL2){
				executeValidation2(v);
				query_reader = v->queries;
				validationId = v->validationId;
				queryCount = v->queryCount;
				for(uint64_t i = 0; i < queryCount; ++i){
					Query_t* q = (Query_t*) query_reader;
					relationId = q->relationId;
					columnCount = q->columnCount;

					for(uint64_t j = 0; j < columnCount; ++j){
						key = CityHash64((char*)(&(q->columns[j])), sizeof(Column_t));

						ventry = vEntryHash[relationId].getHashRecord(key);
						if(ventry != NULL){
							if(!ventry->vList.peek(&tempV) &&
				   			   tempV->validationId == validationId){
								ventry->vList.pop(&tempV);
							}
							if(ventry->vList.is_empty()){
								vEntryHash[relationId].deleteValidationEntry(key);
							}
						}
					}
					query_reader += sizeof(Query_t) + (sizeof(Column_t) *
										   columnCount);
				}
			}
			if(v->validationId == fl->validationId){
				free(v);
				return;
			}
			free(v);
			if(validations_list.get_size() != 0)
				validations_list.pop(&v);
			else
				return;
		}
		if(v->validationId > fl->validationId)
			validations_list.push_front(v);
	}
}

static void processFlush2(Flush_t *fl){
	jsValidationQueries_t 		temp;
	int 						size = jsValidations.get_size();
	uint64_t					validationId;

	if(fl != NULL){
		lastFlush = fl->validationId;
		validationId = fl->validationId;
	}else{
		validationId = lastFlush;
	}
	++curRound;

	if(size != 0 && (curRound == rounds || fl == NULL)){
		curRound = 0;
		jsValidations.peek(&temp);
		if(temp.v->validationId <= validationId){
			js.enqueueJobs(jsValidations, validationId);
			jsValidations.peek(&temp);
			while(temp.v->validationId <= validationId){
				printf("%d", temp.result);
				jsValidations.pop(&temp);
				free(temp.v);
				if(jsValidations.peek(&temp) == -1) return;
			}
		}
	}
}

void destroy_validations(){
	int 					size = validations_list.get_size();
	ValidationQueries_t 	*v = NULL;
	jsValidationQueries_t 	jsv;

	for(int i = 0; i < size; i++){
		validations_list.pop(&v);
		free(v);
	}

	size = jsValidations.get_size();

	for(int i = 0; i < size; i++){
		jsValidations.pop(&jsv);
		free(jsv.v);
	}
}

static void processForget(Forget_t *fo){

	uint32_t relationsCount = journals_arr->get_num_of_relations();
	uint64_t transactionId = fo->transactionId;
	int64_t index;
	transactionInfo    temp;

	forgotId = transactionId;

	for(uint32_t i = 0; i < relationsCount; ++i){
		if(infoarrays[i].index != 0 ){
			if(infoarrays[i].array[infoarrays[i].index-1].tid<=transactionId)
				index = infoarrays[i].index - 1;
			else{
				temp.tid = transactionId;
				index = binarySearch<transactionInfo,int64_t>(infoarrays[i].array, temp, 0, infoarrays[i].index-1);
				if(index < 0)
					return;
				if(infoarrays[i].array[index].tid == transactionId)
					index++;
			}

			vEntryHash[i].forget(transactionId, &(infoarrays[i]), index);

			infoArrayForget(&(infoarrays[i]), transactionId, index);
		}
	}
}

int main(int argc, char **argv) {
	char 						*argCheck;
	MessageHead_t 				head;
	void 						*body = NULL;
	uint32_t 					len;
	struct timespec 			start, stop;
	bool						flags[4] = {false, false, false, false};

	if(argc != 1){
		for(int i = 1; i < argc; ++i){
			if(argv[i][0] == '-'){
				if(!flags[0] && strlen(argv[i]) == 3 && argv[i][1] == 'l' &&
				   argv[i][2] > '0' && argv[i][2] < '4'){
					chosenLevel = argv[i][2] - 49;
				}else if(!flags[1] && strlen(argv[i]) == 2 && argv[i][1] == 't'){
					useTransactionHash = true;
				}else if(!flags[2] && strlen(argv[i]) == 4 && argv[i][1] == 't' &&
						 argv[i][2] == 'h' && argv[i][3] > '0' &&
						 argv[i][3] < '9'){
					num_of_threads = argv[i][3] - 48;
				}else if(!flags[3] && argv[i][1] == 'r'){
					rounds = strtol(&(argv[i][2]), &argCheck, 0);
				    if(*argCheck != '\0' || rounds < 1){
				        if(*argCheck != '\0')
				        	fprintf(stderr, "Invalid argument for ROUNDS.\n");
				        else
				        	fprintf(stderr, "ROUNDS must be a positive number.\n");
				        exit(1);
				    }
				}else{
					fprintf(stderr, "Invalid or duplicate argument.\n");
					exit(1);
				}
			}else{
				fprintf(stderr, "Invalid or duplicate argument.\n");
				exit(1);
			}
		}
	}

	clock_gettime(CLOCK_REALTIME, &start);

	while(1){
		if(read(0, &head, sizeof(head)) <= 0){
			return -1;
		}

		if(body != NULL) free(body);
		if(head.messageLen > 0){
			body = malloc(head.messageLen * sizeof(char));
			if (read(0, body, head.messageLen) <= 0){
				printf("err");
				return -1;
			}
			len -= (sizeof(head) + head.messageLen);
		}

		switch(head.type){
		case Done: {
			if(chosenLevel == LEVEL3 && curRound != 0){
				processFlush2(NULL);
			}
			uint32_t relationsCount = journals_arr->get_num_of_relations();
			for(uint32_t i = 0;i < relationsCount; i++){
				delete[] infoarrays[i].array;
			}
			delete[] infoarrays;
			delete [] hash_array;
			delete [] tHash;
			delete [] vEntryHash;
			delete journals_arr;
			free(schema);
			destroy_validations();
			if(chosenLevel == LEVEL3) js.shutDown();
			clock_gettime(CLOCK_REALTIME, &stop);
			fprintf(stderr, "Finished in about %lf seconds.\n", (stop.tv_sec -
					start.tv_sec) + (stop.tv_nsec - start.tv_nsec) / 1E9);
			return 0;}
		case DefineSchema:
			processDefineSchema((DefineSchema_t*)body);
			break;
		case Transaction:
			processTransaction((Transaction_t*)body);
			break;
		case ValidationQueries:
			processValidationQueries((ValidationQueries_t**)&body);
			break;
		case Flush:
			if(chosenLevel != LEVEL3){
				processFlush((Flush_t*)body);
			}else{
				processFlush2((Flush_t*)body);
			}
			break;
		case Forget:
			if(chosenLevel == LEVEL2)
				processForget((Forget_t*)body);
			break;
		default:
			return -1;
		}
	}

	return 0;
}



