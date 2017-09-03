#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "jobscheduler.hpp"
#include <pthread.h>

extern int               num_of_threads;
extern uint64_t          finishedJobs;
extern pthread_mutex_t   listMtx;
extern pthread_cond_t    listCv;
extern pthread_mutex_t   finishedMtx;
extern pthread_cond_t    finishedCv;

void JobScheduler::enqueueJobs(List<jsValidationQueries_t> jsvl, uint64_t vid){
    jsValidationQueries_t       temp;
    node<jsValidationQueries_t> *cur;

    jobsAmount = 0;

    pthread_mutex_lock(&listMtx);

    cur = jsvl.get_root();
    while(1){
        if(cur != NULL && cur->data.v->validationId <= vid){
            jobs.push_back(&(cur->data));
            cur = cur->next;
        }else{
            break;
        }
    }
    jobsAmount = jobs.get_size();

    pthread_mutex_lock(&finishedMtx);
    pthread_cond_broadcast(&listCv);
    pthread_mutex_unlock(&listMtx);
    while(finishedJobs < jobsAmount){
        pthread_cond_wait(&finishedCv, &finishedMtx);
    }

    finishedJobs = 0;

    pthread_mutex_unlock(&finishedMtx);
}

jsValidationQueries_t *JobScheduler::getJob(){
    jsValidationQueries_t *toReturn = NULL;

    jobs.pop(&toReturn);

    return toReturn;
}

void JobScheduler::shutDown(){
    jsValidationQueries_t *exitJob;

    jobsAmount = num_of_threads;

    pthread_mutex_lock(&listMtx);

    for(uint32_t i = 0; i < jobsAmount; ++i){
        exitJob = new jsValidationQueries_t;
        exitJob->result = -1;

        jobs.push_back(exitJob);
    }

    pthread_mutex_lock(&finishedMtx);
    pthread_cond_broadcast(&listCv);
    pthread_mutex_unlock(&listMtx);
    while(finishedJobs < jobsAmount){
        pthread_cond_wait(&finishedCv, &finishedMtx);
    }
    pthread_mutex_unlock(&finishedMtx);
}
