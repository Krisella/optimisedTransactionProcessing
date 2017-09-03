#ifndef _JOBSCHEDULER_HPP_
#define _JOBSCHEDULER_HPP_

#include <stdint.h>
#include "structs.hpp"
#include "list.hpp"

struct jsValidationQueries_t{
    ValidationQueries_t *v;
    int8_t             result;

    jsValidationQueries_t():v(NULL), result(0){};
};

class JobScheduler{
private:
    uint64_t                        jobsAmount;
    List<jsValidationQueries_t*>    jobs;
public:
    JobScheduler(){};
    ~JobScheduler(){};

    uint64_t getJobsAmount(){return jobsAmount;};

    void enqueueJobs(List<jsValidationQueries_t> jsvl, uint64_t vid);

    jsValidationQueries_t *getJob();

    bool gotJobs(){return !jobs.is_empty();};

    void shutDown();
};

#endif
