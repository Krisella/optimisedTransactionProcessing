# optimisedTransactionProcessing

An implementation of the SIGMOD 2015 contest. A demonstration of its performance can be found in readme.pdf

## SIGMOD 2015 Task Overview

The ACID principle, mandating atomicity, consistency, isolation and durability is one of the cornerstones of relational database management systems. In order to achieve the third principle, isolation, concurrent reads and writes must be handled carefully.

In practice, transaction processing is often done in an optimistic fashion, where queries and statements are processed first, and only at the commit point the system validates if there was a conflict between readers and writers.

In this challenge, we simulate this in a slightly simplified way: The system processes already executed transactions and only needs to check efficiently whether concurrent queries conflict with them. So, in short: we issue a list of insert and delete statements and your program needs to figure out whether given predicates match the inserted or deleted data.

For more details go to: http://db.in.tum.de/sigmod15contest/task.html
