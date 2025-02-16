# reader_writer

A program in java that simulates a database and people wanting to access information (readers)  and people
trying to change information (writers).Each type has a pseudo-work to it (sleep) to simulate work,the goal 
is to avoid race conditions (classic reader writer) where readers can read at the same time but when a writer 
is changeing an index of the database no work should be done,the program tries 3 different ways,reader priority
,writer priority,and writer priority with java monitor.A costum threadpool and  thread-safe task queue is used.
