# DIMA + DL-Join ( Distributed Streaming Similarity Join ) 
## how to compile spark project
```
> sbt clean assembly
```
## how to run code
```
example)
> ./assembly_run.sh 1000 2 1 DS_Sim -> run DS_Sim class, and run mesos cluster, using musical_1000(mongodb)  
> ./assembly_run.sh 3000 0 1 DS_join -> run DS_join class, and run local mode, using musical_3000(mongodb)
```
argument 1 : data number.

argument 2 : isDistributed? (0: local, 1: standalone, 2:cluster(mesos)).

argument 3 : sbt clean or not? (0: not compile, 1: compile).

argument 4 : class name.




