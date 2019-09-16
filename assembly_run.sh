#!/bin/bash

num=$1 #data number ( default : musical_xx)
dis=$2 #isDistribute?
com=$3 #dose need to compile?
class=$4

if [ ${dis} -eq 0 ];then
	master="local[4]"					#for local mode(not distributed)
elif [ $[dis] -eq 1 ];then
	master="spark://192.168.0.11:7077"  #for spark standalone
else 
	master="mesos://192.168.0.11:5050"	#for mesos cluster manager
fi

if [ ${com} -eq 1 ];then
	sbt clean assembly
fi


rm -rf final_result
../spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class ds_join.$class --master $master /home/user/Desktop/hongji/Dima_Ds_join/target/scala-2.11/Dima-DS-assembly-1.0.jar $num > log
#../spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class ds_join.DS_SimJoin_stream --master $master /home/user/Desktop/hongji/Dima_Ds_join/target/scala-2.11/Dima-DS-assembly-1.0.jar $num > log
#../spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class ds_join.DimaJoin_alone --master $master /home/user/Desktop/hongji/Dima_Ds_join/target/scala-2.11/Dima-DS-assembly-1.0.jar $num > log
#cat log
