#!/usr/bin/env bash

num_iters=$1 ; shift
work_dir=$1
for (( curr=0 , next=1 ; "$curr" < "$num_iters" ; curr++ , next++ )) ; do
  curr_str=`printf "%03d" ${curr}`
  next_str=`printf "%03d" ${next}`
  curr_dir=${work_dir}/pagerank_graph_${curr_str}
  next_dir=${work_dir}/pagerank_graph_${next_str}
  pig -x local -param PRGRPH="${curr_dir}" -param OUT="${next_dir}" pagerank.pig
  # pig -param PRGRPH="${curr_dir}" -param OUT="${next_dir}" pagerank.pig
done

