#!/bin/bash


# "/root/bily/leveldb_log"
LOG_DIR="${1:-/root/bily/leveldb_log/2022-01-11-11-34-56}" 
OUTPUT_DATA_FILE="all_data.csv"
DATA_PATH=${LOG_DIR}/${OUTPUT_DATA_FILE}

pushd ${LOG_DIR}
#echo $(pwd)
echo model,disk_type,write_bench_name,thread,value_size,write_latency_ms/OP,\
    write_rate_MB/s,overwrite_latency_ms/OP,overwrite_rate_MB/s,\
    read_seq_latency_ms/op,read_seq_rate_MB/s,\
    read_random_latency_ms/op | tee ${OUTPUT_DATA_FILE}

# date="2022-01-04-02-21-38"
MODEL_LOG_NAME=("leveldb_log" "leveldb_nvm_log" "nvlsm_log") 
function join_by() {
    local IFS="$1"
    shift
    echo "$*"
    # return val
}

function pop_last_ele() {
    input=$2
    local IFS=$1
    arr=(${input[@]})
    # echo ${arr[2]}
    unset 'arr[${#arr[@]}-1]'
    IFS=' '
    ret_val=$( join_by $1 "${arr[@]}" )
    
    echo $ret_val
}

# test_str="as_df_sd_a"
# pop_last_ele "_" $test_str

# MODELS=()
# for model_log in ${MODEL_LOG_NAME[@]}; do
#     IFS='_'
#     model_log=(${model_log[@]})
#     unset 'model_log[${#model_log[@]}-1]'
#     IFS=' '
#     model_log=$( join_by _ ${model_log[@]} )

#     # echo ${model_log}
#     MODELS+=(${model_log})
# done
# echo ${MODELS[@]}
# IFS=' '
for model in "${MODEL_LOG_NAME[@]}"; do

    model_log_path="${LOG_DIR}/${model}"
    #all_file_names=`ls ${model_log_path}`

    pushd ${model_log_path}
    echo ${model_log_path}
    for file in *; do 


        #echo ${file}
        # disk write_bench thread value_size.log
        write_latency=""
        write_rate=""
        read_seq_latency=""
        read_seq_rate=""
        read_random_latency=""
        orig_ifs=$IFS
        IFS='.'

        arg_arr=($file)
        arg_arr=( ${arg_arr[0]} )
        # echo ${arg_arr[@]}

        IFS='_'
        arg_arr=( ${arg_arr[@]} )
        # echo ${arg_arr[2]}

        write_bench_name=${arg_arr[1]}
        # echo ${write_bench_name}
        # echo ${arg_arr[@]}
        IFS=' '
        # write_stats=`cat ${file} | grep ${write_bench_name} \
                #    | awk '{print $3","$5}'`
        write_stats=`cat ${file} | grep -o "${write_bench_name}.*" \
                    | awk '{print $3","$5}'`
        overwrite_stats=`cat ${file} | grep -o 'overwrite.*' \
                    | awk '{print $3","$5}'`
        read_seq_stats=`cat ${file} | grep -o 'readseq.*' | awk '{print $3","$5}'`
        # readrandom is at the end of a long long string, so grep readrandom will not
        # return a string starting with readrandom
        # read_random_stats=`cat ${file} | grep readrandom | awk '{print $55}'`
        # read_random_stats=`cat ${file} | grep -o 'readrandom.*' | awk '{print $3}'`
        read_random_stats=`cat ${file} | sed -n -e 's/^.*\(readrandom\)/\1/p' | awk '{print $3}'`
        
        # read_random_stats=( ${read_random_stats[@]} )
        # echo ${read_random_stats[3]}

        # echo ${write_stats}
        # echo ${overwrite_stats}
        # echo ${read_seq_stats}
        # echo ${read_random_stats}


        model_name=$( pop_last_ele "_" ${model} )
        echo ${model_name},${arg_arr[0]},${write_bench_name},${arg_arr[2]},\
            ${arg_arr[3]},${write_stats},${overwrite_stats},${read_seq_stats},\
            ${read_random_stats} \
            | tee -a ${DATA_PATH}

        # IFS='_'
        # IFS=' '
    done
    popd
done
popd
