#!/bin/bash

EXEC_PATH="./build/db_bench"
LOG_STORE_PATH="~/bily/nvlsm/log"
MODEL_NAME="nvlsm"

POSITIONNAL=()
while [[ $# -gt 0 ]]; do
    key="$1"

    case $key in 
            --exec_path)
                    EXEC_PATH="$2"
                    shift
                    shift
                    ;;
            --log_path)
                    LOG_STORE_PATH="$2"
                    shift
                    shift
                    ;;
            *)
                    POSITIONAL+=("$1")
                    shift
                    ;;
    esac
done
cur_date=`date "+%Y-%m-%d-%H-%M-%S"`
log_dir="$(pwd)/temp/log/${cur_date}"
mkdir -p ${log_dir}

value_size=("64" "256" "1024" "4096" "16384" "65536" )
doOneBenchmarkTest (){
# log path
# nvlsm exec path
        model_name=$3
        bench_name=$1
        value_size=$2
        log_file_name="${model_name}_${bench_name}_${value_size}"
        #log_path="$(pwd)/log/${log_file_name}"
        log_path="${log_dir}/${log_file_name}"

        exec_str="$EXEC_PATH --benchmarks=${bench_name},stats --histogram=1 --value_size=${value_size} | tee ${log_path}"
        echo ${exec_str}
        eval ${exec_str}
}

bench_names=("fillseq" "fillrandom" "readseq" "readrandom")
for bench in ${bench_names[*]}; do
    for value in ${value_size[@]}; do
        doOneBenchmarkTest      ${bench} ${value}   "nvlsm"
    done
done

#echo "exec_path $EXEC_PATH log_path: $LOG_STORE_PATH"
#cur_time=$(date -I"seconds")
#log_time_path="${LOG_STORE_PATH}/${cur_time}" 
##mkdir -p ${log_time_path}
##value_size=("64" "256" "1024" "4096" "16384" "65536" )
#for value in ${value_size[*]}; do
#    exe_str="${EXEC_PATH} --value_size=${value} 2>&1 | tee ${log_time_path}"
#    #echo $exe_str
#    #eval $exe_str
#
#done



