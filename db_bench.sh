#!/bin/bash

EXEC_PATH="./build/db_bench"
LOG_STORE_PATH="~/bily/nvlsm/log"

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


echo "exec_path $EXEC_PATH log_path: $LOG_STORE_PATH"
cur_time=$(date -I"seconds")
log_time_path="${LOG_STORE_PATH}/${cur_time}" 
#mkdir -p ${log_time_path}
value_size=("64" "256" "1024" "4096" "16384" "65536" )
for value in ${value_size[@]} do
    exe_str="${EXEC_PATH} "--value_size=${value}" 2>&1 | tee ${log_time_path}"
    echo $exe_str
    #eval $exe_str
        

done



