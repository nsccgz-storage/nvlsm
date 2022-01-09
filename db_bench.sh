#!/bin/bash

EXEC_PATHS=( "~/bily/leveldb_hdd/leveldb/build/db_bench" "~/bily/leveldb/build/db_bench"  "~/bily/nvlsm/build/db_bench")
LOG_STORE_PATH="~/bily/leveldb_log"
#LOG_STORE_PATHS=("~/bily/leveldb_log" "~/bily/leveldb_nvm")
LOG_DIR_NAMES=("leveldb_log" "leveldb_nvm_log" "nvlsm_log")
MODEL_NAME="nvlsm"

DB_DIR_PAIRS=(
"HDD            /mnt/hdd2/leveldb" 
"NVMeSSD            /mnt/nvme_ssd/leveldb"  
"PMEM          /mnt/pmem/leveldb"  
)

DISKS=("HDD" "NVMeSSD" "PMEM")
DIRS=("/mnt/hdd2/leveldb" "/mnt/nvme_ssd/leveldb" "/mnt/pmem/leveldb")
for (( i = 0; i < ${#DISKS[@]}; i++)); do
    echo ${DISKS[$i]} 
    echo ${DIRS[$i]}
done

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
#log_dir="$(pwd)/temp/log/${cur_date}"
log_dir="${LOG_STORE_PATH}/${cur_date}"
model_log_paths=()
for (( i = 0; i < ${#LOG_DIR_NAMES[@]}; i++)); do
    model_log_path="${log_dir}/${LOG_DIR_NAMES[$i]}"
    echo $model_log_path
    mkdir -p "${model_log_path}"
    if [ $? -ne 0 ]; then 
        echo "mkdir not ok"
    fi
    echo "fuck"
    model_log_paths+=("${model_log_path}")
done
#mkdir -p ${log_dir}

doOneBenchmarkTest (){
        disk_type=$1
        db_dir=$2
        value_size=$3
        write_bench_name=$4
        thread=$5
        exec_path=$6
        model_log_dir=$7
        num=$8
        #model_name=$3
        #bench_name=$1
        #value_size=$2
    log_file_name="${disk_type}_${write_bench_name}_${thread}_${value_size}.log"
        #log_path="$(pwd)/log/${log_file_name}"
    log_path="${model_log_dir}/${log_file_name}"
    echo ${log_path}

    exec_str="$exec_path --benchmarks=${write_bench_name},overwrite,stats,readseq,readrandom \
             --value_size=${value_size} \
            --db=${db_dir}  \
            --threads=${thread} \
            --num=${num} \
            2>&1 | tee ${log_path}"
    echo ${exec_str}
    eval ${exec_str}
}

value_size=("64" "256" "1024" "4096" "16384") #65536
bench_names=("fillseq" "fillrandom") 
thread_num=("1" "2")
total_value_size=1073741824

for thread in ${thread_num[*]}; do


#for DB_DIR_PAIR in "${DB_DIR_PAIRS[*]}"; do
for value in ${value_size[@]}; do


for (( log_idx = 0; log_idx <${#model_log_paths[@]}; log_idx++ )); do

for ((i = 0; i < ${#DISKS[@]}; i++)); do

DB_DIR_P=( "${DISKS[$i]}" "${DIRS[$i]}"  )

for bench in ${bench_names[*]}; do

for (( model_idx = 0; model_idx < ${#EXEC_PATHS[@]}; model_idx++)); do
#        doOneBenchmarkTest      ${bench} ${value}   "nvlsm"
num_kv=`expr ${total_value_size} / \( 16 + ${value} \)`

echo doOneBenchmarkTest ${DB_DIR_P[0]} ${DB_DIR_P[1]} ${value} ${bench}  ${thread} ${EXEC_PATHS[$model_idx]} ${model_log_paths[$model_idx]} ${num_kv}
doOneBenchmarkTest ${DB_DIR_P[0]} ${DB_DIR_P[1]} ${value} ${bench}  ${thread} ${EXEC_PATHS[$model_idx]} ${model_log_paths[$log_idx]} ${num_kv}

done
done
done
done
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



