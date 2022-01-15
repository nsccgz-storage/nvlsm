log_dir="/root/bily/leveldb_log/2022-01-11-11-34-56"
#data_file=sprintf("%s/%s", log_dir, "all_data.csv")
data_file=sprintf("%s/%s", log_dir, "all_data_sort.csv")


set terminal svg size 800, 500 enhanced background rgb 'white'
set output 'gnuplot/readrandom-latency-HDD-fillrandom.svg'


set xlabel "Value size (B)"
set ylabel "Latency (ms/op)"

# set xrange [ * : * ] noreverse writeback
# set yrange [ * : * ] noreverse writeback

#009E73
#E69F00
#56B4E9

#9400D3
#009E73
#E69F00

set style line 1 \
    linetype 1 linewidth 1 \
    pointtype 4 pointsize 0.8 \
    linecolor  rgb "#9400D3" # "#3A5FCD"

set style line 2 \
    linetype 3 linewidth 1 \
    pointtype 7 pointsize 0.8 \
    linecolor rgb "#009E73" #"#3CB371"

set style line 3 \
    linetype 2  linewidth 1 \
    pointtype 2 pointsize 0.8 \
    linecolor rgb "#E69F00" #"#FF6347"


# set style fill solid 1.00 border lt -1
# set style histogram clustered 
# set style data histogram


set style fill solid  1.00 border lt  -1
set style histogram clustered
set style data histogram
set key on top left  vertical maxrows 5 box  noenhanced


#disk_types="HDD NVMeSSD"
models="leveldb  leveldb_nvm nvlsm"

set datafile separator ','
set grid
# set multiplot layout 2,1


set title "(HDD readrandom latency after fillrandom  )"
plot \
    for [i=1:words(models)] \
    "< grep -e 'model' -e '".word(models,i).",HDD,fillrandom,1'  ".data_file \
    using (column("read_random_latency_ms/op")):xtic(5) \
    title word(models,i) \
    ls i
    ;


