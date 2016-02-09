#!/bin/bash

directory=test/systemtests/data/ndvi/
compression=RAW

source_name=ndvi
channel_file=1
channel_source=0


for file in $directory*.TIFF; do
	#extract date from filename
	time=$(basename $file | sed 's/^.*NDVI_//g' | sed 's/_rgb_3600x1800.TIFF//g')
	year=${time:0:4}
    month=${time:5:2}
    day=${time:8:2}
    time_start=$(date -u -d "$year-$month-$day" +%s)
    
    #calculate time_end from time_start + 1 month
    year2=$year
    month=$((10#$month)) #force base 10 because of leading zero
    month2=$(($month + 1))
    if [ $month2 -gt 12 ] ; then
        year2=$(( $year2 + 1 ))
        month2=1
    fi
    time_end=$(date -u -d "$year2-$month2-$day" +%s)

    duration=$(( $time_end - $time_start ))

	./mapping_manager import $source_name $file $channel_file $channel_source $time_start $duration $compression
done
