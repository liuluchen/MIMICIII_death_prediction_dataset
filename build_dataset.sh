#!/bin/sh
mode=$1
host=162.105.146.246
user=mimic
passwd=mimic
schema=mimiciii

if [ -z "$mode" ]; then
    mode="all"
fi

if [ "$mode" = "dump" ] || [ "$mode" = "all" ]; then
    python data_process/extract.py --host $host --user $user --passwd "$passwd" --schema $schema
    python data_process/gather_static_data.py --mode dump --host $host --user $user --passwd "$passwd" --schema $schema
fi

if [ "$mode" = "select" ] || [ "$mode" = "all" ]; then
    python data_process/stat_data.py
    python data_process/stat_value.py
    python data_process/gather_stat.py
    python data_process/sort_stat.py
    python data_process/select_feature.py
fi

if [ "$mode" = "filter" ] || [ "$mode" = "all" ]; then
    python data_process/build_event.py
    python data_process/event_des.py
    python data_process/gather_static_data.py --mode admission --host $host --user $user --passwd "$passwd" --schema $schema
fi

if [ "$mode" = "build_death" ] || [ "$mode" = "all" ]; then
    # python data_process/patient.py
    # python data_process/stat_event_seq.py
    # python data_process/build_sample_setting.py
    # python data_process/build_death_sample.py
    python data_process/build_death_dataset.py
fi

