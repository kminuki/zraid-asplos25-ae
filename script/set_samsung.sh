#!/bin/bash
gap=1572864000

base_device="/dev/nvme1n1"

for i in {1..5}
do
    start_sector=$(( (i - 1) * gap ))

    echo "sudo dmsetup create linear_${i} --table "0 ${gap} linear ${base_device} ${start_sector}""
    sudo dmsetup create linear_${i} --table "0 ${gap} linear ${base_device} ${start_sector}"
done

sudo dmsetup ls

