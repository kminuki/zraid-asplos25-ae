# Usage: ./run_raizn.sh [dev_type] [mod_name]

if [ $# -eq 0 ]; then
    echo "Select device: wd or sam, select module name."
    exit 1 
fi

sudo umount /dev/mapper/raizn0
sudo umount /dev/nvme3n1p1
sudo dmsetup remove /dev/mapper/raizn0
sudo rmmod zraid
sudo rmmod raizn
sudo rmmod raizn_orig

cd ../nvme/
bash ./make.sh
bash ./insmod.sh
cd -

sleep 3

bash ./reset.sh $1
sleep 1


if [ "$2" == "zraid" ] || [ "$2" == "zs" ] || [ "$2" == "zsm" ]; then
    scheduler="none"
else
    scheduler="mq-deadline"
fi

if [ "$1" == "wd" ]; then
    model_name="WZS4C8T1TDSP303" #ZN540
    namespace_suffix="n2"

    device_list=$(lsblk -o NAME,MODEL -d -n | grep "$model_name" | awk '$1 ~ /nvme/ && $1 !~ /1$/ {print "/dev/" $1}' | sort)

    device_string=$(echo $device_list | tr '\n' ' ')

    for i in {0..6}; do
        echo $scheduler | sudo tee /sys/block/nvme${i}n2/queue/scheduler
    done
else
    echo $scheduler | sudo tee /sys/block/nvme1n1/queue/scheduler
fi

sudo insmod modules/$1/$2.ko
if [ "$1" == "wd" ]; then
    echo "0 15082717184 raizn 64 16 1 0 $device_string" | sudo dmsetup create raizn0
fi
if [ "$1" == "sam" ]; then
    echo "0 6056574976 raizn 64 16 1 0 /dev/mapper/linear_1 /dev/mapper/linear_2 /dev/mapper/linear_3 /dev/mapper/linear_4 /dev/mapper/linear_5" | sudo dmsetup create raizn0
fi
#sudo blkzone report /dev/mapper/raizn0
