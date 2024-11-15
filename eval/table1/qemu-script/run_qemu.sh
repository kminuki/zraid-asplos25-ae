#!/bin/bash

if [ $# -eq 0 ]; then
  echo "Usage: $0 wd or sam"
  exit 1
fi

if [ "$1" = "wd" ]; then
	echo "ZN540 mode"
	#vfio/vfio_zns.sh
fi

if [ "$1" = "sam" ]; then
	echo "Samsung mode"
	#vfio/vfio_samsung.sh
fi


cmd="sudo /home/csl/mwkim/qemu/build/qemu-system-x86_64 \
    -enable-kvm \
    -name zns \
    -m 100G \
    -machine accel=kvm \
    -cpu host \
    -smp 40 \
    -nographic \
    -serial file:../qemu.log \
    -netdev user,id=net0,hostfwd=tcp::2222-:22 \
    -device virtio-net-pci,netdev=net0 \
    -drive file=/mnt/sdb1/mwkim/qemu_img/jammy-server-cloudimg-amd64.img \
    -drive if=virtio,format=raw,file=seed.img \
    -virtfs local,path=/home/csl/mwkim/linux-5.15.0,mount_tag=host0,security_model=passthrough,id=host0 \
    -virtfs local,path=/home/csl/mwkim/release/zraid,mount_tag=host1,security_model=passthrough,id=host1 "

#: '
if [ "$1" = "wd" ]; then
	cmd+="-device vfio-pci,host=18:00.0 \
	-device vfio-pci,host=5e:00.0 \
	-device vfio-pci,host=86:00.0 \
	-device vfio-pci,host=af:00.0 \
	-device vfio-pci,host=d8:00.0 \
	-device vfio-pci,host=81:00.0 "
fi

if [ "$1" = "sam" ]; then
	cmd+="-device vfio-pci,host=3b:00.0 \
	-device vfio-pci,host=81:00.0 "
fi
#'

cmd+="-kernel /home/csl/mwkim/linux-5.15.0/arch/x86/boot/bzImage \
    -initrd /mnt/sdb1/mwkim/qemu_img/initrd.img-5.15.0-po2-nvm+ \
    -append 'root=/dev/sda1 console=ttyS0'"

#echo $cmd > t
eval "$cmd"

