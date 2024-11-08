obj-m += zraid.o
zraid-objs := main.o pp.o util.o zrwa.o
ccflags-y := -std=gnu11 -Wimplicit-fallthrough=0 -O3 -w $(CFLAGS)
KBUILD_EXTRA_SYMBOLS =/home/csl/mwkim/zrwa_raid/nvme/host-5.15.0/Module.symvers
all:
	make -C /lib/modules/$(shell uname -r)/build/ M=$(PWD) modules 

clean:
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) clean
