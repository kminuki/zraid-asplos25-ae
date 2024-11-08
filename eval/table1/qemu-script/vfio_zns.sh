#!/bin/bash

modprobe vfio-pci

PCI_NVM="0000:18:00.0"
DEV_NVM="1b96 2600"

#Bind NVME drive to VFIO
echo "$DEV_NVM" > /sys/bus/pci/drivers/vfio-pci/new_id
echo "$PCI_NVM" > /sys/bus/pci/devices/$PCI_NVM/driver/unbind
echo "$PCI_NVM" > /sys/bus/pci/drivers/vfio-pci/bind
echo "$DEV_NVM" > /sys/bus/pci/drivers/vfio-pci/remove_id


PCI_NVM="0000:5e:00.0"
DEV_NVM="1b96 2600"

#Bind NVME drive to VFIO
echo "$DEV_NVM" > /sys/bus/pci/drivers/vfio-pci/new_id
echo "$PCI_NVM" > /sys/bus/pci/devices/$PCI_NVM/driver/unbind
echo "$PCI_NVM" > /sys/bus/pci/drivers/vfio-pci/bind
echo "$DEV_NVM" > /sys/bus/pci/drivers/vfio-pci/remove_id

PCI_NVM="0000:86:00.0"
DEV_NVM="1b96 2600"

#Bind NVME drive to VFIO
echo "$DEV_NVM" > /sys/bus/pci/drivers/vfio-pci/new_id
echo "$PCI_NVM" > /sys/bus/pci/devices/$PCI_NVM/driver/unbind
echo "$PCI_NVM" > /sys/bus/pci/drivers/vfio-pci/bind
echo "$DEV_NVM" > /sys/bus/pci/drivers/vfio-pci/remove_id

PCI_NVM="0000:af:00.0"
DEV_NVM="1b96 2600"

#Bind NVME drive to VFIO
echo "$DEV_NVM" > /sys/bus/pci/drivers/vfio-pci/new_id
echo "$PCI_NVM" > /sys/bus/pci/devices/$PCI_NVM/driver/unbind
echo "$PCI_NVM" > /sys/bus/pci/drivers/vfio-pci/bind
echo "$DEV_NVM" > /sys/bus/pci/drivers/vfio-pci/remove_id

PCI_NVM="0000:d8:00.0"
DEV_NVM="1b96 2600"

#Bind NVME drive to VFIO
echo "$DEV_NVM" > /sys/bus/pci/drivers/vfio-pci/new_id
echo "$PCI_NVM" > /sys/bus/pci/devices/$PCI_NVM/driver/unbind
echo "$PCI_NVM" > /sys/bus/pci/drivers/vfio-pci/bind
echo "$DEV_NVM" > /sys/bus/pci/drivers/vfio-pci/remove_id

#FADU
PCI_NVM="0000:81:00.0"
DEV_NVM="1dc5 4081"

#Bind NVME drive to VFIO
echo "$DEV_NVM" > /sys/bus/pci/drivers/vfio-pci/new_id
echo "$PCI_NVM" > /sys/bus/pci/devices/$PCI_NVM/driver/unbind
echo "$PCI_NVM" > /sys/bus/pci/drivers/vfio-pci/bind
echo "$DEV_NVM" > /sys/bus/pci/drivers/vfio-pci/remove_id
