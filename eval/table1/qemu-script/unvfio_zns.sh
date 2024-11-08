#!/bin/bash

PCI_NVM="0000:18:00.0"
DEV_NVM="1b96 2600"


#Unbind NVME drive to VFIO
echo 1 > /sys/bus/pci/devices/"$PCI_NVM"/remove
echo 1 > /sys/bus/pci/rescan


PCI_NVM="0000:5e:00.0"
DEV_NVM="1b96 2600"

#Bind NVME drive to VFIO
echo 1 > /sys/bus/pci/devices/"$PCI_NVM"/remove
echo 1 > /sys/bus/pci/rescan

PCI_NVM="0000:86:00.0"
DEV_NVM="1b96 2600"

#Bind NVME drive to VFIO
echo 1 > /sys/bus/pci/devices/"$PCI_NVM"/remove
echo 1 > /sys/bus/pci/rescan

PCI_NVM="0000:af:00.0"
DEV_NVM="1b96 2600"

#Bind NVME drive to VFIO
echo 1 > /sys/bus/pci/devices/"$PCI_NVM"/remove
echo 1 > /sys/bus/pci/rescan

PCI_NVM="0000:d8:00.0"
DEV_NVM="1b96 2600"

#Bind NVME drive to VFIO
echo 1 > /sys/bus/pci/devices/"$PCI_NVM"/remove
echo 1 > /sys/bus/pci/rescan


#FADU                                                          
PCI_NVM="0000:81:00.0"                                         
DEV_NVM="1dc5 4081"                                            
                                                               
#Bind NVME drive to VFIO                                       
echo 1 > /sys/bus/pci/devices/"$PCI_NVM"/remove
echo 1 > /sys/bus/pci/rescan
