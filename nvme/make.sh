#!/bin/bash

make -C /lib/modules/$(uname -r)/build/ M=$PWD modules
