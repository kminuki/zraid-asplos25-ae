## ZRAID: Leveraging Zone Random Write Area (ZRWA) for Alleviating Partial Parity Tax in ZNS RAID Artifact 

This repository contains scripts for ASPLOS'25 artifact evaluation of the **ZRAID: Leveraging Zone Random Write Area (ZRWA) for Alleviating Partial Parity Tax in ZNS RAID** paper by Minwook Kim, Seongyeop Jeong, and Jin-Soo Kim.

### Evaluation instructions 

Since ZRAID requires specific hardware and software dependencies, we have pre-configured an environment for the evaluators on our machine. Therefore, evaluators do not need to install the kernel or additional applications (please skip steps 2, 4, 5, and 6 in Section A.4 of the artifact appendix.).

### Accessing the evaluation environment

Please contact the authors for information on how to access our machine remotely. We will provide the username and password upon request.

### Directory structure 

* `src/zraid` ZRAID sources.
* `src/raizn` RAIZN sources.

* `eval/fig7` Scripts to reproduce figure 7 in the paper.
* `eval/fig8` Scripts to reproduce figure 8 in the paper.
* `eval/fig9` Scripts to reproduce figure 9 in the paper.
* `eval/fig10` Scripts to reproduce figure 10 in the paper.
* `eval/fig11` Scripts to reproduce figure 11 in the paper.
* `eval/table1` Scripts to reproduce table 1 in the paper.
* `eval/claims` Scripts to validate claims in the paper.

* `nvme/` Modified NVMe driver to support ZRWA-related commands.
* `script/` Miscellaneous scripts for artifact evaluation.
* `patch/` Patches for the software environment (not required for evaluators using our machine).

### Instructions for evaluation testing 

Compile the modified NVMe driver:
```
cd nvme
bash make.sh
```

Compile the variations of ZRAID and RAIZN:
```
cd eval
python3 make_modules.py
```

This will generate four variations of ZRAID (zraid.ko, z.ko, zs.ko, zsm.ko) and two variations of RAIZN (raizn-mf.ko, raizn) in the `eval/modules/` directory. For details on the characteristics of each module, please refer to the paper. Additionally, zraid-stat.ko and raizn-stat.ko modules will be generated to record internal statistics of ZNS RAID (used for validating claims).

There are two directories for the different devices:
* `eval/modules/wd` for the ZN540 array
* `eval/modules/sam` for the PM1731a device

Other setups will be handled automatically by the scripts (`run.py`) in each `eval/fig` directory.

### Kernel boot parameters
To optimize performance, kernel boot parameters can be modified in `/etc/default/grub`. On our test machine, the `GRUB_CMDLINE_LINUX` parameters listed below are already set. You only need to comment/uncomment the relevant lines as required. Please avoid altering any other parameters, as improper modifications may cause the system to fail to boot.

For experiments on a native machine (Figures 7-11, claims):
`GRUB_CMDLINE_LINUX="quite splash intel_pstate=disable intel_iommu=off intel_idle.max_cstate=0" # ASPLOS submit`       

For experiments on QEMU (Table 1) with IOMMU enabled:
`GRUB_CMDLINE_LINUX="quite splash intel_pstate=disable intel_iommu=on iommu=pt processor.max_cstate=1 intel_idle.max_cstate=0 numa_balancing=disable irqbalance=0" #qemu`                                                

To apply these modifications, a reboot is required:
```
sudo update-grub
sudo reboot
```

### Running benchmarks 

Each `eval/figX` subdirectory contains a README with instructions for reproducing the corresponding figures from our paper in PDF and PNG formats.

To reduce reproduction time, we have set the experiments to run once (instead of 5 times). You can modify the `reps` parameter in the configuration file (figX.ini) of each directory if needed.

* **Figure 7**: ~50 minutes; 5 repetitions will require 4hours 10 minutes.
>
* **Figure 8**: ~18 minutes; 5 repetitions will require 1hour 30 minutes.
>
* **Figure 9**: ~33 minutes; 5 repetitions will require 2hours 45 minutes.
> 
* **Figure 10**: ~2hours 24 minutes; 5 repetitions will require 12hours.
> 
* **Figure 11**: ~7 minutes; 5 repetitions will require 35 minutes.
> 
* **Table1**: ~25hours 30minutes (for 100 repetition of 3 policies).
>
* **Claims validation**: ~50minutes.


### Precautions
- Recommended experiment sequence: 
  - Reboot is recommended before running **Figure 10** experiment. Unclean state can make performance anomaly in db_bench experiment.
  - It is advised to separate the **Table1** experiment from the others (the order does not matter). Modifying the kernel boot parameters is recommended when switching between experiments.

- However, if the **Table1** experiment is mixed with others, ensure that the **Figure 11** experiment is conducted before **Table1**. If you need to run **Table1** before **Figure 11**, please reboot the machine beforehand. This is because QEMU with VFIO may occasionally cause issues with PM1731a-related scripts.

- If you need to interrupt the **Table1** experiment midway, please run the `unvfio_zns.sh` script located in the `eval/table1/qemu-script` directory before proceeding with other experiments.
