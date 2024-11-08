### Filebench Performance
Run the Filebench benchmark, over its various pre-defined workloads.
- Workloads: [fileserver, oltp, varmail]
- Block sizes (only for fileserver): [4k, 16k, 64k, 1024k]
- ZNS RAID modules: [zraid, raizn-mf, raizn]
- Used devices: 5x ZN540, 1x FADU Delta (Conventional SSD for `f2fs` metadata area)

### To Reproduce
`sudo python3 run.py` → Runs the benchmark, creating the `./result` directory  
`python3 collect.py` → Collects results, prints summary, and generates `fig9.png` (Figure 9)

### Experiment Workflow
The `fileserver` workload with four different block sizes, as well as the `oltp` and `varmail` workloads, are run on three different ZNS RAID modules: `zraid.ko`, `raizn-mf.ko`, and `raizn.ko`. If `reps` is greater than 1, repetitions are completed sequentially on each fixed module.

### Directory Structure

Scripts refer to configuration files:
* `fig9.ini` — specifies experiment configurations.
* `workload/` — workload configurations, e.g., `fileserver.f`

Generated files:
- `result/` — `filebench` result output files, e.g., `zraid_fileserver_4k.res`. If `reps` is greater than 1, output is appended to each correspoding file.
- `bck/` — if `result/` is not empty at experiment start, its contents are moved here.

### Output Files
- `fig9.png`
- `fig9.pdf`
