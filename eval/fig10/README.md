### db_bench Performance
Run the db_bench benchmark, over its various pre-defined workloads.
- Workloads: [fillseq, fillrandom, overwrite]
- ZNS RAID modules: [zraid, raizn-mf, raizn, z, zs, zsm]
- Used devices: 5x ZN540, 1x FADU Delta (Conventional SSD for `aux` directory)

### To Reproduce
`sudo python3 run.py` → Runs the benchmark, creating the `./result` directory  
`python3 collect.py` → Collects results, prints summary, and generates `fig10.png` (Figure 10)

### Experiment Workflow
Three `db_bench` workloads, `fillseq`, `fillrandom`, and `overwrite`, are run on six different ZNS RAID modules: `zraid.ko`, `raizn-mf.ko`, `raizn.ko`, `z.ko`, `zs.ko`, and `zsm.ko`. The RAID array is reset between the `fillseq` and `fillrandom` workloads, while `overwrite` runs on the existing database. If `reps` is greater than 1, all modules are evaluated in rotation.

### Directory Structure
Scripts refer to a configuration file:
* `fig11.ini` — specifies experiment configurations.

Generated files:
- `result/` — `db_bench` result output files, e.g., `zraid_fillseq.res`. If `reps` is greater than 1, output is appended to each correspoding file.
- `bck/` — if `result/` is not empty at experiment start, its contents are moved here.

### Output Files
- `fig10.png`
- `fig10.pdf`
