### FIO Write Performance (Detailed)
Run the FIO benchmark, varying the number of open zones.
- Open zones: [1, 4, 7, 10, 12]
- Block sizes: [8k]
- Total: 5 configurations
- ZNS RAID modules: [zraid, raizn-mf, raizn, z, zs, zsm]
- Used devices: 5x ZN540

### To Reproduce
`sudo python3 run.py` → Runs the benchmark, creating the `./result` directory  
`python3 collect.py` → Collects results, prints summary, and generates `fig8.png` (Figure 8)

### Experiment Workflow
5 `fio` configurations are run on six different ZNS RAID modules: `zraid.ko`, `raizn-mf.ko`, `raizn.ko`, `z.ko`, `zs.ko`, and `zsm.ko`. If `reps` is greater than 1, repetitions are completed sequentially on a fixed module.

### Directory Structure

Scripts refer to a configuration file:
* `fig8.ini` — specifies `fio` benchmark configurations.

Generated files:
- `result/` — `fio` result files, e.g., `write_1thr_64qd_8kbs.res`
- `conf/` — `fio` configuration files, e.g., `write_1thr_64qd_8kbs.fio` (can be run manually: `sudo fio write_1thr_64qd_8kbs.fio`)
- `save/` — at the end of each module’s experiment, `run.py` automatically copies the `result/` directory here, e.g., `save/zraid`
- `bck/` — if `result/` is not empty at experiment start, its contents are moved here.

### Output Files
- `fig8.png`
- `fig8.pdf`
