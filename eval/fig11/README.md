### FIO Write Performance (Detailed)
Run the FIO benchmark, varying the number of open zones.
- Open zones: [15]
- Block sizes: [4k, 8k, 16k, 32k, 64k]
- Total: 5 configurations
- ZNS RAID modules: [zraid, raizn-mf]
- Used devices: 1x PM1731a

### To Reproduce
`sudo python3 run.py` → Runs the benchmark, creating the `./result` directory  
`python3 collect.py` → Collects results, prints summary, and generates `fig11.png` (Figure 11)

### Experiment Workflow
5 `fio` configurations are run on two different ZNS RAID modules: `zraid.ko`, and `raizn-mf.ko`. If `reps` is greater than 1, repetitions are completed sequentially on a fixed module.

At the start of the experiment, the PM1731a is partitioned into five partitions (`/dev/mapper/linear_[1-5]`), with each partition emulating a single ZNS SSD.

### Directory Structure

Scripts refer to a configuration file:
* `fig11.ini` — specifies `fio` benchmark configurations.

Generated files:
- `result/` — `fio` result files, e.g., `write_15thr_64qd_4kbs.res`
- `conf/` — `fio` configuration files, e.g., `write_15thr_64qd_4kbs.fio` (can be run manually: `sudo fio write_1thr_64qd_8kbs.fio`)
- `save/` — at the end of each module’s experiment, `run.py` automatically copies the `result/` directory here, e.g., `save/zraid`
- `bck/` — if `result/` is not empty at experiment start, its contents are moved here.

### Output Files
- `fig11.png`
- `fig11.pdf`
