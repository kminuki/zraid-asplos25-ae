### FIO Write Performance (Overall)
Run the FIO benchmark, varying the number of open zones and block sizes.
- Open zones: [1, 4, 7, 10, 12]
- Block sizes: [4k, 8k, 16k, 32k, 64k, 128k, 256k]
- Total: 35 configurations
- ZNS RAID modules: [zraid, raizn-mf, raizn]
- Used devices: 5x ZN540

### To Reproduce
`sudo python3 run.py` → Runs the benchmark, creating the `./result` directory  
`python3 collect.py` → Collects results, prints summary, and generates `fig7.png` (Figure 7)

### Experiment Workflow
35 `fio` configurations are run on three different ZNS RAID modules: `zraid.ko`, `raizn-mf.ko`, and `raizn.ko`. If `reps` is greater than 1, repetitions are completed sequentially on a fixed module.

### Directory Structure

Scripts refer to a configuration file:
* `fig7.ini` — specifies `fio` benchmark configurations.

Generated files:
- `result/` — `fio` result files, e.g., `write_1thr_64qd_4kbs.res`
- `conf/` — `fio` configuration files, e.g., `write_1thr_64qd_4kbs.fio` (can be run manually: `sudo fio write_1thr_64qd_4kbs.fio`)
- `save/` — at the end of each module’s experiment, `run.py` automatically copies the `result/` directory here, e.g., `save/zraid`
- `bck/` — if `result/` is not empty at experiment start, its contents are moved here.

### Output Files
- `fig7.png`
- `fig7.pdf`
