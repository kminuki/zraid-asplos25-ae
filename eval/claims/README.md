### Claims in the Paper
- Claim 1: "..RAIZN reaches a WAF of 2.44 ... when running the varmail workload on F2FS.." (Section 3.2)
- Claim 2: "..RAIZN+ has an average WAF of 1.6 ... reaching up to 2 in fillseq ... ZRAID produces only 26 MB of permanent PP blocks" (Section 6.4)

### To Reproduce
`sudo python3 claim1.py` → Runs the benchmark, outputs statistics to syslog.  
`sudo python3 claim2.py` → Runs the benchmark, outputs statistics to syslog.

### Printed Statistics
* `total_write_count` — Number of logical writes.
* `total_write_amount` — Total amount of logical writes (in KB).
* `pp_volatile` — Total partial parity blocks written to ZRWA (overwritten by data).
* `pp_permanent` — Total partial parity blocks written to the main flash area.
* `gc_count` — Number of garbage collection cycles on dedicated partial parity zones.
* `gc_migrated` — Total blocks migrated during garbage collection.

### Flash WAF Calculation
Claim 1: A simple comparison between data blocks and PP-related blocks.
PP-induced WAF = (`pp_permanent` + `total_write_count`) / `total_write_count`

Claim 2: This claim addresses the overall array WAF for data permanently written to flash.  
The printed `total_write_amount` reflects pure data writes. Write amplification in ZNS RAID arises from two types of parity: full parity and partial parity. The full parity overhead is always `(1 / stripe_width)` of total data, which is 25% in a five-ZN540 setup. The Flash WAF can be calculated as follows (excluding `pp_volatile`, which is not written to the main flash area):

Flash WAF = (`pp_permanent` + `total_write_count` * 1.25) / `total_write_count`

### Experiment Workflow
- `claim1.py` runs the `varmail` workload on the `raizn-stat.ko` module, logging statistics to the syslog, viewable via the `dmesg` command.
- `claim2.py` executes three `db_bench` workloads (as described in Figure 10) on both `zraid-stat.ko` and `raizn-stat.ko` modules. After each workload completes, it logs statistics to syslog and saves a copy in the `result/` directory.

### Directory Structure
Generated files:
- `result/` — Copies of `dmesg` output at the end of each workload run, e.g., `zraid_fillseq.dmesg`
- `bck/` — If `result/` is not empty at the experiment's start, its contents are moved here.
