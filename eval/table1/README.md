### Fault injection test
Configure ZRAID and run a synthetic workload inside the QEMU, then forcibly exit QEMU at random time. Reconfigure ZRAID and check its consistency.
- Consistency policies of ZRAID: [Stripe-based, Chunk-based, WP log]

### To Reproduce
`python3 crash-test.py [policy_name]` → Automatically runs the whole work-crash-recovery process with 100 repetitions, outputs fail rate and data loss to syslog. 
Name of consistency policies is simplified: [stripe, chunk, log]
Example command: `python3 crash-test.py stripe`

### Printed Statistics
Output of each run looks like:
* `Workload runtime` — Duration from the workload started before the crash.
* `Commit lba` — The last printed LBA int the redirected log after the crash (in KB).
* `Reported lba` — A LBA reported via "blkzone report" command in QEMU after the recovery.
* `LBA [Success | Error]` — If `Commit lba` is bigger than `Reported lba`, the run is treated as Error.
* `Verify [Success | Error]` — Verifying the pre-defined pattern which is related to LBA. If the pattern integrity is corrupted, the script stops the repetition.

Output after completing 100 repetitions:
* `success: [n], fail: [m]` — Summary of the total repetitions. Occasionally, n+m may be less than 100 because, if the `Commit lba` is 0 (indicating the crash occurred too early), that case is considered invalid and not counted.
* `Data loss total: [x] (KB), avg: [y] (KB)` — x is accumulated difference between `Commit lba` and `Reported lba`, if the recovery fails. y is the average difference per a failure, y = x/m.

### Experiment Workflow
- Each consistency policies should be evaluated separately. When the policy is given, the corresponding pre-compiled module is used to configure ZRAID inside the QEMU.
- 
- [Note] The script has a minor bug that the terminal 가 먹통이 되는 after the completion. Result is still visible, but you need to restart the terminal to execute next evaluation.

### Directory Structure

Generated files:
- `qemu.log` — Redirected syslog from QEMU.
- `bck-qemu.log` — `qemu.log` is automatically renamed to this file after the workload running phase. Last committed LBA can manually confirmed through this file.


### Accessing QEMU
QEMU can be manually started by commands:
```
cd qemu-script
sudo vfio_zns.sh
./run_qemu.sh wd
```

QEMU can be accessed by commands:
```
cd qemu-script
./ssh.sh
```
Then you can navigate scripts inside the QEMU environment.

### Scripts in QEMU
- `~/all.sh` — Setup and configure ZRAID with the given consistency policy.
- `~/work.sh` — Run synthetic workload consisted of FUA writes.
- `~/recover.sh` — Setup and configure ZRAID to perform recovery.
- `~/verify.sh` — Read the array and verify the content until the given LBA (`Reported lba`).
- `~/crash_test/crash_wl.ko` — A custom module that performs synthetic workload at insert time. Source code can be referred at `~/crash_test/crash_wl.c`.
