### Script Descriptions

- `setup-all.sh` — Clones the git repositories for `fio`, `filebench`, `rocksdb`, and `zenfs`, then git checkouts to the target versions.
- `apply-patch.sh` — Applies patches to the cloned repositories and builds them.
- `run_raizn.sh` — Configures the specified ZNS RAID module on the given device type. Example usage:  
  `bash ./run_raizn.sh wd zraid`
- `set_samsung.sh` — Make partitions on PM1731a.
