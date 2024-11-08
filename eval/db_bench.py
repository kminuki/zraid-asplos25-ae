import os, sys, time, re, copy, shutil, ast, math
import zns_raid
from configparser import ConfigParser, ExtendedInterpolation
from pandas import *
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statistics import mean
from statistics import StatisticsError

############################################################################
class db_bench_tester():
    def __init__(self, **kwargs):
        self.start_rep_idx = 1
        self.sleeptime = 5
        self.multi_arg_idx = -1 # used in function multi_args
        self.store_fio_args = [] # stack

        self.global_cfg = kwargs['global_cfg']
        global_cfg = ConfigParser(interpolation=ExtendedInterpolation())
        global_cfg.read(self.global_cfg)
        self.fio_cmd = global_cfg['fio']['fio_cmd']

        self.local_cfg = kwargs['local_cfg']
        local_cfg = ConfigParser(interpolation=ExtendedInterpolation())
        local_cfg.read(self.local_cfg)
        local_cfg = local_cfg['test']

        self.device = local_cfg['dev_name']
        self.dev_type = local_cfg['dev_type']
        self.reps = int(local_cfg['reps'])
        self.debug = (local_cfg['debug'] == 'True')
        if self.debug:
            print('DEBUG mode on!!')
        self.targ_mod_list = ast.literal_eval(local_cfg['targ_mod_list'])
        self.zns_operator = zns_raid.zns_operator(
            global_cfg=kwargs['global_cfg'], dev_type=self.dev_type, debug=self.debug)

        self.base_dir = local_cfg['base_dir']
        self.res_dir = os.path.join(self.base_dir, 'result')
        self.bck_dir = os.path.join(self.base_dir, 'bck')
        self.setup_dirs()

        self.dm_name = local_cfg['dm_name']
        self.zenfs_mk_path = local_cfg['zenfs_mk_path']
        self.zenfs_aux_path = local_cfg['zenfs_aux_path']
        self.db_bench_path = local_cfg['db_bench_path']
        self.db_bench_param_str = local_cfg['db_bench_param_str']
        # print(self.db_bench_param_str)

    def run_cmd(self, cmd):
        if self.debug:
            print(cmd)
        else:
            print(cmd)
            os.system(cmd)

    def bck_dmesg(self, filename):
        cmd = f'dmesg > {filename}'
        self.run_cmd(cmd)

    def bck_res(self):
        if len(os.listdir(self.res_dir)) == 0:
            print('res_dir is empty. No need to backup')
            return

        idx_list = []
        for f in os.listdir(self.bck_dir):
            if f.isdigit():
                idx_list.append(int(f))
        if idx_list == []:
            new_idx = 1
        else:
            idx_list.sort()
            new_idx = idx_list[-1] + 1
        shutil.move(self.res_dir, os.path.join(self.bck_dir, str(new_idx)))
        os.makedirs(self.res_dir)
        print(f'db_bench results are moved to {os.path.join(self.bck_dir, str(new_idx))}')

    def setup_dirs(self):
        if not os.path.exists(self.res_dir):
            os.makedirs(self.res_dir)
        else:
            self.bck_res()
            
        if not os.path.exists(self.bck_dir):
            os.makedirs(self.bck_dir)

    def perf_tune(self):
        self.run_cmd('sudo /etc/init.d/irqbalance stop')
        self.run_cmd('sudo tuned-adm profile throughput-performance')
        
    def run_db_bench_workload(self, wl_name, res_file_name, use_existing_db):
        self.run_cmd(f'sudo {self.db_bench_path} \
            --fs_uri=zenfs://dev:{self.dm_name} \
            {self.db_bench_param_str} \
            --benchmarks={wl_name} \
            --use_existing_db={use_existing_db} \
            2>&1 | tee -a {res_file_name}')

    def run_db_bench(self, mod_name, bck_dmesg):
        # perf_tune()
        self.run_cmd('echo 3 > /proc/sys/vm/drop_caches')
        self.run_cmd('sync')
        time.sleep(3)
        wl_name = 'fillseq'
        res_file_name = os.path.join(self.res_dir, f'{mod_name}_{wl_name}.res')
        before = self.zns_operator.get_raid_write_stat()
        self.run_db_bench_workload(wl_name, res_file_name, 0)
        after = self.zns_operator.get_raid_write_stat()
        # write_amount_file = res_file_name.replace('.res', '.write')
        # with open(write_amount_file, 'a') as file:
        #     file.write(f'Total writes(kB): {after - before}\n')
        if bck_dmesg:
            os.system('sudo dmsetup message /dev/mapper/raizn0 0 dev_stat')
            dmesg_file = res_file_name.replace('.res', '.dmesg')
            self.bck_dmesg(dmesg_file)
            os.system('sudo dmsetup message /dev/mapper/raizn0 0 dev_stat_reset')

        # reset array
        self.zns_operator.init_zns_raid(mod_name)
        self.zns_operator.set_raizn(self.dev_type)
        self.zns_operator.init_zenfs(self.dm_name, self.zenfs_mk_path, self.zenfs_aux_path)

        # perf_tune()
        self.run_cmd('echo 3 > /proc/sys/vm/drop_caches')
        self.run_cmd('sync')
        time.sleep(3)
        wl_name = 'fillrandom'
        res_file_name = os.path.join(self.res_dir, f'{mod_name}_{wl_name}.res')
        before = self.zns_operator.get_raid_write_stat()
        self.run_db_bench_workload(wl_name, res_file_name, 0)
        after = self.zns_operator.get_raid_write_stat()
        # write_amount_file = res_file_name.replace('.res', '.write')
        # with open(write_amount_file, 'a') as file:
        #     file.write(f'Total writes(kB): {after - before}\n')
        if bck_dmesg:
            os.system('sudo dmsetup message /dev/mapper/raizn0 0 dev_stat')
            dmesg_file = res_file_name.replace('.res', '.dmesg')
            self.bck_dmesg(dmesg_file)
            os.system('sudo dmsetup message /dev/mapper/raizn0 0 dev_stat_reset')

        # perf_tune()
        self.run_cmd('echo 3 > /proc/sys/vm/drop_caches')
        self.run_cmd('sync')
        time.sleep(3)
        wl_name = 'overwrite'
        res_file_name = os.path.join(self.res_dir, f'{mod_name}_{wl_name}.res')
        before = self.zns_operator.get_raid_write_stat()
        self.run_db_bench_workload(wl_name, res_file_name, 1)
        after = self.zns_operator.get_raid_write_stat()
        # write_amount_file = res_file_name.replace('.res', '.write')
        # with open(write_amount_file, 'a') as file:
        #     file.write(f'Total writes(kB): {after - before}\n')
        if bck_dmesg:
            os.system('sudo dmsetup message /dev/mapper/raizn0 0 dev_stat')
            dmesg_file = res_file_name.replace('.res', '.dmesg')
            self.bck_dmesg(dmesg_file)
            os.system('sudo dmsetup message /dev/mapper/raizn0 0 dev_stat_reset')

    def db_bench_main(self, bck_dmesg=False):
        start = time.time()
        for i in range(self.reps):
            for mod_name in self.targ_mod_list:
                self.zns_operator.init_zns_raid(mod_name)
                self.zns_operator.set_raizn(self.dev_type)
                self.zns_operator.init_zenfs(self.dm_name, self.zenfs_mk_path, self.zenfs_aux_path)
                self.run_db_bench(mod_name, bck_dmesg)

        end = time.time()
        elapsed_time = end - start
        self.zns_operator.print_elapsed_time(elapsed_time)

############################################################################
class db_bench_res_collector():
    def __init__(self, **kwargs):
        self.local_cfg = kwargs['local_cfg']
        local_cfg = ConfigParser(interpolation=ExtendedInterpolation())
        local_cfg.read(self.local_cfg)

        self.base_dir = local_cfg['test']['base_dir']

        self.res_dir = local_cfg['collect']['res_dir']
        self.res_path = os.path.join(self.base_dir, self.res_dir)

        self.targ_mod_list = ast.literal_eval(local_cfg['test']['targ_mod_list'])
        self.wl_list = ast.literal_eval(local_cfg['collect']['wl_list'])
        self.stat_list = ast.literal_eval(local_cfg['collect']['stat_list'])


    def get_mod_from_filename(self, filename):
        end_idx = filename.rfind('_')
        return filename[:end_idx]

    def get_wl_files(self, res_dir, wl_name):
        res_files = [os.path.join(f) for f in os.listdir(res_dir) if (wl_name in f)]
        return res_files

    def extract_write_bw(self, line):
        if 'MB/s' in line:
            return (line.split()[line.split().index('MB/s')-1])

    def extract_iops(self, line):
        if 'ops/sec' in line:
            return (line.split()[line.split().index('ops/sec')-1])
            
    def extract_latency(self, line):
        if 'micros/op' in line:
            return (line.split()[line.split().index('micros/op')-1])

    def collect_main(self):
        ret_df = {}
        stat_list = self.stat_list
        for wl_name in self.wl_list:
            print(wl_name)
            res_files = self.get_wl_files(self.res_path, wl_name)

            res_list = {}

            for module_name in self.targ_mod_list:
                res_list[module_name] = {}        
                for stat_name in stat_list:
                    res_list[module_name][stat_name] = []

            for res_file in res_files:
                for module_name in self.targ_mod_list:
                    if module_name == self.get_mod_from_filename(res_file):
                        break
                
                # print(module_name, res_file)
                with open(os.path.join(self.res_path, res_file)) as f:
                    write_bw_first = True
                    for line in f.readlines():
                        if f'{wl_name}' in line:
                            # print(line)
                            res_list[module_name]['IOPS'].append(self.extract_iops(line))
                            res_list[module_name]['latency'].append(self.extract_latency(line))
                            res_list[module_name]['WR_BW'].append(self.extract_write_bw(line))
                            # write_bw_first = True
                        
                        if write_bw_first:
                            if 'wrtfile' in line:
                            # if 'appendfile' in line:
                                res_list[module_name]['WR_BW'].append(self.extract_write_bw(line))
                                write_bw_first = False
                        
            temp_df = {}            
            for k, v in res_list.items():
                try:
                    print("{:<20} {:<20} {:<20} {:<20}".format(
                        k, 
                        mean(float(n) for n in v[stat_list[0]] if n), 
                        mean(float(n) for n in v[stat_list[1]] if n),
                        mean(float(n) for n in v[stat_list[2]] if n)
                    ))
                except StatisticsError:
                    continue
                    print("{:<20} {:<20} {:<20}".format(
                        k, 
                        mean(float(n) for n in v[stat_list[0]] if n),
                        mean(float(n) for n in v[stat_list[1]] if n)
                    ))
                temp_df[k] = mean(float(n) for n in v['IOPS'] if n)
            # print((temp_df))
            print()
            ret_df[wl_name] = temp_df

        # print((ret_df))
        return ret_df


############################################################################
class db_bench_res_visualizer():
    def __init__(self, **kwargs):        
        self.local_cfg = kwargs['local_cfg']
        local_cfg = ConfigParser(interpolation=ExtendedInterpolation())
        local_cfg.read(self.local_cfg)

        self.base_dir = local_cfg['test']['base_dir']
        self.res_dir = local_cfg['collect']['res_dir']
        self.res_path = os.path.join(self.base_dir, self.res_dir)

        self.targ_mod_list = ast.literal_eval(local_cfg['test']['targ_mod_list'])
        self.wl_list = ast.literal_eval(local_cfg['collect']['wl_list'])
        self.stat_list = ast.literal_eval(local_cfg['collect']['stat_list'])

    def visualize_main(self, df):
      
        # Setting up plot parameters
        fig, ax = plt.subplots(figsize=(10, 5))#, sharey=True)
        # fig.set_size_inches(6, 4)
        # fig.tight_layout()

        # Data for plotting
        interval = 1.5
        x_pos = np.arange(0, len(self.wl_list)*interval, interval)
        width = 0.2
        interval = 0


        # print ([df[wl_name]['zs']['IOPS'] for wl_name in self.wl_list])
        # print (type([df[wl_name]['zs']['IOPS'][0]  for wl_name in self.wl_list]))
        # exit()
        # 막대 그래프 생성
        pos_val = 0.5 - len(self.targ_mod_list)/2
        ax.bar(x_pos + pos_val*width, [df[wl_name]['raizn'] for wl_name in self.wl_list], width, label="RAIZN", edgecolor='black', linewidth=0.5,  color='white')
        pos_val += 1.0
        ax.bar(x_pos + pos_val*width, [df[wl_name]['raizn-mf'] for wl_name in self.wl_list], width, label="RAIZN+", edgecolor='black', linewidth=0.5, color='#808080')
        pos_val += 1.0
        ax.bar(x_pos + pos_val*width, [df[wl_name]['z'] for wl_name in self.wl_list], width, label="Z (ZRWA)", edgecolor='black', linewidth=0.5, color='white', hatch='\\')
        pos_val += 1.0
        ax.bar(x_pos + pos_val*width, [df[wl_name]['zs'] for wl_name in self.wl_list], width, label="Z+S", edgecolor='black', linewidth=0.5, color='white', hatch='xx')
        pos_val += 1.0
        ax.bar(x_pos + pos_val*width, [df[wl_name]['zsm'] for wl_name in self.wl_list], width, label="Z+S+M", edgecolor='black', linewidth=0.5, color='white', hatch='....')
        pos_val += 1.0
        ax.bar(x_pos + pos_val*width, [df[wl_name]['zraid'] for wl_name in self.wl_list], width, label="ZRAID", edgecolor='black', linewidth=0.5, color='black')
        pos_val += 1.0

        # Setting labels and titles
        ax.set_xticks(x_pos)
        ax.set_xticklabels(self.wl_list, fontsize=16)
        # ax.set_xlabel('[ZenFS] 1mkey, 1mop, kv8KB, worker-thr1, comp/flush-thr16')
        # ax.set_xlabel('Workload', fontsize=16)
        # ax.set_ylabel('Normalized QPS')
        ax.set_ylabel('QPS', fontsize=16)
        # ax.set_ylim(0,32000)
        ax.grid(True, linestyle='dotted', linewidth=0.5)

        order = [0, 3, 1, 4, 2, 5]
        #plt.legend(loc='upper center', ncol=3, shadow=True, frameon=False)# bbox_to_anchor=(0.18, 1.1))
        # handles, labels = ax.get_legend_handles_labels()
        # fig.legend(handles, labels, loc='upper center', ncol= 8, fontsize=16, bbox_to_anchor=(0.5, 1))
        # fig.legend(handles, labels, loc='upper center', ncol=3, fontsize=11, bbox_to_anchor=(0.5, 0.95))
        handles, labels = ax.get_legend_handles_labels()
        fig.legend([handles[idx] for idx in order], [labels[idx] for idx in order], loc='upper center', ncol=3, fontsize=20, bbox_to_anchor=(0.5, 1.15))
        # fig.legend(handles, labels, loc='upper center', ncol=3, fontsize=20, bbox_to_anchor=(0.5, 1.15))

        # Saving the plot
        bw_plot_path = 'db_bench-zenfs.pdf'
        fig.savefig(bw_plot_path, bbox_inches='tight')
        bw_plot_path = 'db_bench-zenfs.png'
        fig.savefig(bw_plot_path, bbox_inches='tight')