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
class filebench_tester():
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

        self.f2fs_mk_path = local_cfg['f2fs_mk_path']
        self.f2fs_mnt_path = local_cfg['f2fs_mnt_path']
        self.wl_list = ast.literal_eval(local_cfg['wl_list'])
        self.fileserver_iosize_list = ast.literal_eval(local_cfg['fileserver_iosize_list'])
        self.wl_dir = local_cfg['wl_dir']

    def run_cmd(self, cmd):
        if self.debug:
            print(cmd)
        else:
            print(cmd)
            os.system(cmd)

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
        print(f'filebench results are moved to {os.path.join(self.bck_dir, str(new_idx))}')

    def setup_dirs(self):
        if not os.path.exists(self.res_dir):
            os.makedirs(self.res_dir)
        else:
            self.bck_res()
        if not os.path.exists(self.bck_dir):
            os.makedirs(self.bck_dir)

    def change_fileserver_iosize(self, iosize):
        file_path = os.path.join(self.wl_dir, 'fileserver.f')

        # 수정할 파일을 읽고 결과를 새로운 내용으로 저장
        new_content = []
        with open(file_path, 'r') as file:
            for line in file:
                # 특정 문자열이 포함되어 있는지 확인
                if 'set $iosize' in line:
                    # 문자열을 새로운 것으로 교체
                    line = f'set $iosize={iosize}k\n'
                new_content.append(line)

        # 수정된 내용을 파일에 다시 쓰기
        with open(file_path, 'w') as file:
            file.writelines(new_content)
        
    def run_filebench_workload(self, mod_name, wl_name, iosize=0):
        if wl_name == 'fileserver':
            res_file_name = f'{mod_name}_{wl_name}_{iosize}k.res'
        else:
            res_file_name = f'{mod_name}_{wl_name}.res'

        self.run_cmd(f'sudo filebench -f {os.path.join(self.wl_dir, wl_name+".f")} | tee -a {os.path.join(self.res_dir, res_file_name)}')

    def filebench_main(self, print_stat=False):
        dev_type = self.dev_type
        wl_list = self.wl_list

        start = time.time()
        for mod_name in self.targ_mod_list:
            self.zns_operator.init_zns_raid(mod_name)

            for wl_name in wl_list:
                if not f'{wl_name}.f' in os.listdir(self.wl_dir):
                    print('wl_name is incorrect, exit')
                for i in range(self.reps):
                    if wl_name == 'fileserver':
                        for iosize in self.fileserver_iosize_list:
                            self.zns_operator.set_raizn(self.dev_type)
                            self.zns_operator.init_f2fs(self.f2fs_mk_path, self.f2fs_mnt_path)
                            self.change_fileserver_iosize(iosize)
                            self.run_filebench_workload(mod_name, wl_name, iosize)
                            if print_stat:
                                os.system('sudo dmsetup message /dev/mapper/raizn0 0 dev_stat')
                            time.sleep(3)
                            self.run_cmd(f'sudo umount {self.f2fs_mnt_path}')
                            self.zns_operator.unset_raizn()
                    else:
                        self.zns_operator.set_raizn(self.dev_type)
                        self.zns_operator.init_f2fs(self.f2fs_mk_path, self.f2fs_mnt_path)
                        self.run_filebench_workload(mod_name, wl_name)
                        if print_stat:
                            os.system('sudo dmsetup message /dev/mapper/raizn0 0 dev_stat')
                        time.sleep(3)
                        self.run_cmd(f'sudo umount {self.f2fs_mnt_path}')
                        self.zns_operator.unset_raizn()
            
        end = time.time()
        elapsed_time = end - start
        self.zns_operator.print_elapsed_time(elapsed_time)


############################################################################
class filebench_res_collector():
    def __init__(self, **kwargs):
        self.local_cfg = kwargs['local_cfg']
        local_cfg = ConfigParser(interpolation=ExtendedInterpolation())
        local_cfg.read(self.local_cfg)

        self.base_dir = local_cfg['test']['base_dir']

        self.res_dir = local_cfg['collect']['res_dir']
        self.res_path = os.path.join(self.base_dir, self.res_dir)

        self.targ_mod_list = ast.literal_eval(local_cfg['test']['targ_mod_list'])
        self.wl_list = ast.literal_eval(local_cfg['test']['wl_list'])
        self.stat_list = ast.literal_eval(local_cfg['collect']['stat_list'])
        self.fileserver_iosize_list = ast.literal_eval(local_cfg['test']['fileserver_iosize_list'])


    def get_mod_from_filename(self, filename, wl_name):
        end_idx = filename.rfind(wl_name)
        end_idx = filename[:end_idx].rfind('_')
        return filename[:end_idx]

    def get_wl_files(self, res_dir, wl_name):
        res_files = [os.path.join(f) for f in os.listdir(res_dir) if (wl_name in f)]
        return res_files

    def extract_write_bw(self, line):
        for elem in line.split():
            if 'mb/s' in elem:
                return elem.replace('mb/s', '')

    def extract_iops(self, line):
        if 'ops/s' in line:
            return (line.split()[line.split().index('ops/s')-1])
            
    def extract_latency(self, line):
        if 'ms/op' in line:
            for elem in line.split():
                if 'ms/op' in elem:
                    return elem.replace('ms/op', '')

    def modify_wl_list(self): # for fileserver
        if 'fileserver' in self.wl_list:
            fs_index = self.wl_list.index('fileserver')
            self.wl_list = self.wl_list[:fs_index] + \
                [f'fileserver_{bs}k' for bs in self.fileserver_iosize_list] + \
                self.wl_list[fs_index + 1:]
        return self.wl_list 

    def collect_main(self):
        ret_df = {}
        self.modify_wl_list()
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
                    if module_name == self.get_mod_from_filename(res_file, wl_name):
                        break
                
                # print(module_name, res_file)
                with open(os.path.join(self.res_path, res_file)) as f:
                    for line in f.readlines():
                        if 'IO Summary' in line:
                            # print(line)
                            res_list[module_name]['IOPS'].append(self.extract_iops(line))
                            res_list[module_name]['latency'].append(self.extract_latency(line))
            # print(res_list)
                        
            temp_df = {}            
            for k, v in res_list.items():
                try:
                    print("{:<20} {:<20} {:<20}".format(
                        k, 
                        mean(float(n) for n in v[stat_list[0]] if n), 
                        mean(float(n) for n in v[stat_list[1]] if n)
                    ))
                except StatisticsError:
                    print("{:<20} {:<20} {:<20} {:<20}".format(
                        k, 
                        mean(float(n) for n in v[stat_list[0]] if n),
                        mean(float(n) for n in v[stat_list[1]] if n),
                        mean(float(n) for n in v[stat_list[2]] if n)
                    ))
                    continue
                temp_df[k] = mean(float(n) for n in v['IOPS'] if n)
            # print((temp_df))
            print()
            ret_df[wl_name] = temp_df

        # print((ret_df))
        return ret_df


############################################################################
class filebench_res_visualizer():
    def __init__(self, **kwargs):        
        self.local_cfg = kwargs['local_cfg']
        local_cfg = ConfigParser(interpolation=ExtendedInterpolation())
        local_cfg.read(self.local_cfg)

        self.base_dir = local_cfg['test']['base_dir']
        self.res_dir = local_cfg['collect']['res_dir']
        self.res_path = os.path.join(self.base_dir, self.res_dir)

        self.targ_mod_list = ast.literal_eval(local_cfg['test']['targ_mod_list'])
        self.stat_list = ast.literal_eval(local_cfg['collect']['stat_list'])
        fbrc = filebench_res_collector(**kwargs)
        self.wl_list = fbrc.modify_wl_list()
        self.xlabel = self.generate_xlabel()

        self.base_mod = local_cfg['visualize']['base_mod']

    def generate_xlabel(self):
        return [wl.replace('_', '\n') for wl in self.wl_list]

    def normalize_data(self, df):
        normalized_data = {
            workload: {module: value / values[self.base_mod] for module, value in values.items()}
            for workload, values in df.items()
        }
        return normalized_data

    def visualize_main(self, df):
        df = self.normalize_data(df)
      
        # Setting up plot parameters
        fig, ax = plt.subplots(figsize=(10, 5))#, sharey=True)
        # fig.set_size_inches(6, 4)
        # fig.tight_layout()

        # Data for plotting
        # interval = 1.5
        # x_pos = np.arange(0, len(self.wl_list)*interval, interval)
        x_pos = np.arange(0, len(self.wl_list))
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
        ax.bar(x_pos + pos_val*width, [df[wl_name]['zraid'] for wl_name in self.wl_list], width, label="ZRAID", edgecolor='black', linewidth=0.5, color='black')
        pos_val += 1.0

        # Setting labels and titles
        ax.set_xticks(x_pos)
        ax.set_xticklabels(self.xlabel, fontsize=16)
        # ax.set_xlabel('Workload', fontsize=16)
        ax.set_ylabel('Normalized IOPS', fontsize=16)
        # ax.set_ylim(0,32000)
        ax.grid(True, linestyle='dotted', linewidth=0.5)

        order = [0, 3, 1, 4, 2, 5]
        #plt.legend(loc='upper center', ncol=3, shadow=True, frameon=False)# bbox_to_anchor=(0.18, 1.1))
        # handles, labels = ax.get_legend_handles_labels()
        # fig.legend(handles, labels, loc='upper center', ncol= 8, fontsize=16, bbox_to_anchor=(0.5, 1))
        # fig.legend(handles, labels, loc='upper center', ncol=3, fontsize=11, bbox_to_anchor=(0.5, 0.95))
        handles, labels = ax.get_legend_handles_labels()
        # fig.legend([handles[idx] for idx in order], [labels[idx] for idx in order], loc='upper center', ncol=3, fontsize=20, bbox_to_anchor=(0.5, 1.15))
        fig.legend(handles, labels, loc='upper center', ncol=3, fontsize=20, bbox_to_anchor=(0.5, 1))

        # Saving the plot
        bw_plot_path = 'filebench-iops.pdf'
        fig.savefig(bw_plot_path, bbox_inches='tight')
        bw_plot_path = 'filebench-iops.png'
        fig.savefig(bw_plot_path, bbox_inches='tight')