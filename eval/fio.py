import os, sys, time, re, copy, shutil, ast, math
import zns_raid
from configparser import ConfigParser, ExtendedInterpolation
from pandas import *
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

############################################################################
class fio_tester():
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
        self.save_dir = os.path.join(self.base_dir, 'save')
        self.conf_dir = os.path.join(self.base_dir, 'conf')
        self.bck_dir = os.path.join(self.base_dir, 'bck')
        self.setup_dirs()

        self.fio_init_args = local_cfg['fio_init_args']
        self.fio_init_args = ast.literal_eval(self.fio_init_args.replace("\n", " "))
        self.fio_inc_args = local_cfg['fio_inc_args']
        self.fio_inc_args = ast.literal_eval(self.fio_inc_args.replace("\n", " "))


    def fio_file_path(self, filetype, filename, rep_idx=None):
        dir_path = getattr(self, f'{filetype}_dir')
        if rep_idx:
            return os.path.join(dir_path, str(rep_idx), filename)
        else:
            return os.path.join(dir_path, filename)

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
        print(f'fio results are moved to {os.path.join(self.bck_dir, str(new_idx))}')
        
    def bck_save(self):
        if len(os.listdir(self.save_dir)) == 0:
            print('save_dir is empty. No need to backup')
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
        shutil.move(self.save_dir, os.path.join(self.bck_dir, 'save_' + str(new_idx)))
        os.makedirs(self.save_dir)
        print(f'fio results are moved to {os.path.join(self.bck_dir, str(new_idx))}')
  
    def setup_dirs(self):
        if not os.path.exists(self.res_dir):
            os.makedirs(self.res_dir)
        else:
            self.bck_res()
        for rep_idx in range(1, self.start_rep_idx+self.reps):
            print(os.path.join(self.res_dir, str(rep_idx)))
            if not os.path.exists(os.path.join(self.res_dir, str(rep_idx))):
                os.makedirs(os.path.join(self.res_dir, str(rep_idx)))

        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)
        else:
            self.bck_save()
        if not os.path.exists(self.bck_dir):
            os.makedirs(self.bck_dir)
        if not os.path.exists(self.conf_dir):
            os.makedirs(self.conf_dir)

    def run_fio(self, conf_file, res_file, rep_idx): 
        command = f'sudo {self.fio_cmd} {self.fio_file_path("conf", conf_file)} --output={self.fio_file_path("res", res_file, rep_idx)}'
        before = self.zns_operator.get_raid_write_stat()
        if self.debug:
            print(command)
        else:
            print(command)
            os.system(command)
        after = self.zns_operator.get_raid_write_stat()
        # write_amount_file = res_file.replace('.res', '.write')
        # with open(self.fio_file_path("res", write_amount_file, rep_idx), 'a') as file:
        #     file.write(f'Total writes(kB): {after - before}')
        print('\n\n')
        print(f'Total writes(kB): {after - before}')
        print('\n\n')

    def fio_param_validate(self, fio_args):
        if 'z' in fio_args['size']:
            if 'zonemode' in fio_args: # zonemode must be 'zbd'
                if not (fio_args['zonemode'] == 'zbd'):
                    return False
            else:
                return False
        return True        

    def validate_runtime(self, fio_args):
        if ('runtime' in fio_args):
            if (fio_args['runtime'].isdigit()):
                return True
        return False

    def setup_common_parameter(self, fio_args):
        if not self.fio_param_validate(fio_args):
            print('fio parameter is invalid!! Check once again')
            return False, False

        file_name = f'{fio_args["rw"]}' \
        f'_{fio_args["numjobs"]}thr' \
        f'_{fio_args["iodepth"]}qd' \
        f'_{fio_args["bs"]}bs'
        conf_file = file_name + '.fio'
        output_file = file_name + '.res'
        
        f = open(self.fio_file_path('conf', conf_file), 'w')

        f.write("[global]\n")
        f.write("filename=" + self.device + '\n')
        for key in fio_args.keys():
            if key == 'job_args': # job specific args should be appended later
                continue
            f.write(f'{key}={str(fio_args[key])}\n')

        #if self.validate_runtime(fio_args):
        #    f.write('time_based=1\n') # If set, fio will run for the duration of the runtime specified even if the file(s) are completely read or written. It will simply loop over the same workload as many times as the runtime allows.

        # if (fio_args['iodepth'] == '1'):
        #     f.write('ioengine=psync\n')
        # else:
        #     f.write('ioengine=libaio\n')
        
        
        f.write(f'[file0]\n')

        # if do below, # thr = numjobs ** 2
        # for n in range(int(fio_args['numjobs'])):
        #     f.write(f'[file{str(n)}]\n')
        #     job_args = fio_args['job_args']
        #     for key in job_args.keys():
        #         f.write(f'{key}={str(job_args[key])}\n')
                
        return conf_file, output_file
       
    def test_once(self, fio_args, dummy=False, rep_idx=1):
        if fio_args['rw'] == 'write':
            if 'raizn' in self.device:
                self.zns_operator.reload_raizn(self.device)
            else:
                self.zns_operator.reset_all_zone(self.device)
        if not self.debug:
            time.sleep(5)

        os.system('echo 3 > /proc/sys/vm/drop_caches')
        os.system('sync')
        time.sleep(3)

        conf_file, output_file = self.setup_common_parameter(fio_args)
        if not conf_file:
            return False
        else:
            if dummy:
                output_file = '/dev/null'
            self.run_fio(conf_file, output_file, rep_idx)
            time.sleep(2)

    def inc_fio_arg(self, fio_args, arg_name, inc_type, inc_interval, do_modify):
        arg = fio_args[arg_name]
        digit_part = re.findall(r'\d+', arg)
        str_part = re.sub(r'[^a-zA-Z]', '', arg)
        # print(arg, digit_part, str_part)
        if len(digit_part) > 1 or len(str_part) > 1:
            print(f'{__name__}: fio arg is invalid')
            print(arg)
        
        if len(str_part):
            str_part = str_part[0]
        else:
            str_part = ''

        if inc_type == 'mult':
            fio_args[arg_name] = str(int(digit_part[0]) * inc_interval) + str_part
        elif inc_type == 'add':
            fio_args[arg_name] = str(int(digit_part[0]) + inc_interval) + str_part
        elif inc_type == 'list':
            if do_modify:
                if len(inc_interval):
                    fio_args[arg_name] = str(int(inc_interval.pop(0))) + str_part
            else:
                fio_args[arg_name] = str(int(inc_interval[-1])) + str_part
        else:
            print(f'{__name__}: inc_type is invalid')

    def test_increasing_arg(self, fio_args, arg_name, num_test, inc_type, inc_interval, rep_idx):
        for i in range(num_test):
            self.test_once(fio_args, False, rep_idx)
            self.inc_fio_arg(fio_args, arg_name, inc_type, inc_interval, True)

    def test_multi_args(self, fio_args, arg_list, num_list, inc_list, int_list, rep_idx):
        if not (len(arg_list) == len(num_list) == len(inc_list) == len(int_list)):
            print('Error: list length not match')
            return
        if self.multi_arg_idx == -1:
            self.multi_arg_idx = 0

        self.store_fio_args.append(copy.deepcopy(fio_args))

        if len(arg_list) == self.multi_arg_idx + 1:
            self.store_fio_args.append(copy.deepcopy(fio_args))
            self.test_increasing_arg(fio_args,
                arg_list[self.multi_arg_idx],
                num_list[self.multi_arg_idx],
                inc_list[self.multi_arg_idx],
                int_list[self.multi_arg_idx],
                rep_idx
            )
            fio_args = copy.deepcopy(self.store_fio_args.pop())
        else:
            arg_name = arg_list[self.multi_arg_idx]
            num_test = num_list[self.multi_arg_idx]
            inc_type = inc_list[self.multi_arg_idx]
            inc_interval = int_list[self.multi_arg_idx]
            for i in range(num_test):
                self.multi_arg_idx += 1
                self.store_fio_args.append(copy.deepcopy(fio_args))
                self.test_multi_args(fio_args, arg_list, num_list, inc_list, int_list, rep_idx)
                fio_args = copy.deepcopy(self.store_fio_args.pop())
                self.inc_fio_arg(fio_args, arg_name, inc_type, inc_interval, True)
        
        fio_args = copy.deepcopy(self.store_fio_args.pop())
        
        self.multi_arg_idx -= 1
        # self.multi_arg_idx = -1 # re-init needed?

    def get_max_range_arg(self, fio_args, arg_list, num_list, inc_list, int_list):
        max_args = copy.deepcopy(fio_args)
        if not (len(arg_list) == len(num_list) == len(inc_list) == len(int_list)):
            print('Error: list length not match')
            return
        for i in range(len(arg_list)):
            for j in range(num_list[i] - 1):
                self.inc_fio_arg(max_args,
                    arg_list[i],
                    inc_list[i],
                    int_list[i],
                    False
                )
        return max_args

    def read_precond(self, fio_args): # pre-conditioning with write before read workload
        if 'read' in fio_args['rw']:
            print('READ workload. Need to pre-condition')
            fio_args['rw'] = 'write'
            self.test_once(fio_args, True)
            if not self.debug:
                time.sleep(3)
        else:
            print('WRITE workload. Pre-condition skipped')

    def fio_main(self):
        start = time.time()
        for mod in self.targ_mod_list:
            max_fio_args = self.get_max_range_arg(self.fio_init_args, **self.fio_inc_args)
            self.read_precond(max_fio_args)
            dummy_init_args = copy.deepcopy(self.fio_init_args)
            dummy_inc_args = copy.deepcopy(self.fio_inc_args)

            self.zns_operator.init_zns_raid(mod)
            for rep_idx in range(1, self.reps + self.start_rep_idx):
                dummy_inc_args['rep_idx'] = rep_idx
                self.test_multi_args(dummy_init_args, **dummy_inc_args)
                dummy_init_args = copy.deepcopy(self.fio_init_args)
                dummy_inc_args = copy.deepcopy(self.fio_inc_args)
                self.store_fio_args = []

            shutil.copytree(self.res_dir, os.path.join(self.save_dir, mod))

        end = time.time()
        elapsed_time = end - start
        self.zns_operator.print_elapsed_time(elapsed_time)


############################################################################
class fio_res_collector():
    def __init__(self, **kwargs):
        self.local_cfg = kwargs['local_cfg']
        local_cfg = ConfigParser(interpolation=ExtendedInterpolation())
        local_cfg.read(self.local_cfg)

        self.base_dir = local_cfg['test']['base_dir']
        self.save_dir = os.path.join(self.base_dir, 'save')
        self.targ_mod_list = ast.literal_eval(local_cfg['test']['targ_mod_list'])

        self.wl_name = local_cfg['collect']['wl_name']
        self.stat_name = local_cfg['collect']['stat_name']
        self.var_name_1 = local_cfg['collect']['var_name_1']
        self.var_name_2 = local_cfg['collect']['var_name_2']

        
    def arg_check(self):
        if len(sys.argv) != num_args + 1:
            return False
        else:
            return True

    def print_example(self):
        print('Input correct arguments. Ex) python3 res_collect.py /home/mwkim/dev_tester/size_var_res/lat_tcp_off randread \"avg lat\"')

    def extract_percent(self, line, perc_str):
        start_idx = line.find(perc_str)
        offset = line[start_idx:].find('[')
        end_idx = line[start_idx:].find(']')
        return float(line[start_idx+offset+1:start_idx+end_idx])

    def get_var_from_filename(self, filename, id_str):
        end_idx = filename.find(id_str)
        start_idx = filename[:end_idx].rfind('_')+1
        # print(filename[start_idx:end_idx])
        var_str = filename[start_idx:end_idx]
        if not var_str.isdigit():
            var_int = int(re.sub(r'[^0-9]', '', var_str))
        else:
            var_int = int(var_str)
        return var_int

    # assumption: res_files must be sorted in ascending order by var
    def get_var_list(self, res_files):
        var_1_list = []
        var_2_list = []
        comp = False
        prev = ''
        for rf in res_files:
            curr = self.get_var_from_filename(rf, self.var_name_1)
            if not comp:
                var_2_list.append(self.get_var_from_filename(rf, self.var_name_2))
            if prev == '':
                var_1_list.append(curr)
            elif prev != curr:
                var_1_list.append(curr)
                comp = True
            prev = curr

        return var_1_list, var_2_list

    def get_sorted_filenames(self, res_dir):
        res_files = [os.path.join(f) for f in os.listdir(res_dir) if (f.startswith(self.wl_name) and f.endswith('.res'))]
        res_files.sort(key=lambda x: 
            (int(self.get_var_from_filename(x, self.var_name_1)), 
            int(self.get_var_from_filename(x, self.var_name_2)) 
            ))
        for idx, f in enumerate(res_files):
            res_files[idx] = res_files[idx].replace(res_dir+'/', '')
        # print(res_files)
        return res_files

    def get_lat_unit(self, fp):
        for line in fp.readlines():
            if 'error' in line:
                print('error detected: '+res_file)

            if (' lat (' in line) and ('):' in line):
                lat_unit = 1 # default is msec
                if 'msec' in line:
                    lat_unit = 0.001
                elif 'usec' in line:
                    lat_unit = 1
                elif 'nsec' in line:
                    lat_unit = 1000
                else:
                    print('No matching lat_unit!')
                    exit()
        fp.seek(0)
        return lat_unit

    def collect_main(self):
        ret_df = {}
        for mod in self.targ_mod_list:
            merge_res_dir = os.path.join(self.save_dir, mod)
            extract_wl_name = self.wl_name
            extract_stat_name = self.stat_name

            stat_list = [
                'BW',
                'IOPS',
                'avg lat',
                'amount'
            ]

            merge_list = []
            num_iter = len([name for name in os.listdir(merge_res_dir) if os.path.isdir(os.path.join(merge_res_dir, name))])
            no_iter = False
            if num_iter == 0:
                no_iter = True
                num_iter = 1

            for i in range(1, num_iter+1):
                if no_iter:
                    res_dir = merge_res_dir
                else:
                    res_dir = os.path.join(merge_res_dir, str(i))


                res_list = {}

                res_files = self.get_sorted_filenames(res_dir)
                # print(res_dir, res_files)

                var_1_list, var_2_list = self.get_var_list(res_files)

                for stat_name in stat_list:
                    res_list[stat_name] = []
                    for j in range(len(var_2_list)):
                        res_list[stat_name].append([])


                var_2_idx = -1 # workload iterates 1 -> 2
                for res_file in res_files:
                    var_1 = self.get_var_from_filename(res_file, self.var_name_1)
                    var_2 = self.get_var_from_filename(res_file, self.var_name_2)
                    var_2_idx = var_2_list.index(var_2)
                    # print(res_file, var_2, min_var_2)
                    # print(var_2_idx)

                    with open(os.path.join(res_dir, res_file)) as f:
                        lat_unit = self.get_lat_unit(f)

                        for line in f.readlines():
                            if 'BW=' in line:
                                start_idx = line.find('BW=')
                                offset = line[start_idx:].find('(')
                                end_idx = line[start_idx:].find('B/s)')
                                bw = line[start_idx+offset+1:start_idx+end_idx-1]
                                if 'GB/s' in line:
                                    bw = float(bw) * 1000
                                if 'kB/s' in line:
                                    bw = float(bw) / 1000
                                res_list['BW'][var_2_idx].append(float(bw))

                            if 'IOPS=' in line:
                                start_idx = line.find('IOPS=')
                                offset = len('IOPS=')
                                end_idx = line[start_idx:].find(',')
                                iops = line[start_idx+offset:start_idx+end_idx]
                                if 'k,' in line:
                                    iops = iops[:-1]
                                    iops = float(iops) * 1000
                                res_list['IOPS'][var_2_idx].append(float(iops))

                            if ' lat (' in line and '):' in line:
                                start_idx = line.find('avg=')
                                end_idx = line[start_idx:].find(',')
                                res_list['avg lat'][var_2_idx].append(float(line[start_idx+len('avg='):start_idx+end_idx]) / lat_unit)

                    # with open(os.path.join(res_dir, res_file.replace('.res', '.write'))) as f:
                    #     for line in f.readlines():
                    #         if '(kB):' in line:
                    #             amount = line.split()[-1]
                    #             res_list['amount'][var_2_idx].append(float(amount)/1000/1000) # GB unit

                merge_list.append(res_list)

            merge_pd = DataFrame()
            res_pd = DataFrame(merge_list[0][extract_stat_name])
            # print(res_pd)
            # exit()
            while len(res_pd.columns) > 0:
                try:
                    # print all dataframes
                    first = True
                    for i, temp_res_list in zip(range(len(merge_list)), merge_list):
                        res_pd = DataFrame(temp_res_list[extract_stat_name])
                        res_pd.columns = var_1_list
                        res_pd.index = var_2_list

                        if first:
                            first = False
                            merge_pd = res_pd
                        else:
                            merge_pd = pd.concat([merge_pd, res_pd])
                        merge_pd
                    print(mod)
                    print('---ALL---')
                    print(merge_pd.round(1))
                    by_row_index = merge_pd.groupby(merge_pd.index)
                    # print('\n---MEAN---')
                    print(by_row_index.mean().round(1))
                    print('\n---STDDEV---')
                    print(by_row_index.std().round(1))
                    ret_df[mod] = by_row_index.mean().round(1)
                    break

                except Exception as e:
                    print(e)
                    merge_pd.drop(merge_pd.columns[-1], axis=1, inplace=True)
        return ret_df


############################################################################
class fio_res_visualizer():
    def __init__(self, **kwargs):
        self.local_cfg = kwargs['local_cfg']
        local_cfg = ConfigParser(interpolation=ExtendedInterpolation())
        local_cfg.read(self.local_cfg)

        self.base_dir = local_cfg['test']['base_dir']
        self.save_dir = os.path.join(self.base_dir, 'save')

        self.targ_mod_list = ast.literal_eval(local_cfg['test']['targ_mod_list'])
        self.wl_name = local_cfg['collect']['wl_name']
        self.stat_name = local_cfg['collect']['stat_name']
        self.var_name_1 = local_cfg['collect']['var_name_1']
        self.var_name_2 = local_cfg['collect']['var_name_2']

        self.base_mod = local_cfg['visualize']['base_mod']

    def visualize_main(self, df):        
        # block_sizes = df[self.targ_mod_list[0]].index.tolist()
        block_sizes = [4, 16, 32, 64, 128, 256]
        open_zone = df[self.targ_mod_list[0]].columns.tolist()

        # 전체 그림 설정
        graph_columns = (len(block_sizes)+1)//2
        fig, axs = plt.subplots(2, graph_columns, figsize=(20, 10))#, sharey=True)
        fig.tight_layout(pad=5.0)

        # 각 블록 크기에 대한 그래프 생성
        for idx, bs in enumerate(block_sizes):
            row = idx // graph_columns
            col = idx % graph_columns

            ax = axs[row][col]
            zraid = df['zraid'].loc[bs, open_zone]
            raizn = df['raizn'].loc[bs, open_zone]
            raizn_mf = df['raizn-mf'].loc[bs, open_zone]

            x_pos = np.arange(len(open_zone))
            # x_pos = x_pos *2
            width = 0.2

            # 막대 그래프 생성
            pos_val = -1
            # pos_val = -4
            ax.bar(x_pos + pos_val*width, raizn, width, label="RAIZN", edgecolor='black', linewidth=0.5, color='white')
            pos_val += 1.0
            ax.bar(x_pos + pos_val*width, raizn_mf, width, label="RAIZN+", edgecolor='black', linewidth=0.5, color='#808080')
            pos_val += 1.0
            ax.bar(x_pos + pos_val*width, zraid, width, label="ZRAID", edgecolor='black', linewidth=0.5, color='black')
            pos_val += 1.0

            #ax.plot(x_pos, ideal, 'k--', label="ideal")

            # 레이블 및 제목 설정
            ax.set_xticks(x_pos)
            ax.set_xticklabels(open_zone)
            ax.set_xlabel(f'# I/O zones', fontsize=16)

            block_size_text = f'{bs}KB'
            # ax.set_title(block_size_text)
            ax.text(0.02, 0.98, block_size_text, transform=ax.transAxes, fontsize=12, fontweight='bold', va='top', color='black')
            # ax.text(0.02, 0.98, block_size_text, transform=ax.transAxes, fontsize=12, va='top', color='#1933D2')

            #ax.set_yticks(np.arange(len(ylabel)))
            #ax.set_yticklabels(ylabel)
            if idx%3 == 0:
                ax.set_ylabel('Throughput (MB/s)',fontsize=16)
            if idx//3 == 0:
                ax.set_ylim(0,3300)
            else:
                ax.set_ylim(0,5500)
            ax.grid(True, linestyle='dotted', linewidth=0.5)

        # 범례 설정
        handles, labels = ax.get_legend_handles_labels()
        fig.legend(handles, labels, loc='upper center', ncol=8, fontsize=20)

        #     ax.set_xticks(x_pos)
        #     ax.set_xticklabels(open_zone, fontsize=16)
        #     ax.set_xlabel(f'Open Zone Numbers\n\nBlock Size: {bs}KiB', fontsize=16)


        #     #ax.set_yticks(np.arange(len(ylabel)))
        #     #ax.set_yticklabels(ylabel)
        #     if idx == 0:
        #         ax.set_ylabel('Throughput (MB/s)',fontsize=16)
        #     ax.grid(True, linestyle='dotted', linewidth=0.5)

        # order = [0, 1, 5, 2, 3, 4]
        # order = [0, 2, 1, 3, 5, 4]

        # # 범례 설정
        # handles, labels = ax.get_legend_handles_labels()
        # fig.legend([handles[idx] for idx in order], [labels[idx] for idx in order], loc='upper center', ncol=3, fontsize=20, bbox_to_anchor=(0.5, 1.1))

        # 그래프 저장
        plt.savefig('fig7.pdf', bbox_inches='tight')
        plt.savefig('fig7.png', bbox_inches='tight')

    def visualize_detail(self, df):        
        # block_sizes = df[self.targ_mod_list[0]].index.tolist()
        block_sizes = [8]
        open_zone = df[self.targ_mod_list[0]].columns.tolist()

        # 전체 그림 설정
        graph_columns = (len(block_sizes)+1)//2
        fig, ax = plt.subplots(1, 1, figsize=(10, 5))#, sharey=True)
        fig.tight_layout(pad=5.0)

        # 각 블록 크기에 대한 그래프 생성
        for idx, bs in enumerate(block_sizes):
            zraid = df['zraid'].loc[bs, open_zone]
            raizn = df['raizn'].loc[bs, open_zone]
            raizn_mf = df['raizn-mf'].loc[bs, open_zone]
            z = df['z'].loc[bs, open_zone]
            zs = df['zs'].loc[bs, open_zone]
            zsm = df['zsm'].loc[bs, open_zone]

            x_pos = np.arange(len(open_zone))
            x_pos = x_pos *2
            width = 0.2

            # 막대 그래프 생성
            # pos_val = -1
            pos_val = -2.5
            ax.bar(x_pos + pos_val*width, raizn, width, label="RAIZN", edgecolor='black', linewidth=0.5, color='white')
            pos_val += 1.0
            ax.bar(x_pos + pos_val*width, raizn_mf, width, label="RAIZN+", edgecolor='black', linewidth=0.5, color='#808080')
            pos_val += 1.0
            ax.bar(x_pos + pos_val*width, z, width, label="Z", edgecolor='black', linewidth=0.5, color='white', hatch='\\')
            pos_val += 1.0
            ax.bar(x_pos + pos_val*width, zs, width, label="Z+S", edgecolor='black', linewidth=0.5, color='white', hatch='xx')
            pos_val += 1.0
            ax.bar(x_pos + pos_val*width, zsm, width, label="Z+S+M", edgecolor='black', linewidth=0.5, color='white', hatch='....')
            pos_val += 1.0
            ax.bar(x_pos + pos_val*width, zraid, width, label="ZRAID", edgecolor='black', linewidth=0.5, color='black')
            pos_val += 1.0

            #ax.plot(x_pos, ideal, 'k--', label="ideal")

            # 레이블 및 제목 설정
            ax.set_xticks(x_pos)
            ax.set_xticklabels(open_zone)
            ax.set_xlabel(f'# I/O zones', fontsize=16)

            block_size_text = f'{bs}KB'
            # ax.set_title(block_size_text)
            ax.text(0.02, 0.98, block_size_text, transform=ax.transAxes, fontsize=12, fontweight='bold', va='top', color='black')
            # ax.text(0.02, 0.98, block_size_text, transform=ax.transAxes, fontsize=12, va='top', color='#1933D2')

            #ax.set_yticks(np.arange(len(ylabel)))
            #ax.set_yticklabels(ylabel)
            if idx == 0:
                ax.set_ylabel('Throughput (MB/s)',fontsize=16)
            ax.set_ylim(0,3300)
            ax.grid(True, linestyle='dotted', linewidth=0.5)

        order = [0, 3, 1, 4, 2, 5]
        # 범례 설정
        handles, labels = ax.get_legend_handles_labels()
        fig.legend([handles[idx] for idx in order], [labels[idx] for idx in order], loc='upper center', ncol=3, fontsize=20, bbox_to_anchor=(0.5, 1.1))
        #     ax.set_xticks(x_pos)
        #     ax.set_xticklabels(open_zone, fontsize=16)
        #     ax.set_xlabel(f'Open Zone Numbers\n\nBlock Size: {bs}KiB', fontsize=16)


        #     #ax.set_yticks(np.arange(len(ylabel)))
        #     #ax.set_yticklabels(ylabel)
        #     if idx == 0:
        #         ax.set_ylabel('Throughput (MB/s)',fontsize=16)
        #     ax.grid(True, linestyle='dotted', linewidth=0.5)

        # order = [0, 1, 5, 2, 3, 4]
        # order = [0, 2, 1, 3, 5, 4]

        # # 범례 설정
        # handles, labels = ax.get_legend_handles_labels()
        # fig.legend([handles[idx] for idx in order], [labels[idx] for idx in order], loc='upper center', ncol=3, fontsize=20, bbox_to_anchor=(0.5, 1.1))

        # 그래프 저장
        plt.savefig('fig8.pdf', bbox_inches='tight')
        plt.savefig('fig8.png', bbox_inches='tight')

    def normalize_data(self, data):
        first_column = data['raizn-mf'].columns[0]
        normalized_data = {}

        for module, df in data.items():
            if module == self.base_mod:
                normalized_data[module] = pd.DataFrame({first_column: [1.0] * len(df)}, index=df.index)  # Normalize raizn-mf to 1.0
            else:
                normalized_values = df[first_column] / data[self.base_mod][first_column]
                normalized_data[module] = pd.DataFrame({first_column: normalized_values}, index=df.index)

        return normalized_data

    def visualize_sam(self, data):
        df = self.normalize_data(data)
        # df = df.dropna(axis=1)
        df_zraid = df['zraid']
        df_raizn_mf = df['raizn-mf']

        block_sizes = ['4k', '8k', '16k', '32k', '64k']

        fig, ax = plt.subplots(figsize=(10, 5))
        fig.tight_layout(pad=5.0)

        zraid = df_zraid.values.flatten().tolist()
        raizn_mf = df_raizn_mf.values.flatten().tolist()
        x_pos = np.arange(len(block_sizes))
        width = 0.2

        # 막대 그래프 생성
        # ax.bar(x_pos - 1*width, raizn, width, label="RAIZN", edgecolor='black', linewidth=0.5, color='white')
        ax.bar(x_pos - 0.5*width, raizn_mf, width, label="RAIZN+", edgecolor='black', linewidth=0.5, color='#808080')
        # ax.bar(x_pos - 0*width, zout, width, label="Z+S+M", edgecolor='black', linewidth=0.5, color='white', hatch='....')
        # ax.bar(x_pos + 0.5*width, zraid_mq_deadline, width, label="ZRAID-mq-deadline", edgecolor='black', linewidth=0.5, color='skyblue')
        ax.bar(x_pos + 0.5*width, zraid, width, label="ZRAID", edgecolor='black', linewidth=0.5, color='black')

        #ax.plot(x_pos, ideal, 'k--', label="ideal")

        # 레이블 및 제목 설정
        ax.set_xticks(x_pos)
        ax.set_xticklabels(block_sizes, rotation=0,fontsize=16)
        ax.set_xlabel(f'Block sizes (KB)', fontsize=16)


        #ax.set_yticks(np.arange(len(ylabel)))
        #ax.set_yticklabels(ylabel)
        ax.set_ylabel('Normalized Throughput',fontsize=16)
        # ax.set_ylim([0, 1.2])
        ax.grid(True, linestyle='dotted', linewidth=0.5)

        # 범례 설정
        handles, labels = ax.get_legend_handles_labels()
        # fig.legend(handles, labels, loc='upper center', bbox_to_anchor=[0.5,0.95], ncol=3, fontsize=10)
        fig.legend(handles, labels, loc='upper center', ncol=3, fontsize=20,bbox_to_anchor=(0.5,1.))

        # 그래프 저장
        plt.savefig('fig11.pdf', bbox_inches='tight')
        plt.savefig('fig11.png', bbox_inches='tight')