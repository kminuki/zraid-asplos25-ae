import sys, os, time, shutil, subprocess
from configparser import ConfigParser, ExtendedInterpolation

class zns_operator():
    def __init__(self, **kwargs):
        self.global_cfg = kwargs['global_cfg']
        global_cfg = ConfigParser(interpolation=ExtendedInterpolation())
        global_cfg.read(self.global_cfg)
        self.nvme_path = global_cfg['kernel']['nvme_path']
        self.nvme_cmd = global_cfg['kernel']['nvme_cmd']
        self.MOD_DIR_BASE = global_cfg['kernel']['mod_dir_base']
        self.MOUNT_POINT = global_cfg['kernel']['mount_point']

        self.debug = kwargs['debug']
        self.dev_type = kwargs['dev_type']
        self.CONV_DEV = None
        self.raid_devices = None
        self.samsung_dev = None
        self.get_devs()

        self.ZNS_DEV = '/dev/mapper/raizn0'
    
    def get_devs(self):
        result = subprocess.run(['lsblk', '-o', 'NAME,MODEL', '-d', '-n'], stdout=subprocess.PIPE, text=True)
        lines = result.stdout.splitlines()
        if self.dev_type == 'wd':
            # 필터링 및 정렬
            filtered_devices = [
                line.split()[0]
                for line in lines
                if 'WZS4C8T1TDSP303' in line and not ('n1' in line.split()[0])
            ]
            filtered_devices.sort()
            self.raid_devices = filtered_devices

        else: 
            for line in lines:
                if 'NETAPPX4022S173A4T0NTZ' in line:
                    self.samsung_dev = line.split()[0]
            self.raid_devices = [
                'mapper/linear_1',
                'mapper/linear_2',
                'mapper/linear_3',
                'mapper/linear_4',
                'mapper/linear_5',
            ]
        print(self.raid_devices)

        for line in lines:
            if 'FADU' in line:
                self.CONV_DEV = f'/dev/{line.split()[0]}p1'
        
        if self.CONV_DEV is None:
            print('No FADU devices.')

    def sleep(self, s_time):
        if not self.debug:
            time.sleep(s_time)

    def run_cmd(self, cmd):
        if self.debug:
            print(cmd)
        else:
            print(cmd)
            os.system(cmd)

    def reset_conv_dev(self):
        command = f'sudo nvme format -f /dev/{self.CONV_DEV}'


    def reset_all_zone(self, dev):
        # command = f'sudo blkzone reset /dev/{dev}'
        command = f'sudo nvme zns reset-zone -a /dev/{dev}'
        # command = f'sudo nvme format -f /dev/{dev}'
        self.run_cmd(command)

    def reset_all_raid_devs(self):
        if 'mapper' in self.raid_devices[0]:
            self.reset_all_zone(self.samsung_dev)
        else:
            for dev in self.raid_devices:
                self.reset_all_zone(dev)
        time.sleep(3)

    def set_scheduler(self, scheduler, dev_type):
        if dev_type == 'wd':
            for dev in self.raid_devices:
                self.run_cmd(f'echo {scheduler} | sudo tee /sys/block/{dev}/queue/scheduler')
        elif dev_type == 'sam':
            self.run_cmd(f'echo {scheduler} | sudo tee /sys/block/{self.samsung_dev}/queue/scheduler')

    def unset_raizn(self):
        command = f'sudo dmsetup remove /dev/mapper/raizn0'
        self.run_cmd(command)

    def set_raizn(self, dev_type):
        self.reset_all_raid_devs()
        self.sleep(2)
        if dev_type == 'wd':
            command = f'echo "0 15082717184 raizn 64 16 1 0'
        elif dev_type == 'sam':
            # command = f'echo "0 6291456000 raizn 64 16 1 0'
            command = f'echo "0 6056574976 raizn 64 16 1 0'
        for dev in self.raid_devices:
            command += f' /dev/{dev}'
        command += '" | sudo dmsetup create raizn0'
        self.run_cmd(command)

    def reload_raizn(self, dummy):
        # self.reset_all_zone('/dev/mapper/raizn0')
        # self.sleep(2)
        self.unset_raizn()
        self.sleep(2)
        self.set_raizn(self.dev_type)

    def rmmod_all(self):
        self.run_cmd(f'sudo rmmod zraid')
        self.run_cmd(f'sudo rmmod raizn_orig')

    def set_scheduler_mod(self, mod_name, dev_type):
        if 'raizn' in mod_name:
            self.set_scheduler('mq-deadline', dev_type)
        elif ('z' == mod_name):
            self.set_scheduler('mq-deadline', dev_type)
        else:
            self.set_scheduler('none', dev_type)

    def ins_nvme(self):
        self.unset_raizn()
        self.rmmod_all()
        orig_pwd = os.getcwd()
        os.chdir(self.nvme_path)
        self.run_cmd(self.nvme_cmd)
        time.sleep(3)
        os.chdir(orig_pwd)
        self.get_devs()

    def set_samsung_part(self):
        lba_gap = 1572864000 #8000 zones
        for i in range(1, 6):
            start_sector = (i-1) * lba_gap
            self.run_cmd(f'sudo dmsetup create linear_{i} --table "0 {lba_gap} linear /dev/{self.samsung_dev} {start_sector}"')
        time.sleep(3)
        
    def unset_samsung_part(self):
        for i in range(1, 6):
            self.run_cmd(f'sudo dmsetup remove linear_{i}')
        time.sleep(3)

    def insmod(self, mod_name, dev_type):
        self.run_cmd(f'sudo umount {self.CONV_DEV}')
        time.sleep(3)
        self.unset_raizn()
        time.sleep(3)
        self.rmmod_all()
        time.sleep(3)
        self.run_cmd(f'sudo insmod {os.path.join(self.MOD_DIR_BASE, dev_type, mod_name+".ko")}')
        time.sleep(3)

    def perf_tune(self):
        self.run_cmd('sudo /etc/init.d/irqbalance stop')
        time.sleep(1)
        self.run_cmd('sudo tuned-adm profile throughput-performance')
        time.sleep(1)
        self.run_cmd('echo 0 | sudo tee /proc/sys/kernel/randomize_va_space')
        time.sleep(1)

    def init_zns_raid(self, mod_name):
        self.unset_samsung_part()
        self.ins_nvme()
        if self.dev_type == 'sam':
            self.set_samsung_part()
        self.insmod(mod_name, self.dev_type)
        self.reset_conv_dev()
        self.set_scheduler_mod(mod_name, self.dev_type)
        self.perf_tune()

    def init_f2fs(self, mkfs_path, mnt_path):
        self.run_cmd(f'sudo {mkfs_path} -f -m -c {self.ZNS_DEV} {self.CONV_DEV}')
        self.run_cmd(f'sudo mount -t f2fs {self.CONV_DEV} {mnt_path}')
        self.run_cmd('echo 0 | sudo tee /proc/sys/kernel/randomize_va_space')
        time.sleep(1)

    def init_zenfs(self, dm_name, zenfs_mk_path, zenfs_aux_path):
        self.run_cmd(f'sudo mkfs.ext4 -F {self.CONV_DEV}')
        time.sleep(1)
        self.run_cmd(f'sudo mount {self.CONV_DEV} {zenfs_aux_path}')
        time.sleep(1)
        # try:
        #     shutil.rmtree(zenfs_aux_path)  # 디렉토리와 그 내용물 삭제
        #     os.makedirs(zenfs_aux_path)
        # except Exception as e:
        #     print(f'Failed to delete {zenfs_aux_path}. Reason: {e}')

        for entry in os.listdir(zenfs_aux_path):
            entry_path = os.path.join(zenfs_aux_path, entry)  # 항목의 전체 경로
            try:
                if os.path.isfile(entry_path) or os.path.islink(entry_path):
                    os.unlink(entry_path)  # 파일이나 심볼릭 링크 삭제
                    print(f"Deleted file or link: {entry_path}")
                elif os.path.isdir(entry_path):
                    shutil.rmtree(entry_path)  # 서브디렉토리 삭제
                    print(f"Deleted directory: {entry_path}")
            except Exception as e:
                print(f'Failed to delete {entry_path}. Reason: {e}')

        time.sleep(1)
        self.run_cmd(f'sudo {zenfs_mk_path} mkfs --zbd={dm_name} --aux_path={zenfs_aux_path}')
        time.sleep(1)
        # self.run_cmd('echo 0 | sudo tee /proc/sys/kernel/randomize_va_space')
        # time.sleep(1)

    def umount_f2fs(self):
        self.run_cmd(f'sudo umount {self.MOUNT_POINT}')
        time.sleep(1)


    # 디스크의 쓰기 통계를 얻는 함수
    def get_disk_write_stat(self, disk):
        # iostat 실행하여 디스크의 쓰기 통계 추출
        result = subprocess.run(['iostat', '-d', f'/dev/{disk}', '1', '1'], stdout=subprocess.PIPE)
        output = result.stdout.decode('utf-8').splitlines()

        # 적절한 라인 찾기
        for line in output:
            if disk in line:
                # 원하는 디스크 통계 라인에서 kB_wrtn 값을 추출
                values = line.split()
                kb_wrtn = float(values[6])  # kB_wrtn은 보통 6번째 위치
                # print(kb_wrtn)
                return kb_wrtn

    def get_raid_write_stat(self):
        total_write = 0
        if self.dev_type == 'wd':
            for device in self.raid_devices:
                total_write += self.get_disk_write_stat(device)
        elif self.dev_type == 'sam':
            total_write += self.get_disk_write_stat(self.samsung_dev)
        return total_write

    def print_elapsed_time(self, elapsed_time_seconds):
        days = elapsed_time_seconds // (24 * 3600)
        remaining_seconds = elapsed_time_seconds % (24 * 3600)
        hours = remaining_seconds // 3600
        remaining_seconds %= 3600
        minutes = remaining_seconds // 60
        seconds = remaining_seconds % 60
        print(f"Elapsed time: {days} days, {hours} hours, {minutes} minutes, {seconds} seconds")