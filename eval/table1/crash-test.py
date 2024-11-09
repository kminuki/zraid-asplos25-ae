import paramiko, re, shutil
import time
import subprocess
import os, sys
import signal
import random

def set_vfio(qemu_directory):
    currdir = os.getcwd()
    os.chdir(qemu_directory)    
    os.system('sudo ./vfio_zns.sh')
    os.chdir(currdir)

def unset_vfio(qemu_directory):
    currdir = os.getcwd()
    os.chdir(qemu_directory)    
    os.system('sudo ./unvfio_zns.sh')
    os.chdir(currdir)

# QEMU 프로세스 종료 함수
def kill_qemu():
    os.system('sudo pkill qemu')
    time.sleep(5)

# QEMU 다시 부팅 함수 (QEMU 실행 스크립트나 명령어를 사용)
def start_qemu(qemu_cmd, qemu_directory):
    currdir = os.getcwd()
    os.chdir(qemu_directory)
    try:
        # QEMU 프로세스를 다시 실행
        subprocess.Popen('./run_qemu.sh wd',
            shell=True,
            stdout=subprocess.DEVNULL,  # 출력 버림
            stderr=subprocess.DEVNULL)   # 에러 출력도 버림)
        print("QEMU started.")
        # QEMU가 부팅될 시간을 기다림 (필요 시 조정)
        time.sleep(60)
    except Exception as e:
        print(f"Error starting QEMU: {e}")

    os.chdir(currdir)

# QEMU 내부에서 명령어 실행 함수 (SSH 사용)
def run_command_in_qemu(hostname, username, password, command):
    try:
        # SSH 연결 설정
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname, username=username, password=password)
        
        # 명령 실행
        stdin, stdout, stderr = ssh.exec_command(command)
        print(f"Executing: {command}")
        print(stdout.read().decode())

        ssh.close()
    except Exception as e:
        print(f"Error running command in QEMU: {e}")


# SSH 접속 가능 여부 확인 함수
def check_ssh_ready(hostname, username, password, max_retries=100, delay=5):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    retries = 0
    while retries < max_retries:
        try:
            # SSH 접속 시도
            ssh.connect(hostname, username=username, password=password, port=2222, timeout=5)
            print("SSH is ready and connected.")
            ssh.close()
            return True
        except (paramiko.ssh_exception.NoValidConnectionsError, paramiko.ssh_exception.SSHException) as e:
            print(f"SSH connection attempt {retries + 1} failed: {e}")
            retries += 1
            time.sleep(delay)
    
    print("SSH is not ready after max retries.")
    return False

def run_ssh_command(host, user, password, command, sync=True):
    try:
        ssh_command = f"sshpass ssh -p 2222 -o StrictHostKeyChecking=no {user}@{host} '{command}'"
        # os.system(ssh_command)
        if sync:
            result = subprocess.run(ssh_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return result.stdout
        else:
            result = subprocess.Popen(ssh_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return None
        

    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        return None

def get_reported_lba(logfile):
    i = 1
    with open(logfile, 'r') as f:
        lines = f.readlines()
        while (i<len(lines)):
            line = lines[len(lines) - i]
            nextline = lines[len(lines) - i - 1]
            next_lba = -1
            # print(i, line)
            if ('LBA:' in line):
                if ('LBA:' in nextline):
                    lba_str = nextline.split()[-1]
                    next_lba = re.sub(r'\D', '', lba_str)
                    if isinstance(next_lba, str):
                        if next_lba.isdigit():
                            next_lba = int(next_lba)

                lba_str = line.split()[-1]
                lba = re.sub(r'\D', '', lba_str)
                if isinstance(lba, str):
                    if lba.isdigit():
                        if (next_lba != -1) and (next_lba < int(lba)):
                            # print(f'upperline: {next_lba}, curr: {lba}')
                            return int(lba)
                        else:
                            print(f'Pass to upperline! upperline: {next_lba}, curr: {lba}')
            i += 1
        return 0 # No successed write

def extract_wptr_decimal(lines):
    i = 1
    # 정규식을 사용하여 wptr에 해당하는 16진수 값을 추출
    while (i<len(lines)):
        line = lines[len(lines) - i]
        # print(line)
        match = re.search(r'wptr\s*0x([0-9a-fA-F]+)', line)
        if match:
            hex_value = match.group(1)  # wptr의 16진수 값 추출
            lba = int(hex_value, 16)  # 16진수를 10진수로 변환
            return lba
        i += 1
    raise ValueError("wptr not found in the line")

def print_elapsed_time(elapsed_time_seconds):
    # 일, 시간, 분, 초로 변환
    days = elapsed_time_seconds // (24 * 3600)
    remaining_seconds = elapsed_time_seconds % (24 * 3600)
    hours = remaining_seconds // 3600
    remaining_seconds %= 3600
    minutes = remaining_seconds // 60
    seconds = remaining_seconds % 60
    # 결과 출력
    print(f"{days} days, {hours} hours, {minutes} minutes, {seconds} seconds")


if __name__ == "__main__":
    qemu_ip = "0.0.0.0"  # QEMU VM의 IP 주소
    qemu_username = "ubuntu"  # QEMU VM의 사용자 이름
    qemu_password = "raizn"  # QEMU VM의 사용자 비밀번호
    qemu_directory = "qemu-script/"
    qemu_command = "./run_qemu.sh wd"
    valid_policy_list = ['stripe', 'chunk', 'log']

    if len(sys.argv) != 2:
        print('Input one consistency policy: stripe, chunk, log')
        exit()

    cons_policy = sys.argv[1]
    if not cons_policy in valid_policy_list:
        print(f'Invalid consistency policy: {cons_policy}')
        exit()
    
    if cons_policy == 'stripe':
        build_flag = '-DSTRIPE_BASED_REC'
    elif cons_policy == 'chunk':
        build_flag = ''
    elif cons_policy == 'wd':
        build_flag = '-DWRITE_BACK_CACHE'

    start_time = time.time()

    # 0. set vfio
    set_vfio(qemu_directory)

    # 1. QEMU 강제 종료
    kill_qemu()

    success = 0
    fail = 0
    wp_delta = 0

    for rep in range(100):
        lba_fail = False
        verify_fail = False
        print(f'REP {rep+1} running..')
        # 2. QEMU 다시 부팅 (예시로 QEMU 실행 명령어를 사용)
        # 작업 디렉토리 변경
        start_qemu(qemu_command, qemu_directory)

        check_ssh_ready(qemu_ip, qemu_username, qemu_password)

        # 3. 
        run_ssh_command(qemu_ip, qemu_username, qemu_password, f"./all.sh {build_flag}")
        print("ZRAID configured")
        run_ssh_command(qemu_ip, qemu_username, qemu_password, "./work.sh", False)

        # 4.
        runtime = random.uniform(0.5, 20)
        print(f'Workload runtime: {runtime} (sec)')
        time.sleep(runtime)
        kill_qemu()
        print("Workload killed")
        commit_lba = get_reported_lba('./qemu.log')
        shutil.copy('./qemu.log', './bck-qemu.log')
        print(f"Commit lba: {commit_lba/2} (KB)")

        start_qemu(qemu_command, qemu_directory)

        # 5.
        output = run_ssh_command(qemu_ip, qemu_username, qemu_password, f"./recover.sh {build_flag}")
        report_lba = extract_wptr_decimal(lines = re.split(r'[\n\r]', output))
        print(f"Report lba: {report_lba/2} (KB)")
        if report_lba < commit_lba:
            print(f'LBA error! reported: {report_lba/2} (KB), commit: {commit_lba/2} (KB)')
            # print(output)
            # exit()
            wp_delta += (commit_lba - report_lba)
            lba_fail = True
        elif report_lba == 0:
            print('Reported LBA is zero. No count')
            continue

        output = run_ssh_command(qemu_ip, qemu_username, qemu_password, f"./verify.sh {report_lba/2}") # lba is sector unit, convert to KiB
        if 'OK' in output:
            print('Verify Success!')
        else:
            print('Verify failed!')
            # print(output)
            exit()
            verify_fail = True

        if lba_fail or verify_fail:
            print(f'REP {rep+1} fail')
            fail += 1
        else:
            print(f'REP {rep+1} success')
            success += 1
        print()
        kill_qemu()
    
    print(f'success: {success}, fail: {fail}')
    if (fail > 0):
        print(f'Data loss total: {wp_delta/2} (KB), avg: {wp_delta/fail/2} (KB)')
    print()

    end_time = time.time()
    elapsed_time = end_time - start_time
    print_elapsed_time(elapsed_time)

    unset_vfio(qemu_directory)