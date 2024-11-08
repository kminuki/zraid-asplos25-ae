import os, sys

ZRAID_DIR = '../src/zraid'
RAIZN_DIR = '../src/raizn'
MOD_DIR_BASE = './modules'

def run_cmd(cmd):
    print(cmd)
    os.system(cmd)

def setup_dirs():
    if not os.path.exists(os.path.join(MOD_DIR_BASE, 'wd')):
        os.makedirs(os.path.join(MOD_DIR_BASE, 'wd'))
    if not os.path.exists(os.path.join(MOD_DIR_BASE, 'sam')):
        os.makedirs(os.path.join(MOD_DIR_BASE, 'sam'))

def make_zraid(dev_type):
    orig_pwd = os.getcwd()
    os.chdir(f'{ZRAID_DIR}')

    if dev_type == "wd":
        add_opt = ''
        MOD_DIR = os.path.join(MOD_DIR_BASE, 'wd')
    elif dev_type == "sam":
        add_opt = '-DSAMSUNG_MODE'
        MOD_DIR = os.path.join(MOD_DIR_BASE, 'sam')
    else:
        print("Provide valid dev_type (wd or sam)")
        exit()

    run_cmd(f'make CFLAGS=\"-DPP_INPLACE -DPERF_MODE {add_opt}\"')
    run_cmd(f'cp zraid.ko {os.path.join(orig_pwd, MOD_DIR, "zraid.ko")}')
    
    run_cmd(f'make CFLAGS=\"-DPP_INPLACE -DPERF_MODE -DRECORD_PP_AMOUNT {add_opt}\"')
    run_cmd(f'cp zraid.ko {os.path.join(orig_pwd, MOD_DIR, "zraid-stat.ko")}')

    run_cmd(f'make CFLAGS=\"-DMQ_DEADLINE -DPP_OUTPLACE -DDUMMY_HDR -DPERF_MODE {add_opt}\"')
    run_cmd(f'cp zraid.ko {os.path.join(orig_pwd, MOD_DIR, "z.ko")}')

    run_cmd(f'make CFLAGS=\"-DPP_OUTPLACE -DDUMMY_HDR -DPERF_MODE {add_opt}\"')
    run_cmd(f'cp zraid.ko {os.path.join(orig_pwd, MOD_DIR, "zs.ko")}')

    run_cmd(f'make CFLAGS=\"-DPP_OUTPLACE -DPERF_MODE {add_opt}\"')
    run_cmd(f'cp zraid.ko {os.path.join(orig_pwd, MOD_DIR, "zsm.ko")}')

    os.chdir(orig_pwd)


def make_raizn(dev_type):
    orig_pwd = os.getcwd()
    os.chdir(f'{RAIZN_DIR}')

    if dev_type == "wd":
        add_opt = ''
        MOD_DIR = os.path.join(MOD_DIR_BASE, 'wd')
    elif dev_type == "sam":
        add_opt = '-DSAMSUNG_MODE'
        MOD_DIR = os.path.join(MOD_DIR_BASE, 'sam')
    else:
        print("Provide valid dev_type (wd or sam)")
        exit()

    run_cmd(f'make CFLAGS=\"{add_opt}\"')
    run_cmd(f'cp raizn_orig.ko {os.path.join(orig_pwd, MOD_DIR, "raizn.ko")}')

    run_cmd(f'make CFLAGS=\"-DMULTI_FIFO {add_opt}\"')
    run_cmd(f'cp raizn_orig.ko {os.path.join(orig_pwd, MOD_DIR, "raizn-mf.ko")}')

    run_cmd(f'make CFLAGS=\"-DMULTI_FIFO -DRECORD_PP_AMOUNT {add_opt}\"')
    run_cmd(f'cp raizn_orig.ko {os.path.join(orig_pwd, MOD_DIR, "raizn-stat.ko")}')
    

    os.chdir(orig_pwd)

if __name__ == "__main__":
    setup_dirs()
    make_zraid('wd')
    make_raizn('wd')
    make_zraid('sam')
    make_raizn('sam')
