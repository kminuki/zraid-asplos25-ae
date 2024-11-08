import sys
sys.path.append('../')
import filebench

fbt = filebench.filebench_tester(global_cfg='../global.ini', local_cfg='claim1.ini')
fbt.filebench_main(True)