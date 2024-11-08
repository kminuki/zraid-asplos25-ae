import sys
sys.path.append('../')
import filebench

fbt = filebench.filebench_tester(global_cfg='../global.ini', local_cfg='fig9.ini')
fbt.filebench_main()