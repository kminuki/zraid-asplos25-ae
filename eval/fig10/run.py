import sys
sys.path.append('../')
import db_bench

dbt = db_bench.db_bench_tester(global_cfg='../global.ini', local_cfg='fig10.ini')
dbt.db_bench_main()