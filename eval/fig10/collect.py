import sys
sys.path.append('../')
import db_bench

dbrc = db_bench.db_bench_res_collector(local_cfg='fig10.ini')
df = dbrc.collect_main()

dbrv = db_bench.db_bench_res_visualizer(local_cfg='fig10.ini')
dbrv.visualize_main(df)