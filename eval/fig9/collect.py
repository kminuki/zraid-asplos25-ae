import sys
sys.path.append('../')
import filebench

fbrc = filebench.filebench_res_collector(local_cfg='fig9.ini')
df = fbrc.collect_main()

fbrv = filebench.filebench_res_visualizer(local_cfg='fig9.ini')
fbrv.visualize_main(df)