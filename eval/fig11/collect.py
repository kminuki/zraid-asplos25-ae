import sys
sys.path.append('../')
import fio

fiorc = fio.fio_res_collector(local_cfg='fig11.ini')
df = fiorc.collect_main()
fiorv = fio.fio_res_visualizer(local_cfg='fig11.ini')
fiorv.visualize_sam(df)
