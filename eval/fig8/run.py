import sys
sys.path.append('../')
import fio

fiot = fio.fio_tester(global_cfg='../global.ini', local_cfg='fig8.ini')
fiot.fio_main()
