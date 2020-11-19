import argparse

from logviewtools import PETScLogView
from logviewtools.data import scalingrun2frame


parser = argparse.ArgumentParser()
parser.add_argument('logview',
                    nargs='+',
                    type=str,
                    help='logview.py to read results from (glob)')
args, unknown = parser.parse_known_args()

loglist = []
for log in args.logview:
    logview = PETScLogView(log)
    loglist.append(logview)

frame = scalingrun2frame(loglist)
