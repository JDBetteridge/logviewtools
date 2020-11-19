''' Attempt to recreate the same ascii output as `-log_view` does from a
saved logview file
'''
import argparse
import pandas as pd

from logviewtools import PETScLogView
from pandas import DataFrame

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

# Grab dataframe and remove empty rows
frame = logview.dataframe
frame = frame[(frame['count']>0) | (frame['call']=='summary')]

# Keep size, list of stages and list of calls for ordering
size = logview.size
stagelist = list(frame['stage'].unique())
stagelist.remove('Global')
calllist = frame['call'].unique()
calldict = {k : ii for ii,k in enumerate(calllist)}

pd.set_option('display.max_rows', None)
pd.set_option('display.show_dimensions', False)
pd.set_option('display.precision', 3)
print('---------------------------------------------- PETSc Performance Summary: ----------------------------------------------')

'''                         Max       Max/Min     Avg       Total
Time (sec):                   x         x           x
Objects:                      x         x           x
Flop:                         x         x           x          x
Flop/sec:                     x         x           x          x
MPI Messages:                 x         x           x          x
MPI Message Lengths:          x         x           x          x
MPI Reductions:               x         x
'''

# Add additional fields
with pd.option_context('mode.chained_assignment', None):
    frame['flop/sec'] = frame['flop']/frame['time']

# Create stats frame
maxframe = frame.groupby(['nprocs', 'stage', 'call']).max()
minframe = frame.groupby(['nprocs', 'stage', 'call']).min()
avgframe = frame.groupby(['nprocs', 'stage', 'call']).mean()
sumframe = frame.groupby(['nprocs', 'stage', 'call']).sum()

# Create summary stats table
selection = (size, 'Global', 'summary')
rows = ['time', 'objects', 'flop', 'flop/sec', 'numMessages', 'messageLength', 'numReductions']
summary = DataFrame(index=rows)
summary = summary.join(maxframe.loc[selection][rows].rename('Max'))
summary = summary.join((maxframe.loc[selection]/minframe.loc[selection])[rows].rename('Max/Min'))
rows.remove('numReductions')
summary = summary.join(avgframe.loc[selection][rows].rename('Avg'))
rows.remove('time')
rows.remove('objects')
summary = summary.join(sumframe.loc[selection][rows].rename('Total'))
print(summary.fillna(''))
print('\n\n')

'''
Summary of Stages:   ----- Time ------  ----- Flop ------  --- Messages ---  -- Message Lengths --  -- Reductions --
                        Avg     %Total     Avg     %Total    Count   %Total     Avg         %Total    Count   %Total
 0:      Main Stage:     x         x        x         x        x        x        x            x         x        x
 1:         Stage_1:     x         x        x         x        x        x        x            x         x        x
 2:         Stage_2:     x         x        x         x        x        x        x            x         x        x
'''

# Create stats frame
pcttotframe = sumframe*100/sumframe.loc[selection]

# Create stage stats table
selection = (size, slice(None), 'summary')
cols = ['time', 'flop', 'numMessages', 'messageLength', 'numReductions']
percent = pcttotframe.loc[selection]
avg = avgframe.loc[selection]
stagesummary = pd.concat([avg, percent], keys=['Avg', '%Total'], axis=1)
stagesummary = stagesummary.swaplevel(axis=1).groupby(axis=1, level=[0,1]).max()
stagesummary = stagesummary[cols]
print('Summary of Stages:')
print(stagesummary.loc[stagelist].reset_index())
print('\n\n')
print('------------------------------------------------------------------------------------------------------------------------')

'''
------------------------------------------------------------------------------------------------------------------------
Event                Count      Time (sec)     Flop                              --- Global ---  --- Stage ----  Total
                   Max Ratio  Max     Ratio   Max  Ratio  Mess   AvgLen  Reduct  %T %F %M %L %R  %T %F %M %L %R Mflop/s
------------------------------------------------------------------------------------------------------------------------

--- Event Stage n: Main Stage

Call1               x    x     x        x      x     x     x       x       x      x  x  x  x  x   x  x  x  x  x    x
Call2               x    x     x        x      x     x     x       x       x      x  x  x  x  x   x  x  x  x  x    x
Call3               x    x     x        x      x     x     x       x       x      x  x  x  x  x   x  x  x  x  x    x
Call4               x    x     x        x      x     x     x       x       x      x  x  x  x  x   x  x  x  x  x    x
'''

for ii, stage in enumerate(stagelist):
    print('--- Event Stage', ii,':', stage)
    print()
    # Create stats frame
    selection = (size, stage, slice(None))
    maxstage = maxframe.loc[selection]
    minstage = minframe.loc[selection]
    avgstage = avgframe.loc[selection]
    sumstage = sumframe.loc[selection]
    pcttot = pcttotframe.loc[selection]

    pctstage = sumstage*100/sumstage.loc['summary']

    stageframe = DataFrame(index=pctstage.index)
    for col in ['count', 'time', 'flop']:
        stageframe[(col, 'Max')] = maxstage[col]
        stageframe[(col, 'Ratio')] = maxstage[col]/minstage[col]

    stageframe[('', 'numMessages')] = sumstage['numMessages']
    stageframe[('', 'AvgLength')] = avgstage['messageLength']
    stageframe[('', 'numReductions')] = sumstage['numReductions']

    longcol = ['time', 'flop', 'numMessages', 'messageLength', 'numReductions']
    shorttag = ['%T', '%F', '%M', '%L', '%R']
    for col, tag in zip(longcol, shorttag):
        stageframe[('Global', tag)] = pcttot[col]

    for col, tag in zip(longcol, shorttag):
        stageframe[('Stage', tag)] = pctstage[col]

    stageframe.columns = pd.MultiIndex.from_tuples(stageframe.columns)
    stageframe = stageframe.sort_index(key=lambda x: [calldict[y] for y in x])
    stageframe = stageframe.fillna(0.0)
    with pd.option_context('mode.chained_assignment', None):
        stageframe[('count', 'Max')].loc['summary'] = ''
        stageframe[('count', 'Ratio')].loc['summary'] = ''

    print(stageframe)
    print('\n\n')
