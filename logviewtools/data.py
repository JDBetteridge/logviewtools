import pandas as pd

from pandas import DataFrame


def scalingrun2frame(scalingrun):
    frame = pd.concat([run.dataframe for run in scalingrun])
    return frame

def logview2frame(logview):
    summary = summary2frame(logview)
    stages = allstages2frame(logview.Stages)
    frame = pd.concat([summary, stages])
    frame['nprocs'] = logview.size
    return frame

def summary2frame(logview):
    frame = DataFrame(index=range(logview.size))
    conversion = {'LocalTimes': 'time',
                  'LocalMessages': 'numMessages',
                  'LocalMessageLens': 'messageLength',
                  'LocalReductions': 'numReductions',
                  'LocalFlop': 'flop',
                  'LocalObjects': 'objects',
                  'LocalMemory': 'memory'}
    for key in conversion:
        col = DataFrame.from_dict(getattr(logview, key),
                                  orient='index',
                                  columns=[conversion[key]])
        frame = frame.join(col)
    frame['cpuid'] = frame.index
    frame['stage'] = 'Global'
    frame['call'] = 'summary'
    return frame

def allstages2frame(allstages):
    stagelist = []
    for key in list(allstages.keys()):
        stageframe = stage2frame(allstages[key])
        stageframe['stage'] = key
        stagelist.append(stageframe)
    frame = pd.concat(stagelist)
    return frame

def stage2frame(stage):
    calllist = []
    for key in list(stage.keys()):
        callframe = call2frame(stage[key])
        callframe['call'] = key
        calllist.append(callframe)
    frame = pd.concat(calllist)
    return frame

def call2frame(call):
    frame = DataFrame.from_dict(call, orient='index')
    frame['cpuid'] = frame.index
    return frame
