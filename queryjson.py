# Python program to convert
# json to parquet tables
 
import json
import string
import sys
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--stagestate', nargs='?', const=False, type=bool)
parser.add_argument('--path')
args = parser.parse_args()

def time_val(tstr):
  time = 0
  if (tstr[-2:] == 'ns' or tstr[-2:] == 'ms' or tstr[-2:] == 'us') :
    time = 0
  elif (tstr[-1] == 's') :
    time = float(tstr[:-1])
  elif (tstr[-1] == 'm') :
    wh = int(tstr[:-4])
    part = int(tstr[-3:-1]) 
    time = wh * 60 + part * 60 / 100
  elif (tstr[-1] == 'h') :
    wh = int(tstr[:-4])
    part = int(tstr[-3:-1])
    time = wh * 60 * 60 + part * 60 * 60 / 100
  return time
 
if (args.path == ""):
  print("Specify the input json file or directory")

jsonfiles = []

if os.path.isfile(args.path):
  jsonfiles.append(args.path)
else:
  for subdir, dirs, files in os.walk(args.path):
     for file in files:
        jsonfiles.append(os.path.join(subdir, file))

failed = []
queries = []
for file in jsonfiles:
  jsonfile = open(file)
  data = json.load(jsonfile)

  root = {}
  root['file'] = file;
  if (data['state'] == 'FAILED'):
    failed.append(file)

  if (args.stagestate) :
    stages = []
    states = []
    stages.append(data['outputStage'])
    while (len(stages) > 0):
      stage = stages.pop(0)
      offset = stage['stageId'].index('.')
      stageId = stage['stageId'][offset + 1:]
      state = stage['latestAttemptExecutionInfo']['state']
      states.append((int(stageId), {"Stage " + stageId + ' : ' + state}))
      stages += stage['subStages']
    sorted_states = sorted(states, key = lambda x: x[0])
    for state in sorted_states:
      print(state[1])
    print()

  root['query'] = data['query'];
  queryStats = data['queryStats'];
  root['execTime'] = queryStats['executionTime'];
  timeVal = time_val(root['execTime'])

  root['totalTasks'] = queryStats['totalTasks'];
  root['peakRunningTasks'] = queryStats['peakRunningTasks'];
  root['totalDrivers'] = queryStats['totalDrivers'];
  root['totalCpuTime'] = queryStats['totalCpuTime'];
  root['totalBlockedTime'] = queryStats['totalBlockedTime'];
  root['shuffledDataSize'] = queryStats['shuffledDataSize']; 

  opSummaries = queryStats['operatorSummaries'];
  summaries = []
  for s in opSummaries:
    opVal = time_val(s['getOutputWall'])
    if (opVal > 0) :
       summaries.append((time_val(s['getOutputWall']), 
                        {'s':s['stageId'],
                         'p':s['planNodeId'],
                         'o':s['operatorType'],
                         'd':s['totalDrivers'],
                         'outputWall':s['getOutputWall'],
                         'inputWall':s['addInputWall'],
                         'blockedWall':s['blockedWall'],
                         'outputRows':s['outputPositions'], 
                        }));
  
  sorted_summaries = sorted(summaries, key = lambda x: x[0], reverse = True)
  root['opSummaries'] = sorted_summaries;
  jsonfile.close()
  queries.append((timeVal, root));

#failed
if (len(failed) > 0):
  print('Failed Queries:')
  for query in failed:
    print(query)
  print()

sorted_queries = sorted(queries, key = lambda x: x[0], reverse = True) 
print('Sorted Queries')
print('s: stageId, p: planNode, o: operatorName, d:DriverCount')
for query in sorted_queries:
  print('execTime : ' + query[1]['execTime'] + '(' + str(query[0])  + 's)')
  print('file : ' + query[1]['file'])
  print('totalTasks : ' + str(query[1]['totalTasks']))
  print('peakRunningTasks : ' + str(query[1]['peakRunningTasks']))
  print('totalCpuTime : ' + str(query[1]['totalCpuTime']))
  print('totalBlockedTime : ' + query[1]['totalBlockedTime'])
  print('shuffledDataSize : ' + query[1]['shuffledDataSize'])
  for summary in query[1]['opSummaries']:
    print(summary[1])
  print('')
