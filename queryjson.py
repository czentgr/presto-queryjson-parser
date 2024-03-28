# Python program to convert
# json to parquet tables
 
import json
import sys
import os

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
 
if (len(sys.argv) != 2):
  print("Specify the input json file or directory")
 
jsonfiles = []

if os.path.isfile(sys.argv[1]):
  jsonfiles.append(sys.argv[1])
else:
  for subdir, dirs, files in os.walk(sys.argv[1]):
     for file in files:
        jsonfiles.append(os.path.join(subdir, file))

failed = []
slow_queries = []
for file in jsonfiles:
  jsonfile = open(file)
  data = json.load(jsonfile)

  root = {}
  root['file'] = file;
  if (data['state'] == 'FAILED'):
    failed.append(file)
    continue

  root['query'] = data['query'];

  queryStats = data['queryStats'];
  root['execTime'] = queryStats['executionTime'];
  timeVal = time_val(root['execTime'])
  if (timeVal < 120) :
    continue

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
    if (opVal > 3600) :
       summaries.append({'id':str(s['stageId']) + '-' + s['operatorType'] + '-' + str(s['totalDrivers']),
                         'outputWallTime':s['getOutputWall'],
                         'addInputWall':s['addInputWall'],
                         'blockedWall':s['blockedWall'],
                        });
  root['opSummaries'] = summaries;
  jsonfile.close()
  slow_queries.append((timeVal, root));


#failed
print('Failed Queries:')
for query in failed:
  print(query)
print('')

sorted_slow_queries = sorted(slow_queries, key = lambda x: x[0], reverse = True)[:25] 
print('Slow Queries')
for query in sorted_slow_queries:
  print('execTime : ' + query[1]['execTime'] + '(' + str(query[0])  + 's)')
  print('file : ' + query[1]['file'])
  print('totalTasks : ' + str(query[1]['totalTasks']))
  print('peakRunningTasks : ' + str(query[1]['peakRunningTasks']))
  print('totalCpuTime : ' + str(query[1]['totalCpuTime']))
  print('totalBlockedTime : ' + query[1]['totalBlockedTime'])
  print('shuffledDataSize : ' + query[1]['shuffledDataSize'])
  for summary in query[1]['opSummaries']:
    print(summary)
  print('')
