# presto-queryjson-parser
A Python parser to read Presto query json files. 
Given a path to a query json file or a folder with query json files, the parser
1) Filters failed queries.
2) Sorts queries from slowest to fastest and prints operators with outputWall greater than a second.
3) Optionally prints the state of the stages.

```
Usage: queryjson [options] path

Analyze a Presto Query JSON document. If multiple queries are provided the
output is ordered by execution time longest to shortest.

Options:
  -h, --help         show this help message and exit
  --stagestate       Collect and print stage and state information
  --opwall=OPWALL_S  Operator Wall time in seconds to show operator details
                     (Default: 0)
  --sortby=SORT_KEY  Sort field (Default: 'getOutputWall'. Other fields:
                     'addInputWall', 'blockedWall')
```

The program can be executed as a program or as input to python3.  
`python3 queryjson.py [options] path` is equivalent to `queryjson.py [options] path`.

Example:
```
$ python3 queryjson.py ~/jsonfiles/q9.opt+.json --stagestate
Sorted Queries  
s: stageId, p: planNode, o: operatorName, d:DriverCount  
execTime : 1.41m(84.6s)  
file : ~/jsonfiles/q9.opt+.json  
totalTasks : 77  
peakRunningTasks : 76  
totalCpuTime : 1.07d  
totalBlockedTime : 46.62m  
shuffledDataSize : 1.21MB  
Stage State  
{'Stage 0 : FINISHED'}  
{'Stage 1 : FINISHED'}  
{'Stage 2 : FINISHED'}  
Operators  
{'s': 1, 'p': '2', 'o': 'FilterProject', 'd': 1200, 'outputWall': '13.76h', 'inputWall': '7.86s', 'blockedWall': '0.00ns', 'outputRows': 275040303967}  
{'s': 1, 'p': '0', 'o': 'TableScanOperator', 'd': 1200, 'outputWall': '4.66h', 'inputWall': '0.00ns', 'blockedWall': '23.85s', 'outputRows': 275040303967}  
{'s': 1, 'p': '685', 'o': 'PartialAggregation', 'd': 1200, 'outputWall': '7.45s', 'inputWall': '8.03h', 'blockedWall': '0.00ns', 'outputRows': 1200} 
```
