# presto-queryjson-parser
A Python parser to read Presto query json files
Given a path to a query json file or a folder with query json files, the parser
1) Filters failed queries.
2) Sorts queries from slowest to fastest.
3) Optionally prints the state of the stages

Usage:

python queryjson.py --path ~/jsonfiles/q64.json --stagestate=True

python queryjson.py --path ~/jsonfiles
