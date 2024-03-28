# presto-queryjson-parser
A Python parser to read Presto query json files
Given a folder with query json files, the parser filters
1) Failed queries.
2) Queries taking more than 2 minutes and operators taking more than an hour CPU output wall time.
3) Sorts queries from slowest to fastest.
