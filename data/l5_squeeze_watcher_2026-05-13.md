# L5 — run_squeeze_watcher Wall Distribution 2026-05-13

## Wall samples (raw)
```
wall=48.825s

  Total samples: 1
```

## Finviz errors / timeouts
```
           your squeeze logic is a tactical error — Bollinger                   
```

## Productivity (candidates per scrape)
```
[Info] loading page [#############-----------------] 6/14 [Info] loading page [###############---------------] 7/14 [Info] loading page [#################-------------] 8/14 [Info] loading page [###################-----------] 9/14 [Info] loading page [#####################---------] 10/14 [Info] loading page [########################------] 11/14 [Info] loading page [##########################----] 12/14 [Info] loading page [############################--] 13/14 [08:07:03] Squeeze Scanner: 273 candidates from Finviz    squeeze_scanner.py:278
[Info] loading page [###############---------------] 7/14 [Info] loading page [#################-------------] 8/14 [Info] loading page [###################-----------] 9/14 [Info] loading page [#####################---------] 10/14 [Info] loading page [########################------] 11/14 [Info] loading page [##########################----] 12/14 [Info] loading page [############################--] 13/14 [09:07:59] Squeeze Scanner: 273 candidates from Finviz    squeeze_scanner.py:278
[Info] loading page [############################--] 13/14 [10:02:40] Squeeze Scanner: 273 candidates from Finviz    squeeze_scanner.py:278
[Info] loading page [############################--] 13/14 [10:58:18] Squeeze Scanner: 273 candidates from Finviz    squeeze_scanner.py:278
[Info] loading page [####--------------------------] 2/14 [Info] loading page [######------------------------] 3/14 [Info] loading page [#########---------------------] 4/14 [Info] loading page [###########-------------------] 5/14 [Info] loading page [#############-----------------] 6/14 [Info] loading page [###############---------------] 7/14 [Info] loading page [#################-------------] 8/14 [Info] loading page [###################-----------] 9/14 [Info] loading page [#####################---------] 10/14 [Info] loading page [########################------] 11/14 [Info] loading page [##########################----] 12/14 [Info] loading page [############################--] 13/14 [11:51:25] Squeeze Scanner: 273 candidates from Finviz    squeeze_scanner.py:278
[Info] loading page [###################-----------] 9/14 [Info] loading page [#####################---------] 10/14 [Info] loading page [########################------] 11/14 [Info] loading page [##########################----] 12/14 [Info] loading page [############################--] 13/14 [12:43:21] Squeeze Scanner: 273 candidates from Finviz    squeeze_scanner.py:278
[Info] loading page [#################-------------] 8/14 [Info] loading page [###################-----------] 9/14 [Info] loading page [#####################---------] 10/14 [Info] loading page [########################------] 11/14 [Info] loading page [##########################----] 12/14 [Info] loading page [############################--] 13/14 [13:29:55] Squeeze Scanner: 273 candidates from Finviz    squeeze_scanner.py:278
[Info] loading page [###################-----------] 9/14 [Info] loading page [#####################---------] 10/14 [Info] loading page [########################------] 11/14 [Info] loading page [##########################----] 12/14 [Info] loading page [############################--] 13/14 [14:34:30] Squeeze Scanner: 273 candidates from Finviz    squeeze_scanner.py:278
[Info] loading page [###########-------------------] 5/14 [Info] loading page [#############-----------------] 6/14 [Info] loading page [###############---------------] 7/14 [Info] loading page [#################-------------] 8/14 [Info] loading page [###################-----------] 9/14 [Info] loading page [#####################---------] 10/14 [Info] loading page [########################------] 11/14 [Info] loading page [##########################----] 12/14 [Info] loading page [############################--] 13/14 [15:27:23] Squeeze Scanner: 273 candidates from Finviz    squeeze_scanner.py:278
[Info] loading page [#################-------------] 8/14 [Info] loading page [###################-----------] 9/14 [Info] loading page [#####################---------] 10/14 [Info] loading page [########################------] 11/14 [Info] loading page [##########################----] 12/14 [Info] loading page [############################--] 13/14 [16:25:55] Squeeze Scanner: 273 candidates from Finviz    squeeze_scanner.py:278
```

## Hypothesis interpretation

- Wall >40s consistently → Hypothesis B (yfinance per-symbol hydration)
  → Migrate to Polygon batch endpoints; expected 5-10s
- Wall <10s → No action
- Variable + Finviz timeouts → Hypothesis A (HTTP latency)
  → Async parallel fetches, or migrate source
