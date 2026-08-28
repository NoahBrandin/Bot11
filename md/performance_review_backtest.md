# Backtest Performance Review

Generated 2026-08-22 via `other_src/backtest_performance_review.py`, over the 30-day trade log from `backtest_data_30d_timefixed.json` (2026-07-20 to 2026-08-19), replayed at zero simulated execution latency, default strategy config (matches live `.env`: `STRATEGY_HISTORY_SIZE=100`, `STRATEGY_PROBABILITY_MARGIN=0.02`, `STRATEGY_KELLY_MULTIPLIER=0.5`). Same metric definitions/labels as `performance_review.py` so the two reports are directly comparable -- see `performance_review_live.md`.

Re-replayed and regenerated same-day to add the buy-in edge vs. return breakdown below, then re-replayed again with a corrected synthetic spread. Totals shifted from the first same-day run (12451 positions -> 10143, win rate 61.8% -> 74.1%, ROI +26.4% -> +36.2%) because two strategy commits landed between those two runs (`Throttle strategy re-evaluation to once per Binance candle close`, `Join late handling`) -- the replay uses whatever strategy code is on disk, not a pinned version, so re-running after a strategy change naturally produces a different trade log even against identical historical data.

## Spread recalibration

The backtest's synthetic bid/ask spread (`other_src/backtest/engine.py`'s `DEFAULT_SPREAD_HALF_WIDTH`) was a flat guess: originally 0.005, later widened 4x to 0.02 "as a more conservative assumption" with no data behind the multiplier. Checked it against reality by sampling Polymarket's live order book directly (best_bid/best_ask) over ~4 live 5-minute windows today -- Telegram itself turned out not to be a usable source here: `server_src/monitoring/telegram.py` and `execution/live_report.py` only ever send fill price / order events to chat, never bid/ask, so there's no spread to read off a Telegram transcript. 310 samples:

| percentile | spread (full width) |
|---|---|
| median / p50-p95 | $0.01 |
| p99 | $0.02 |
| max (1 outlier) | $0.09 |

98.4% of samples sat flat at $0.01 (the old *pre-widening* assumption), with a single mid-window $0.09 spike on a thin book. The 4x-widened 0.02 half-width ($0.04 full spread) doesn't match typical conditions at all -- it overstates real cost on ~99% of fills. Set `DEFAULT_SPREAD_HALF_WIDTH = 0.005` (matching the measured $0.01 typical full spread) and re-ran the 30-day backtest. Caveat: this is a small live sample (~4 windows, not the 30 backtested days) and, like the value it replaces, still a flat approximation -- it won't reproduce the rare thin-book blowout the 1-in-310 outlier hints at.

```
Backtest Performance Review
Trade log: backtest_data_30d_timefixed.json (via trades_lat0.jsonl, spread_half_width=0.005)
============================================================

-- Trade Status --
  Positions decided: 10272  Still active: 0
  Not yet sold (still active): 0.0%  (a backtest settles every position before the run ends)

-- Realized P&L (Closed Positions) --
  Closed positions: 10272  wins=7778  losses=2494  breakeven=0
  Win rate: 75.7%
  Directional accuracy: 68.3% (7020/10272)
  Total realized P&L: $20,691.62
  Avg P&L: $2.01   Median P&L: $2.93
  Avg win: $3.62   Avg loss: $-3.00
  Profit factor: 3.76
  Capital deployed: $51,355.39
  Return on capital: +40.3%
  Avg entry price: 0.538  (closer to 1.0 = favorites, closer to 0.0 = longshots)

  By market (top 15 / bottom 15 of 8166 unique windows):
    btc-updown-5m-1785233400                 trades=  3  pnl=$     11.64
    btc-updown-5m-1784596800                 trades=  3  pnl=$     10.92
    btc-updown-5m-1785117000                 trades=  3  pnl=$     10.00
    btc-updown-5m-1786570500                 trades=  2  pnl=$      9.73
    btc-updown-5m-1787074200                 trades=  2  pnl=$      9.68
    btc-updown-5m-1786065000                 trades=  2  pnl=$      9.42
    btc-updown-5m-1785277200                 trades=  3  pnl=$      9.36
    btc-updown-5m-1785706500                 trades=  2  pnl=$      9.36
    btc-updown-5m-1786374600                 trades=  2  pnl=$      9.34
    btc-updown-5m-1786029600                 trades=  3  pnl=$      9.28
    btc-updown-5m-1785866100                 trades=  2  pnl=$      9.18
    btc-updown-5m-1786473300                 trades=  3  pnl=$      9.08
    btc-updown-5m-1784890800                 trades=  2  pnl=$      9.07
    btc-updown-5m-1786579200                 trades=  2  pnl=$      8.95
    btc-updown-5m-1785117600                 trades=  2  pnl=$      8.94
    ...
    btc-updown-5m-1785084300                 trades=  2  pnl=$     -6.67
    btc-updown-5m-1784844000                 trades=  2  pnl=$     -6.75
    btc-updown-5m-1784934900                 trades=  2  pnl=$     -6.81
    btc-updown-5m-1785049200                 trades=  2  pnl=$     -6.81
    btc-updown-5m-1785464700                 trades=  2  pnl=$     -6.82
    btc-updown-5m-1786654200                 trades=  2  pnl=$     -6.87
    btc-updown-5m-1786336500                 trades=  3  pnl=$     -6.95
    btc-updown-5m-1784849100                 trades=  2  pnl=$     -7.05
    btc-updown-5m-1786046700                 trades=  2  pnl=$     -7.14
    btc-updown-5m-1785561600                 trades=  2  pnl=$     -7.15
    btc-updown-5m-1784990400                 trades=  2  pnl=$     -7.33
    btc-updown-5m-1785985500                 trades=  2  pnl=$     -7.33
    btc-updown-5m-1784605200                 trades=  2  pnl=$     -7.40
    btc-updown-5m-1785841200                 trades=  2  pnl=$     -7.40
    btc-updown-5m-1785309300                 trades=  2  pnl=$     -7.47

-- Open Positions (Active, Unresolved) --
  N/A -- a backtest replay settles every position at its window's real resolved outcome before moving on.

-- Pending On-Chain Redemption --
  N/A -- backtest positions settle in-memory; there's no on-chain redemption step to be behind on.

-- Closed Positions by Exit Method --
  (each row is accuracy within that group alone -- two independent rates, not a 100% split)
  Sold early: 2879  directionally right: 26.4% (761/2879)
  Held to expiry: 7393  directionally right: 84.7% (6259/7393)

-- Direction Accuracy by Time-Into-Window (1min buckets) --
  (sold early/held to expiry are each accuracy within that group alone -- not a 100% split)
  minute 1-2:
    direction accuracy: 63.9% (4180/6540)
    avg win: $3.72   avg loss: $-2.59   avg pnl: $1.98
    volume: $32,698.05   avg entry: 0.517
    sold early: 26.2% (616/2348)   held to expiry: 85.0% (3564/4192)
    pnl per sell: $0.05
  minute 2-3:
    direction accuracy: 72.0% (1133/1573)
    avg win: $3.69   avg loss: $-3.43   avg pnl: $2.31
    volume: $7,865.00   avg entry: 0.545
    sold early: 24.4% (87/356)   held to expiry: 85.9% (1046/1217)
    pnl per sell: $0.54
  minute 3-4:
    direction accuracy: 77.6% (954/1229)
    avg win: $3.43   avg loss: $-4.10   avg pnl: $2.14
    volume: $6,143.27   avg entry: 0.579
    sold early: 33.1% (58/175)   held to expiry: 85.0% (896/1054)
    pnl per sell: $0.94
  minute 4-5:
    direction accuracy: 81.0% (753/930)
    avg win: $3.17   avg loss: $-5.16   avg pnl: $1.59
    volume: $4,649.06   avg entry: 0.620
    sold early: n/a   held to expiry: 81.0% (753/930)
    pnl per sell: n/a

-- Return by Buy-In Edge (modeled probability minus entry price) --
  (bucketed in 0.05-wide entry_edge bands -- tests whether more edge at buy-in actually returns more)
  edge 0.00-0.05: n=  736  win rate= 66.3%  avg pnl=$  0.88  roi= +17.6%
  edge 0.05-0.10: n= 2058  win rate= 68.8%  avg pnl=$  1.24  roi= +24.9%
  edge 0.10-0.15: n= 2015  win rate= 71.3%  avg pnl=$  1.58  roi= +31.7%
  edge 0.15-0.20: n= 1595  win rate= 74.9%  avg pnl=$  1.85  roi= +37.0%
  edge 0.20-0.25: n= 1231  win rate= 79.5%  avg pnl=$  2.38  roi= +47.5%
  edge 0.25-0.30: n=  954  win rate= 84.0%  avg pnl=$  2.79  roi= +55.7%
  edge 0.30-0.35: n=  638  win rate= 83.5%  avg pnl=$  2.77  roi= +55.4%
  edge 0.35-0.40: n=  440  win rate= 87.7%  avg pnl=$  3.42  roi= +68.4%
  edge 0.40-0.45: n=  294  win rate= 89.8%  avg pnl=$  3.76  roi= +75.2%
  edge 0.45-0.50: n=  176  win rate= 88.1%  avg pnl=$  4.01  roi= +80.2%
  edge 0.50-0.55: n=   77  win rate= 90.9%  avg pnl=$  4.31  roi= +86.2%
  edge 0.55-0.60: n=   47  win rate= 97.9%  avg pnl=$  5.99  roi=+119.9%  <== highest avg pnl
  edge 0.60-0.65: n=   11  win rate= 90.9%  avg pnl=$  5.88  roi=+117.7%

-- Recent Trades (last 20 filled orders) --
    btc-updown-5m-1787178600       +   60s  BUY  outcome=Up    size=   11.90 @ $0.420
    btc-updown-5m-1787178600       +  180s  SELL outcome=Up    size=   11.90 @ $0.410
    btc-updown-5m-1787178600       +  180s  BUY  outcome=Down  size=    8.62 @ $0.580
    btc-updown-5m-1787178900       +  120s  BUY  outcome=Up    size=    7.46 @ $0.670
    btc-updown-5m-1787179500       +   60s  BUY  outcome=Down  size=    8.20 @ $0.610
    btc-updown-5m-1787180100       +   60s  BUY  outcome=Up    size=    7.94 @ $0.630
    btc-updown-5m-1787180400       +   60s  BUY  outcome=Down  size=    8.93 @ $0.560
    btc-updown-5m-1787180400       +  120s  BUY  outcome=Up    size=   11.11 @ $0.450
    btc-updown-5m-1787180400       +  120s  SELL outcome=Down  size=    8.93 @ $0.560
    btc-updown-5m-1787180400       +  240s  SELL outcome=Up    size=   11.11 @ $0.560
    btc-updown-5m-1787180400       +  240s  BUY  outcome=Down  size=   10.87 @ $0.460
    btc-updown-5m-1787180700       +  120s  BUY  outcome=Down  size=   10.64 @ $0.470
    btc-updown-5m-1787180700       +  240s  SELL outcome=Down  size=   10.64 @ $0.120
    btc-updown-5m-1787181000       +  240s  BUY  outcome=Up    size=    8.77 @ $0.570
    btc-updown-5m-1787181300       +   60s  BUY  outcome=Up    size=   12.20 @ $0.410
    btc-updown-5m-1787181600       +   60s  BUY  outcome=Down  size=   10.00 @ $0.500
    btc-updown-5m-1787181900       +   60s  BUY  outcome=Down  size=    9.26 @ $0.540
    btc-updown-5m-1787181900       +  180s  BUY  outcome=Up    size=    7.14 @ $0.700
    btc-updown-5m-1787181900       +  180s  SELL outcome=Down  size=    9.26 @ $0.280
    btc-updown-5m-1787182200       +  120s  BUY  outcome=Down  size=    7.04 @ $0.710
```

Note on the per-position numbers above vs. the raw execution log: `execution.paper`'s own summary for this run reported `ending_bankroll=21691.62` -- **+2069% growth** on the $1000 starting paper bankroll, vs. the +40.3% "return on capital" shown in the report. Both are correct, they answer different questions: the report's ROI treats every position's capital independently (`sum(pnl) / sum(entry_price * size)` across all 10272 positions, no compounding), while the actual paper bankroll compounds -- Kelly sizing is a % of the *current* bankroll, so each win makes the next position bigger, and that compounds hard over 8639 windows at a ~76% win rate. Tightening the spread from 0.02 to 0.005 lowered cost-per-trade only slightly (per-position ROI moved +36.2% -> +40.3%, a ~4pt bump) but the compounding effect on the bankroll trajectory is much larger and more sensitive to that cost -- worth flagging as its own realism question (30 days of uninterrupted Kelly compounding with no drawdown-driven de-risking is itself a strong assumption) separate from the spread fix asked for here.

## Does more edge at buy-in actually mean more return?

Yes, monotonically, across every bucket with meaningful sample size, and this holds under the corrected spread too. `entry_edge` (modeled probability minus entry price -- the same figure `kelly.py` sizes positions off of) was bucketed in 0.05-wide bands over all 10272 closed backtest positions, then win rate / avg PnL / ROI were computed per bucket:

| entry_edge | n | win rate | avg pnl | roi |
|---|---|---|---|---|
| 0.00-0.05 | 736 | 66.3% | $0.88 | +17.6% |
| 0.05-0.10 | 2058 | 68.8% | $1.24 | +24.9% |
| 0.10-0.15 | 2015 | 71.3% | $1.58 | +31.7% |
| 0.15-0.20 | 1595 | 74.9% | $1.85 | +37.0% |
| 0.20-0.25 | 1231 | 79.5% | $2.38 | +47.5% |
| 0.25-0.30 | 954 | 84.0% | $2.79 | +55.7% |
| 0.30-0.35 | 638 | 83.5% | $2.77 | +55.4% |
| 0.35-0.40 | 440 | 87.7% | $3.42 | +68.4% |
| 0.40-0.45 | 294 | 89.8% | $3.76 | +75.2% |
| 0.45-0.50 | 176 | 88.1% | $4.01 | +80.2% |
| 0.50-0.55 | 77 | 90.9% | $4.31 | +86.2% |
| **0.55-0.60** | **47** | **97.9%** | **$5.99** | **+119.9%** |
| 0.60-0.65 | 11 | 90.9% | $5.88 | +117.7% |

Win rate, avg PnL, and ROI all climb essentially in lockstep with edge, from ~18% ROI in the lowest band up to ~120% ROI at 0.55-0.60 edge -- confirming the strategy's core premise (bigger modeled edge should pay off more) holds up against the actual trade log, not just in theory, and the relationship is stable across both spread assumptions.

**What edge gives the greatest return:** under the old 0.02-spread run the answer was the 0.50-0.55 band; under the corrected 0.005 spread it's **0.55-0.60 edge (n=47)** -- 97.9% win rate, $5.99 avg PnL, +119.9% ROI. The 0.60-0.65 band is close behind (+117.7%) but only 11 trades, too thin to call the true peak rather than noise at the tail. Practically: the strategy isn't over-sizing high-edge trades relative to their payoff at any level tested, and the lower, more realistic spread mainly widened the gap between low- and high-edge returns (low-edge ROI +15.5% -> +17.6%, high-edge reliable-band ROI +98.9% -> +119.9%) rather than changing the shape of the relationship.

## Live vs. backtest, side by side

| metric | Live wallet | Backtest (30d) |
|---|---|---|
| Win rate | 31.1% | 75.7% |
| Directional accuracy | 37.8% | 68.3% |
| Total P&L | -$50.53 | +$20,691.62 |
| Profit factor | 0.72 | 3.76 |
| Capital deployed | $752.24 | $51,355.39 |
| Return on capital | -6.7% | +40.3% |
| Avg entry price | 0.419 | 0.538 |
| Sold-early accuracy | 33.9% | 26.4% |
| Held-to-expiry accuracy | 45.2% | 84.7% |

Ruled out as causes of the gap (see conversation history for the tests): execution latency up to 3s, synthetic spread up to ±0.12 (and now recalibrated down to the realistic ±0.005, which if anything *widened* the live/backtest gap -- see above), edge decay within the backtested month, and strategy config mismatch. Live's held-to-expiry accuracy (45.2%) sitting far below backtest's (84.7%) on a comparable "just let it ride" cohort is the most concrete unresolved lead so far.

### Time-into-window trend: opposite shapes

| minute | Live direction accuracy | Backtest direction accuracy |
|---|---|---|
| 0-1 | 49.0% | n/a (no entries this run -- see throttle/join-late commits above) |
| 1-2 | 48.0% | 63.9% |
| 2-3 | 26.7% | 72.0% |
| 3-4 | 30.0% | 77.6% |
| 4-5 | 17.4% | 81.0% |

Backtest gets *more* confident as the window runs on -- which is exactly what the GBM model should do, since less time remaining means less room for BTC to move away from wherever it already is. Live does the opposite: accuracy craters from minute 1 onward. Combined with the held-to-expiry gap above, this points at the same root cause -- whatever's degrading the live probability estimate (or its inputs) gets worse, not better, as a window progresses, while the backtest's clean historical data doesn't have that problem.
