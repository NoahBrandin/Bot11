# Live Performance Review

Generated 2026-08-22 via `other_src/performance_review.py --no-chart`, against the wallet configured in `other_src/.env` (`POLYMARKET_FUNDER_ADRESS`/`POLYMARKET_FUNDER_ADDRESS`). Read-only report over Polymarket's public Data API -- no private key involved.

```
Polymarket Performance Review
Wallet: 0x7aB26527F77D82ba7CF8f731694CEF1f1B447048
============================================================

-- Portfolio Value --
  Current value: $0.00

-- Trade Status --
  Positions decided: 90  Still active: 0
  Not yet sold (still active): 0.0%

-- Realized P&L (Closed Positions) --
  Closed positions: 90  wins=28  losses=62  breakeven=0
  Win rate: 31.1%
  Directional accuracy: 37.8% (34/90)
  Total realized P&L: $-50.53
  Avg P&L: $-0.56   Median P&L: $-1.00
  Avg win: $4.56   Avg loss: $-2.88
  Profit factor: 0.72
  Capital deployed: $752.24
  Return on capital: -6.7%
  Avg entry price: 0.419  (closer to 1.0 = favorites, closer to 0.0 = longshots)

-- Open Positions (Active, Unresolved) --
  No active positions.

-- Pending On-Chain Redemption --
  30 resolved position(s) need redeemPositions() -- 0 won and worth $0.00 unclaimed, 30 lost (nothing to claim, just dust).

-- Closed Positions by Exit Method --
  (each row is accuracy within that group alone -- two independent rates, not a 100% split)
  Sold early: 59  directionally right: 33.9% (20/59)
  Held to expiry: 31  directionally right: 45.2% (14/31)

-- First-Trade Direction (by window) --
  First trade called it right: 35.3% (30/85 resolved windows)

-- Direction Accuracy by Time-Into-Window (1min buckets) --
  (sold early/held to expiry are each accuracy within that group alone -- not a 100% split)
  minute 0-1:
    direction accuracy: 49.0% (48/98)
    avg win: $4.15   avg loss: $-3.49   avg pnl: $-0.29
    volume: $418.76   avg entry: 0.475
    sold early: 44.2% (38/86)   held to expiry: 83.3% (10/12)
    pnl per sell: $-0.66
  minute 1-2:
    direction accuracy: 48.0% (12/25)
    avg win: $3.33   avg loss: $-3.64   avg pnl: $-1.97
    volume: $105.07   avg entry: 0.508
    sold early: 50.0% (12/24)   held to expiry: 0.0% (0/1)
    pnl per sell: $-0.97
  minute 2-3:
    direction accuracy: 26.7% (4/15)
    avg win: $3.89   avg loss: $-3.28   avg pnl: $-2.32
    volume: $49.42   avg entry: 0.451
    sold early: 25.0% (3/12)   held to expiry: 33.3% (1/3)
    pnl per sell: $-0.66
  minute 3-4:
    direction accuracy: 30.0% (6/20)
    avg win: $4.41   avg loss: $-3.19   avg pnl: $-1.67
    volume: $66.12   avg entry: 0.385
    sold early: 30.8% (4/13)   held to expiry: 28.6% (2/7)
    pnl per sell: $-0.75
  minute 4-5:
    direction accuracy: 17.4% (4/23)
    avg win: $9.93   avg loss: $-2.46   avg pnl: $-0.31
    volume: $66.58   avg entry: 0.221
    sold early: 14.3% (2/14)   held to expiry: 22.2% (2/9)
    pnl per sell: $-0.04

-- Recent Trades (up to 20) --
    2026-08-22 01:25  SELL Bitcoin Up or Down - August 21, 9:2 outcome=Down  size=    5.92 @ $0.240
    2026-08-22 01:25  BUY  Bitcoin Up or Down - August 21, 9:2 outcome=Down  size=    5.93 @ $0.410
    2026-08-22 01:05  SELL Bitcoin Up or Down - August 21, 9:0 outcome=Down  size=   17.87 @ $0.190
    2026-08-22 01:05  BUY  Bitcoin Up or Down - August 21, 9:0 outcome=Down  size=   10.88 @ $0.330
    2026-08-22 01:05  BUY  Bitcoin Up or Down - August 21, 9:0 outcome=Down  size=    7.00 @ $0.411
    2026-08-22 01:00  SELL Bitcoin Up or Down - August 21, 9:0 outcome=Down  size=    6.61 @ $0.480
    2026-08-22 01:00  BUY  Bitcoin Up or Down - August 21, 9:0 outcome=Down  size=    5.38 @ $0.450
    2026-08-22 01:00  BUY  Bitcoin Up or Down - August 21, 9:0 outcome=Down  size=    5.15 @ $0.470
    2026-08-22 00:53  SELL Bitcoin Up or Down - August 21, 8:5 outcome=Down  size=   11.14 @ $0.120
    2026-08-22 00:52  BUY  Bitcoin Up or Down - August 21, 8:5 outcome=Down  size=    5.34 @ $0.410
    2026-08-22 00:51  SELL Bitcoin Up or Down - August 21, 8:5 outcome=Down  size=   17.66 @ $0.430
    2026-08-22 00:51  SELL Bitcoin Up or Down - August 21, 8:5 outcome=Down  size=    5.66 @ $0.490
    2026-08-22 00:51  BUY  Bitcoin Up or Down - August 21, 8:5 outcome=Down  size=    9.43 @ $0.470
    2026-08-22 00:51  BUY  Bitcoin Up or Down - August 21, 8:5 outcome=Down  size=    9.84 @ $0.450
    2026-08-22 00:51  BUY  Bitcoin Up or Down - August 21, 8:5 outcome=Down  size=    9.84 @ $0.450
    2026-08-22 00:50  SELL Bitcoin Up or Down - August 21, 8:5 outcome=Down  size=    6.52 @ $0.240
    2026-08-22 00:50  SELL Bitcoin Up or Down - August 21, 8:5 outcome=Down  size=   14.95 @ $0.440
    2026-08-22 00:50  BUY  Bitcoin Up or Down - August 21, 8:5 outcome=Down  size=   11.00 @ $0.400
    2026-08-22 00:50  BUY  Bitcoin Up or Down - August 21, 8:5 outcome=Down  size=   10.48 @ $0.420
    2026-08-22 00:45  BUY  Bitcoin Up or Down - August 21, 8:4 outcome=Down  size=    4.93 @ $0.460
```
