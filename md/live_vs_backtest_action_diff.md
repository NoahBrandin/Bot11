# Live vs. Backtest Action Diff — Bot11's Last Run

Covers the bot's actual last live run only: **2026-08-22T18:53:37Z to 2026-08-23T22:49:01Z UTC** (~27h56m), confirmed via `journalctl` service start/stop events on `bot11-stockholm`. This is narrower than some numbers quoted earlier in this investigation, which spanned several restarts earlier on 2026-08-22 -- see "Scope correction" at the end.

Raw data:
- Filtered live journal (httpx noise stripped): `other_src/data/live_run_2026-08-22_to_08-23_log.jsonl` (5268 lines)
- Real historical BTC + Polymarket data for this exact span, fetched fresh (not the 30-day backtest set): `other_src/data/backtest_data_live_run_window.json` (1781 klines, 100801 1s ticks, 336/336 resolved windows, 0 skipped)
- Backtest replay of that data through the current on-disk strategy code: `other_src/data/backtest_live_run_window_trades.jsonl` (745 order records), `other_src/data/backtest_live_run_window_analysis.json`
- Strategy config verified matching between live `.env` and the backtest defaults before running: `STRATEGY_HISTORY_SIZE=100`, `STRATEGY_PROBABILITY_MARGIN=0.02`, `STRATEGY_KELLY_MULTIPLIER=0.5`, `DEFAULT_EWMA_HALFLIFE_SECONDS=30.0` -- all match.

## Headline finding: the bot went silent for the last 17 of its 28 hours

This is the dominant driver of the live/backtest gap over this run, and it isn't a probability-model accuracy issue at all -- it's a participation collapse.

| hour of run | windows with an order attempt |
|---|---|
| 0-3 | 11,12,12,12 /12 (92-100%) |
| 4 | 5/12 (42%) |
| 5 | 1/12 (8%) |
| 6-8 | 12,12,12 /12 (100%) |
| 9-10 | 8/12, 9/12 (67-75%) |
| **11-27** | **0/12 every single hour (0%)** |

The last order attempt of any kind (BUY or SELL, filled or not) fired at **2026-08-23T05:36:00Z**. From then until the process was stopped via Telegram at 22:49:01Z the next day -- **204 consecutive 5-minute windows, ~17 hours** -- it placed zero orders.

This was not a crash, a pause, or a data outage:
- `systemctl` shows the process running continuously the whole time (no restart between 18:53:37 and 22:49:01).
- No `/pause` command appears anywhere in the log (searched for pause/resume across the full run -- none).
- `Window opened`/`Window closed` events continued firing correctly for all 336 windows, `skip_trading` was `True` for exactly 1 of them.
- `strategy.manager` kept logging `Outcome:` probability ticks continuously and normally throughout the silent period -- sampled hour 12 (deep in the silent zone) directly: probabilities of 0.9999996, 0.9999999977, 0.9630556, 0.9999999998 all appear within a few minutes of each other, comfortably past the 0.52 entry line, with bid/ask quotes visibly changing window to window (not frozen/stale).
- Across the whole run, `Outcome:` ticks cleared the 0.52 entry line on **49.5%** of all readings (1659/3350) -- the underlying signal was firing constantly, including deep into the silent period.

So: Binance klines kept arriving, Polymarket quotes kept arriving and updating, the GBM/EWMA probability estimate kept computing high-confidence values that repeatedly crossed the entry threshold -- and none of it ever turned into a submitted order. Something between "strategy decides it wants a position" and "`execution.converge()` places an order" stopped functioning, silently, without the crash-handling path (`orchestrator.py::run()`'s `notify_and_wait`) or any error-level log ever firing to say so.

**One suggestive but unconfirmed coincidence**: a `datastream.polymarket_feed: Polymarket feed connection error, reconnecting` fired at `05:37:05Z`, ~65 seconds after the last successful order. 12 more of the same reconnect error occurred sporadically over the following 17 hours (not a flood -- roughly one every 1-2 hours). But the market-data pipeline itself clearly kept working after each reconnect (quotes and probabilities both stayed live and sane), so if the reconnect is implicated, it's not simply "the feed died" -- something more specific broke in the signal-to-order path and never recovered on subsequent reconnects either. **This needs a direct code read of the signal-emission path in `strategy/manager.py` (past the excerpt already reviewed this session) and how it's wired to `execution.converge()` -- out of scope for this diff, but it's now the single highest-priority lead in this investigation, well above the probability-noise hypothesis.** This is exactly the blind spot `md/monitoring_review.md` already called out under "Kein Dead-Man's-Switch": the process can be `active (running)` and still be functionally dead, with nothing watching for it.

## Participation and direction numbers

| metric | value |
|---|---|
| Total windows in span | 336 |
| Windows live entered (>=1 filled BUY) | 104 |
| Windows backtest entered | 335 |
| Both entered | 103 |
| Live-only | 1 |
| Backtest-only | 232 |
| Live positions (outcome-level) | 140 |
| Backtest positions (outcome-level) | 445 |

Backtest, replaying the identical strategy code against the same real market data with zero execution friction, trades **~3.2x more positions** than live did (445 vs 140) and touches essentially every window (335/336) vs live's 104/336 (31%). Given the 17-hour silent stretch covers roughly 204 of the 336 windows on its own, that gap is mostly explained by the finding above, not by the strategy disagreeing with itself about direction.

**Direction agreement** (for the 103 windows both sides entered): **66.0%** agreement (68 agree, 35 disagree) on which outcome(s) got entered. Disagreements are a mix of live entering only one side where backtest entered both (or vice versa) and occasional outright opposite calls -- e.g. `btc-updown-5m-1787458500`: live entered UP, backtest entered DOWN.

**Entry timing** (for the 119 matched slug+outcome pairs both sides entered): live enters **40.1s earlier on average** than backtest for the same position (median -59.1s), stdev 75.7s. Live has a real-time information/latency edge here (no replay batching), so entering slightly earlier than a batch replay is expected and not concerning on its own.

**Exit behavior**: live held to expiry on 49.3% of its positions (69/140) vs backtest's 53.3% (237/445) -- close enough that hold-rate itself isn't the story. Among the 119 matched pairs, live and backtest agreed on hold-vs-sell-early 76.5% of the time.

**Probability-band concentration** (does divergence cluster near the 0.5-0.6 entry margin, as the standing hypothesis in `md/performance_review_backtest.md` suggested?): **No** -- entries essentially never happen there on either side. Only 1.4% of live entries (2/140) and 1.8% of backtest entries (8/445) had an entry probability in [0.5, 0.6]; the overwhelming majority of entries on both sides cluster above 0.9 probability. This is worth noting as a mild **update against** the "live's noisier near-band probability estimate explains the gap" theory from the backtest review -- both live and backtest are almost always deciding to enter only once the signal is already very confident, so noise right at the entry line isn't where the actual entries (or their disagreements) are concentrated. (Note: the "win rate on matched pairs" being identical at 75.6% for both sides is expected/tautological, not an independent finding -- a matched pair is by definition the same real window and outcome, so it resolves identically for both.)

## Order failures within this exact run

46 of live's 277 order attempts failed (16.6%) -- all 46 were `"no orders found to match with FAK order"` (37 BUY, 9 SELL). Zero `"not enough balance"` and zero `"trading is disabled"` failures occurred **within this specific run's boundaries**.

## Scope correction

Earlier in this investigation (before the exact run boundaries were confirmed via `journalctl` start/stop events), order-failure statistics were computed over `--since "2026-08-22 00:00:00"`, which unintentionally spans several earlier restarts that same day (the service restarted 4 times between 20:54 UTC on 2026-08-21 and 18:53:37 UTC on 2026-08-22, before settling into the run this report covers). Two things reported earlier as part of "the last run" actually belong to those earlier, shorter-lived restarts, not this one:
- The 372/1172 order-failure-rate figure and the "not enough balance" position-tracking bug (already fixed in `server_src/execution/base.py`) were both real, but measured across that wider multi-run window.
- The `"trading is disabled"` cluster starting at 2026-08-22T13:17:00Z falls entirely before this run started (18:53:37Z) -- it belongs to one of the earlier, shorter restarts that same day, not this run.

Both bugs are still real and the fix already applied stands on its own merits; this note is only to correct which run each stat was measured over.

## Follow-up: is the 34% direction disagreement caused by live-vs-backtest price differences?

**Short answer: a real, near-universal reference-price discrepancy exists, but it does not explain which windows disagree -- it's present almost everywhere, agreeing and disagreeing windows alike.**

### The hypothesis

Each window's `target_price` (the strike BTC has to move away from) is captured once, from whichever Binance 1-minute candle close is available in `self._current_price` at the moment the window opens (`strategy/manager.py::_resolve_live_reference_price`). The candle that closes exactly at `window_start` is, in principle, available at that instant. The hypothesis: live's real-world Polymarket-window-open detection (Gamma polling) might consistently beat Binance's real WebSocket kline delivery for that same minute, so live ends up using the *prior* minute's close almost every time, while a backtest replaying pre-recorded, perfectly-timestamped data would not have that gap.

### What the data actually shows

Joining all 336 `"Window opened: ... reference_price=X"` lines from the live log against the exact-range Binance klines (`other_src/data/backtest_data_live_run_window.json`): live's logged `target_price` matches the candle closing **exactly at `window_start`** in only 7/336 windows (2.1%). It matches the **prior** candle (`window_start - 60s`) in 328/336 (97.6%). This confirms live is essentially always working with one candle less information than the theoretically-freshest price -- a real, large, and previously undocumented discrepancy from the "live and backtest see the same market data" assumption baked into every prior comparison in this investigation. Typical magnitude: mean |live_reference_price - at-open close| = $26.28 across all windows, up to $404.80 on the single largest outlier.

Cross-referencing against the direction-agreement analysis: this mismatch is present in **95.6%** of the 68 windows where live and backtest *agreed* on direction, and **100%** of the 4 windows where they were fully disjoint. Since the baseline rate across all 336 windows is already 97.9%, that's not a meaningful lift -- the mismatch is close to a constant background condition, not something that selectively explains disagreement. The one directionally-suggestive (not statistically solid, n=4) signal: the 4 fully-disjoint windows average a **larger** mismatch magnitude ($56.14 mean, $49.59 median) than the 68 agreeing windows ($29.53 mean, $14.97 median) -- consistent with "when the stale-candle gap happens to be unusually large, it's more likely to flip which side clears the entry line," but far too small a sample to call confirmed.

### Attempted fix, and why it didn't work

The natural first fix: in `backtest/engine.py`, the merge-sort of same-window events orders `_KLINE_PRIORITY` (0) before `_WINDOW_OPEN_PRIORITY` (1), so a kline landing at the same timestamp as `window_start` is processed first, handing the backtest the freshest possible price. Swapping the priority so window-open is evaluated first (deferring that kline to the very next tick, matching manager.py's existing "reference_price captured late" fallback) was implemented and the backtest re-run in full (`--trade-log-output` / `--analysis-output` regenerated).

**Result: zero change.** Identical position counts (445 backtest positions, 335 windows entered) and identical direction-agreement figures (66.0% exact, 96.1% overlap, 3.9% disjoint) before and after. Root cause of the no-op: Binance's official kline `close_time` convention is always `window_start - 0.001` (1ms before the minute boundary, confirmed directly against the downloaded data), never exactly equal to `window_start`. The event merge-sort's primary key is the raw timestamp, with priority only used as a tiebreaker for genuine ties -- and since this timestamp is always strictly earlier by construction, the kline was *already* winning the sort on timestamp alone, both before and after the priority swap. The fix targeted a tiebreak mechanism that was never actually deciding the outcome. **This change was reverted** (`other_src/backtest/engine.py` is back to its pre-session state) rather than leaving a fix in the tree that does nothing but claims otherwise; the redundant `_v2` backtest output files were deleted for the same reason.

### What an actual fix would need

The real gap isn't event *ordering* at all -- it's that the backtest models zero delivery latency for both Gamma window-open detection and Binance kline delivery, while live has real, asymmetric network latency between the two that empirically resolves the same way almost every time. A working fix would need to either (a) explicitly force the window-start-coincident kline to not count as "available" at window-open regardless of timestamp -- e.g. decouple the event list's sort key from `WindowOpenEvent.timestamp`/`clock.set()` value so `window_start` can be nudged a few milliseconds earlier for sorting purposes only, without perturbing the simulated clock -- or (b) explicitly special-case reference_price capture in the backtest to always prefer the candle before `window_start` when one exists, mirroring the empirical 97.6% live behavior directly rather than trying to derive it from event ordering. Neither was implemented this session; this is the concrete next step for whoever picks this up.

### Bottom line for the original question

The reference-price staleness is real, large, and worth fixing for backtest realism in its own right (it likely inflates backtest's apparent edge somewhat, independent of the disagreement question, since backtest usually operates on marginally fresher information than live ever gets). But it is **not** a good explanation for *which* windows disagree -- it's nearly ubiquitous regardless of agreement. The direction-disagreement mystery (66% exact agreement, 3.9% fully opposite) remains open; the next most promising lead, per the magnitude-correlation hint above, is whether unusually *large* single-candle BTC moves (not the mismatch's mere presence) are what's actually flipping decisions -- worth a dedicated pass matching disagreement-window magnitude against typical per-minute BTC volatility, independent of the reference-price-source question addressed here.
