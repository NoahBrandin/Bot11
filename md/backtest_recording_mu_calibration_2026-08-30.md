# Backtest-Neuentwicklung + mu-Kalibrierungs-Exploration

Stand: 2026-08-30

Ausgangspunkt: der Recorder (`server_src/utils/record_datastream.py`) hatte
zu diesem Zeitpunkt ~35h Live-Rohdaten aufgezeichnet
(`data/recorder_logs_2026-08-30/datastream.jsonl*`, 421 Fenster, 975.971
Events). Das alte Backtest-Setup (`other_src/backtest/download.py` +
`engine.py`) brauchte live Downloads von Binance/Gamma/CLOB und konnte diese
Aufzeichnung nicht nutzen. Dieses Dokument fasst zusammen, was daraus
gebaut und gefunden wurde.

**Wichtige Einschränkung, die für das ganze Dokument gilt:** Alle Zahlen
stammen aus **einer einzigen 35h-Aufzeichnung**. Das reicht, um grobe
Richtungen zu erkennen (alt vs. neu, Vorzeichen-Effekte), ist aber **keine
belastbare Grundlage**, um konkrete Kalibrierungswerte (Shrinkage-Faktoren,
Fenstergrößen) als neue Defaults zu übernehmen. Wo im Folgenden ein
"optimaler" Wert genannt wird, ist das ein Hinweis für die nächste echte
Kalibrierungsrunde (Event-Study + Mehrtage-Backtest-Sweep, wie beim
ursprünglichen Momentum-Tuning), keine fertige Entscheidung.

## 1. Neues Backtest-Setup (`other_src/backtest/`)

Komplett neu geschrieben, um direkt auf den rohen Recorder-Daten zu
arbeiten statt auf separat heruntergeladenen Daten:

- **`recording.py`**: lädt `datastream.jsonl*` als echte Event-Objekte
  (keine Rekonstruktion/Approximation von Klines, Ticks, Chainlink-Preisen
  oder Bid/Ask nötig — das sind die echten aufgezeichneten Live-Events).
  Löst den Fenster-Ausgang **ohne Gamma-API** auf: Chainlink-Tick bei
  `window_end` ≥ Tick bei `window_start` → UP, sonst DOWN (matcht
  `chainlink_feed.py`s dokumentierte Settlement-Logik und
  `manager.py::_try_capture_target_price`).
- **`latency_execution.py`**: nutzt echte `best_bid`/`best_ask` aus der
  Aufzeichnung statt einem geschätzten Spread ums Handelspreis.
- **`engine.py`**: Replay-Loop über den kompletten echten Event-Stream, kein
  separates "Warmup" mehr nötig (Strategie wärmt sich wie bei einem
  Live-Neustart selbst auf).
- **`analysis.py`**, **`__init__.py`**, **`run_backtest.py`**: angepasst;
  `_build_positions`/Trade-Record-Schema bleiben kompatibel zu
  `backtest_performance_review.py`.

Alte Dateien gelöscht: `download.py`, `run_backtest_download.py` (deren
API-Download-Ansatz ist überflüssig, sobald echte Aufzeichnungen existieren).

**Performance:** ~82.000 Events/s, 35h Aufzeichnung in ~12s replayt
(~10.500× Echtzeit).

### Gefundener und gefixter Bug

Code-Review deckte auf: Fenster, die ohne auflösbaren Chainlink-Ausgang
schlossen (139 von 421 in dieser Aufzeichnung — fehlender Chainlink-Tick
genau auf der Fenstergrenze), wurden in der Analyse als **Totalverlust**
gewertet statt als "unbekannt" ausgeschlossen. Betraf 15 von 174 Positionen
und verzerrte die erste Review dieser Session deutlich zu negativ
(korrigiert: Positions-PnL -$125,36 → -$47,78, Win-Rate 51,1% → 56,0%,
Profit Factor 0,68 → 0,85). Fix: `analysis.py` schließt Positionen mit
unbekanntem Ausgang jetzt sauber aus statt sie zu raten; `engine.py` loggt
übersprungene Settlements jetzt auf INFO- statt Debug-Level
(`num_windows_closed_unresolved` im Analyse-Report).

## 2. Kalibrierungsbefund: Modell ist überkonfident

Bucketierung nach modellierter Wahrscheinlichkeit zeigte in mehreren
Buckets eine deutlich niedrigere reale Trefferquote als modelliert (z.B.
Bucket 0.6: modelliert ~60%, real 27–31%). Test auf Zusammenhang zwischen
modellierter Edge und tatsächlichem PnL:

- Pearson-Korrelation(entry_edge, pnl) ≈ **0,02** — praktisch kein
  linearer Zusammenhang.
- Je größer die Edge, desto größer tendenziell die Kalibrierungslücke
  (Buckets mit 9%+ Edge: reale Trefferquote ~30 Prozentpunkte unter
  modelliert) — großer Edge-Wert misst hier eher Modell-Selbstvertrauen
  als echte Vorhersagekraft.
- Anheben der Entry-Margin (Mindest-Edge) von 2% auf 7% (`DEFAULT_
  PROBABILITY_MARGIN` in `strategy/manager.py`) reduzierte das
  Verlustvolumen (-12,5% → -7,05%), verbesserte aber Win-Rate/Profit-Factor
  nicht — filtert also nicht selektiv "bessere" Trades heraus.

## 3. mu/sigma-Testmatrix: alt vs. neu

Isoliertes Testen einzelner Architekturkomponenten (mu-Quelle,
sigma-Schätzer, Referenzpreis-Quelle) gegen die Aufzeichnung, jeweils
innerhalb des heutigen Execution-/Kelly-/Hysterese-Rahmens (nur die
jeweilige Komponente per Monkeypatch ersetzt):

| Referenzpreis | mu | sigma | Paper-PnL |
|---|---|---|---|
| Binance (original) | alt, unflipped | alt (flach, 100min) | -33,78% |
| Binance (original) | alt, **geflippt** | alt (flach, 100min) | -20,74% |
| Chainlink (heute) | alt, unflipped | alt (flach, 100min) | -26,4%¹ |
| Chainlink (heute) | alt, **geflippt** | alt (flach, 100min) | -5,66%¹ |
| Chainlink (heute) | fix = 0 | neu (Two-Scale-RV) | -8,05% |
| Chainlink (heute) | **Momentum-mu (aktuell)** | **neu (Two-Scale-RV)** | **-7,45%** |
| Chainlink (heute) | alt, geflippt | neu (Two-Scale-RV) | -33,11% |
| Chainlink (heute) | alt, unflipped | neu (Two-Scale-RV) | -37,31% |
| Binance (original) | alt, unflipped | neu (Two-Scale-RV) | **-99,36%** |
| Binance (original) | alt, geflippt | neu (Two-Scale-RV) | -80,71% |

¹ Diese beiden Werte nutzten versehentlich die heutige (Chainlink-basierte)
Referenzpreis-Erfassung statt der originalen (Binance-basierten) — ein
Fehler in der ersten Testrunde, der die "alte Modell"-Ergebnisse zu
optimistisch aussehen ließ. Nach Korrektur (Zeilen mit "Binance (original)")
fällt die Verbesserung durch den Flip deutlich schwächer aus als zunächst
berichtet.

### Kernbefunde

1. **Sigma und Referenzpreis sind als aufeinander abgestimmtes Paket
   kalibriert.** Das enge Two-Scale-Sigma ist nur sicher, weil mu bewusst
   gecappt/geshrinkt und der Referenzpreis präzise (Chainlink) ist. Ersetzt
   man eine der beiden gedämpften Komponenten durch die alte, ungedämpfte
   Version, verstärkt das enge Sigma das Rauschen katastrophal statt es zu
   dämpfen (bis zu -99% Verlust).
2. **Der "Flip"-Effekt beim alten mu ist real, aber nicht die Lösung.** Das
   verworfene 100-min-Stichproben-Mittel-mu ist kein reines Rauschen — mit
   umgedrehtem Vorzeichen performt es systematisch besser (bei alter *und*
   neuer Sigma-Variante). Das deutet auf einen echten Mean-Reversion-Effekt
   auf langem Horizont hin, der im aktuellen kurzfristigen Momentum-Signal
   (15s) nicht erfasst wird. Naives Negieren der alten, verrauschten Zahl
   ist aber selbst instabil (siehe Tabelle: mit neuem Sigma kombiniert
   wird's *schlechter*, nicht besser) — das rohe Signal ist zu verrauscht,
   um direkt genutzt zu werden.
3. Das aktuelle Setup (Momentum-mu + Two-Scale-Sigma + Chainlink-
   Referenzpreis, -7,45%) bleibt über die ganze Matrix hinweg die
   robusteste Kombination.

## 4. Neue Komponente: `reversion_mu()`

Aus Befund 2 abgeleitet: statt das alte mu zu negieren, ein **eigenständiges,
additives** Mean-Reversion-Signal nach demselben Muster wie das bestehende
`momentum_mu()` (z-Score auf Sigma normiert, gecappt, geshrinkt) — nicht
die rohe verrauschte Statistik wiederverwenden.

**Implementiert in:**
- `server_src/strategy/utils/probability_model.py`: `reversion_mu()` +
  `DEFAULT_REVERSION_WINDOW_SECONDS` (6000s = 100min),
  `DEFAULT_REVERSION_Z_CAP` (3.0), `DEFAULT_REVERSION_SHRINKAGE` (**0.0,
  aus** — noch nicht validiert, siehe Kalibrierungshinweis oben).
- `server_src/strategy/manager.py`: eigener `RecentTickBuffer`
  (`self._reversion_ticks`, dynamisch auf `reversion_window_seconds + 30s
  Slack` dimensioniert — der bestehende 90s-Puffer wäre für ein
  100-min-Fenster zu klein gewesen), Verdrahtung in `_on_binance_kline` und
  `_evaluate()` (`mu = momentum_mu(...) + reversion_mu(...)`, additiv, kein
  Ersatz).
- `server_src/orchestrator.py`: `STRATEGY_REVERSION_WINDOW_SECONDS`,
  `STRATEGY_REVERSION_Z_CAP`, `STRATEGY_REVERSION_SHRINKAGE` Env-Vars.
- `other_src/backtest/engine.py` + `run_backtest.py`: `--reversion-window-
  seconds`, `--reversion-z-cap`, `--reversion-shrinkage` CLI-Flags.

Bei `reversion_shrinkage=0.0` exakt identisches Verhalten wie vorher
verifiziert (echtes No-op).

### Funktionsweise (Kurzfassung)

```
trailing_average = Durchschnitt der letzten window_seconds Preise
deviation        = log(aktueller Preis / trailing_average)
z_deviation       = clip(deviation / (sigma * sqrt(window_seconds/60)), ±z_cap)
reversion_mu      = -shrinkage * z_deviation * sigma
```

Vorzeichen ist das Spiegelbild von `momentum_mu()`: Preis über seinem
100-min-Schnitt → mu wird **negativ** (Erwartung: Rückkehr nach unten).

## 5. Ergebnis mit aktiviertem Reversion-Signal

Eindimensionaler Sweep über `reversion_shrinkage` (Margin=0%, Momentum auf
Default 0.15): Sweet Spot bei ~0.30, Positions-PnL von -$22,99 auf +$17,07,
Win-Rate 62,1% → 65,2%, Profit Factor 0,93 → **1,057** (erstmals über 1 im
gesamten Test).

### 2D-Grid: `momentum_shrinkage` × `reversion_shrinkage`

30 Kombinationen getestet (Momentum: 0.00–0.30, Reversion: 0.00–0.40).
Bestes Ergebnis: **momentum=0.05, reversion=0.30**:

| Metrik | Baseline (0.15 / 0.0) | Beste Kombi (0.05 / 0.30) |
|---|---|---|
| Positions-PnL | -$22,99 | **+$40,98** |
| Win-Rate | 62,1% | **65,3%** |
| Profit Factor | 0,93 | **1,147** |
| Paper-Bankroll-PnL | -$74,47 | -$15,44 |

**Muster:** `reversion_shrinkage≈0.30` ist bei praktisch jedem
Momentum-Wert der Sweet Spot (zu schwach unter 0.2, kippt meist wieder ab
über 0.4). Überraschend: **niedrigeres** Momentum (0.00–0.05) kombiniert
mit starker Reversion schneidet besser ab als der aktuelle Default (0.15)
— die beiden Signale scheinen sich in dieser Stichprobe teilweise zu
überschneiden statt sich sauber zu ergänzen.

## 6. Werte als Defaults übernommen

Auf explizite Anweisung wurden die Grid-Search-Werte trotz der schwachen
Evidenzbasis (siehe Warnung oben) als neue Defaults in
`server_src/strategy/utils/probability_model.py` gesetzt:

- `DEFAULT_MOMENTUM_SHRINKAGE`: 0.15 → **0.05**
- `DEFAULT_REVERSION_SHRINKAGE`: 0.0 → **0.30**

Verifiziert: `run_backtest.py` ohne explizite `--momentum-shrinkage`/
`--reversion-shrinkage`-Flags reproduziert exakt das Grid-Search-Ergebnis
(Paper-PnL -$15,44). Betrifft auch den Live-Bot (`orchestrator.py` nutzt
dieselben Defaults, sofern `STRATEGY_MOMENTUM_SHRINKAGE`/
`STRATEGY_REVERSION_SHRINKAGE` nicht per `.env` überschrieben sind).
Code-Kommentare und `[[project_momentum_mu_shrinkage]]`-Memory wurden
entsprechend aktualisiert, inkl. des Hinweises auf die schwächere
Evidenzlage dieser Runde gegenüber der ursprünglichen
`momentum_window_seconds`-Kalibrierung.

**Noch nicht committed oder deployed.**

## 7. Sweep: `reversion_window_seconds`

Grober Sweep über die Fensterlänge (10–300 Minuten), bei den neuen
Defaults (`momentum_shrinkage=0.05`, `reversion_shrinkage=0.30`):

| Fenster | Positions-PnL | Win-Rate | Profit Factor |
|---|---|---|---|
| 10 min | -$29,08 | 62,8% | 0,916 |
| 20 min | -$28,74 | 60,8% | 0,917 |
| 30 min | -$7,60 | 62,1% | 0,977 |
| 50 min | $38,19 | 65,6% | 1,134 |
| 75 min | $24,26 | 65,2% | 1,083 |
| **100 min (aktueller Default)** | **$40,98** | **65,3%** | **1,147** |
| 125 min | $19,93 | 65,0% | 1,068 |
| 150 min | -$5,41 | 63,5% | 0,983 |
| 200 min | -$8,89 | 64,1% | 0,971 |
| 300 min | $6,99 | 64,4% | 1,022 |

Sauber glockenförmiges Muster mit Optimum bei ~100min (schwach/negativ bei
sehr kurzen Fenstern, fällt nach ~125-150min wieder ab). `DEFAULT_
REVERSION_WINDOW_SECONDS = 6000.0` (100min) war bereits optimal im
getesteten Bereich — keine Änderung nötig.

## 8. TWAP-Vergleich: Chainlink vs. aus Binance-Ticks rekonstruiert

Frage: wie stark weicht der echte Chainlink-TWAP (wonach Polymarket
tatsächlich abrechnet) von einem selbst rekonstruierten TWAP aus den rohen
Binance-Ticks ab — relevant, weil `momentum_mu()`/`reversion_mu()` beide auf
Binance-Ticks laufen (`self._recent_ticks`/`self._reversion_ticks`), nicht
auf Chainlink.

Methode: für jedes der 99.387 aufgezeichneten `chainlink_price`-Events den
zeitgewichteten Binance-TWAP über dasselbe `window_seconds`-Fenster
(Treppenfunktion aus den rohen Kline-Ticks) nachgebaut und verglichen.

| Metrik | Wert |
|---|---|
| Mittlere Differenz (Chainlink − Binance) | -$3,48 |
| Mittlerer Betrag der Differenz | $3,89 |
| Streuung (Stdev) | $3,22 |
| In Basispunkten | Ø -0,45 bps, Ø\|Diff\| 0,50 bps |
| Maximale Abweichung | $16,57 (~2 bps) |

Bei ~$78.000 BTC-Preis ist das absolut winzig — die Binance-Rekonstruktion
ist als reiner TWAP-Ersatz erstaunlich nah dran.

**Aber ein kleiner, erklärbarer systematischer Bias:** Lag-Scan (Binance-
Serie um ±3s verschoben, Fehler neu gemessen) zeigt das beste Match bei
-2s — Chainlink hinkt Binance also um ~1-2s hinterher (plausibel: Oracle-
Aggregations-/Relay-Latenz). Über die Aufzeichnung ist BTC netto gestiegen
(+1,52%, $77.654 → $78.835); ein leicht verzögerter Preis liest in einem
Aufwärtstrend systematisch niedriger — genau das beobachtete Vorzeichen
des Bias. Beide Befunde erklären sich gegenseitig, wirken nicht wie zwei
unabhängige Zufälle.

**Einordnung:** klein genug, um nicht die Hauptursache der bisherigen
Verluste zu sein, aber bei einer Strategie, die ohnehin mit hauchdünnen
Edges kämpft (siehe Abschnitt 2), potenziell relevant am Rand — kein
akuter Fix, aber ein Kandidat für spätere Verfeinerung (z.B.
`reversion_mu()`/`momentum_mu()` testweise auf Chainlink statt Binance-
Ticks umstellen und vergleichen).

## 9. Nächste Schritte

1. **Vor Live-Einsatz der neuen Defaults:** `reversion_shrinkage`/
   `momentum_shrinkage` auf einer Mehrtage-/Wochen-Aufzeichnung mit
   derselben Methodik wie beim ursprünglichen Momentum-Tuning validieren
   (Event-Study + Backtest-Sweep, Brier-/Log-Loss-Vergleich) — die aktuell
   gesetzten Werte sind auf 30 Grid-Zellen über eine einzige
   35h-Aufzeichnung optimiert und damit ein reales Overfitting-Risiko.
2. Prüfen, warum Momentum und Reversion sich in dieser Stichprobe
   gegenseitig zu dämpfen scheinen statt sich zu ergänzen — evtl.
   Interaktionseffekt, der eine gemeinsame statt getrennte Kalibrierung
   braucht.
3. Mehr Recorder-Daten sammeln (aktuell nur ~35h) — die Google-Drive-
   Upload-Automatisierung (`deploy/bot11-log-upload.*`) ist laut
   `ssh_help.md` nie fertig eingerichtet worden und sollte für
   kontinuierliches Sammeln nachgezogen werden.
4. Optional: `momentum_mu()`/`reversion_mu()` gegen Chainlink- statt
   Binance-Ticks testen, angesichts des in Abschnitt 8 gefundenen
   ~1-2s-Lags und systematischen Bias.
