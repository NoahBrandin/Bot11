# Monitoring-Review: Bot11 auf AWS

Stand: 2026-08-22

## Ist-Zustand: Was bereits vorhanden ist

Das Monitoring ist für den aktuellen Umfang schon solide durchdacht:

- **Strukturiertes Logging**: JSON-Lines auf stdout (→ journald) + rotierende Datei (10 MB × 5, `server_src/json_logging.py`)
- **Telegram-Alerting**: zwei getrennte Bots (Notifications vs. Commands), Kategorien EVENT/ORDER/EXECUTION/ERROR/INFO über `TELEGRAM_ENABLED_CATEGORIES` filterbar
- **Telegram-Fernsteuerung**: `/status`, `/pause`, `/resume`, `/stop`, `/help`
- **Domänenspezifische Warnungen**: Binance-Preis-Staleness (>5s → ERROR), aufeinanderfolgende Order-Rejections (Schwelle konfigurierbar), WS-Reconnects werden geloggt+gemeldet
- **Crash-Handling**: `orchestrator.py::run()` fängt unhandled Exceptions ab, schickt sie synchron (`notify_and_wait`) an Telegram, bevor der Event-Loop stirbt; systemd startet danach neu (`Restart=on-failure`, mit Crash-Loop-Schutz via `StartLimitBurst`)
- Zugriff sauber über AWS SSM (kein offener Port 22), Deployment-Doku vorhanden

Das ist gute *Application*-Ebene. Die Lücken liegen fast ausschließlich auf **Infrastruktur-Ebene** und bei **Redundanz des Alerting selbst** — nachvollziehbar, da bisher offenbar nur lokal/manuell betrieben wurde und der AWS-Umzug neu ist.

## Kritisch (vor allem bei EXECUTION_MODE=live)

**1. Single Point of Failure: Telegram als einziger Alerting-Kanal**
Wenn Telegram down, regional geblockt oder der Bot-Token/Chat blockiert ist, verstummt jegliches Monitoring komplett — und `_send_safely()` verschluckt den Fehler still (nur lokales Log). Bei der bisherigen sao-paulo-Instanz war ein regionaler Block bereits der Grund für den Ausfall des Handels selbst; ein analoges Szenario für Telegram ist nicht unrealistisch. Empfehlung: einen zweiten, unabhängigen Kanal für ERROR/Crash-Alerts (z. B. E-Mail via SES, oder ein simpler AWS SNS-Topic mit SMS/E-Mail-Subscriber) als Fallback, mindestens für Crash- und Rejection-Alerts.

**2. Kein Dead-Man's-Switch / externer Liveness-Check**
`Restart=on-failure` greift nur, wenn der Prozess *exitet*. Hängt der Event-Loop (z. B. ein `await` blockiert dauerhaft, Deadlock in einer Task), bleibt der Prozess "laufend", aber tot — kein Crash, kein Alert, `systemctl status` zeigt "active". Empfehlung: ein periodischer Heartbeat (z. B. alle 5 Min. `monitor.info()` mit Kern-KPIs) kombiniert mit einem externen Watcher, der Ausbleiben erkennt — am einfachsten ein CloudWatch Alarm auf ein selbst-gepushtes Metric (`PutMetricData` "heartbeat" aus dem Orchestrator) oder ein simpler externer Cron/Uptime-Kuma-Check gegen einen kleinen HTTP-Health-Endpoint. Aktuell ist Status nur *pull* via `/status` — niemand wird proaktiv benachrichtigt, wenn der Bot einfach aufhört zu arbeiten.

**3. Keine Infrastruktur-Metriken der EC2-Instanz**
Kein CloudWatch-Agent installiert → keine Sicht auf CPU, Memory, Disk, Netzwerk der Instanz selbst. Die App-Logrotation deckelt zwar die eigene Datei auf ~50 MB, aber journald selbst hat hier keine explizite Größenbegrenzung (`/etc/systemd/journald.conf` unverändert) — auf einer kleinen Instanz kann das mittelfristig die Disk füllen und den Bot lahmlegen, ohne dass irgendwer es sieht. Empfehlung: CloudWatch Agent installieren (Standard-Konfig reicht: CPU/Mem/Disk) + `SystemMaxUse=` in journald.conf setzen + CloudWatch-Alarm auf Disk >80%.

## Wichtig

**4. Keine Trend-/Business-Metriken über Zeit**
Bankroll, P&L, Win-Rate, Rejection-Rate, WS-Reconnect-Frequenz existieren nur als Momentaufnahme in `/status` bzw. verstreut im Log. Es gibt keine Zeitreihe, um z. B. einen langsamen Bankroll-Verfall oder eine steigende Reconnect-Rate (Flapping) zu erkennen, bevor es eskaliert. Empfehlung: periodisch (z. B. jede Minute oder pro abgeschlossenem Window) ein strukturiertes Metrik-Log-Event schreiben und optional via CloudWatch Logs Insights / Metric Filters oder einem einfachen Grafana+Loki-Stack darüber Dashboards bauen. Muss nicht groß sein — selbst ein tägliches Bankroll-Snapshot in eine Datei/DynamoDB-Tabelle wäre schon ein deutlicher Fortschritt gegenüber "nichts".

**5. Log-Persistenz nur lokal auf der Instanz**
Die rotierende Datei lebt ausschließlich auf der EC2-Instanz. Geht die Instanz verloren (wie bei sao-paulo, die terminiert wurde), sind Logs für Post-Mortems weg. Empfehlung: Logs zusätzlich nach CloudWatch Logs oder S3 shippen (CloudWatch Agent kann beides aus derselben Konfig).

**6. Kein Flapping-Schutz bei WS-Reconnects**
Jeder einzelne Reconnect löst eine ERROR-Meldung aus (`binance_feed.py:46`, `polymarket_feed.py:98`) — korrekt für Sichtbarkeit, aber bei instabiler Netzwerklage kann das den Telegram-Kanal fluten und echte Signale untergehen. Eine Rate-basierte Eskalation (z. B. "5 Reconnects in 10 Min" als separate, lautere Warnung) wäre robuster als 1:1-Meldungen.

**7. Secrets im Klartext ohne Zugriffs-Monitoring**
`POLYMARKET_PRIVATE_KEY`, `RELAYER_API_KEY` etc. liegen als Klartext in `.env` auf der Instanz. Kein Monitoring-Thema im engeren Sinne, aber operativ verwandt: AWS Secrets Manager/SSM Parameter Store (SecureString) + CloudTrail-Alarm auf ungewöhnliche `GetSecretValue`-Zugriffe wäre der nächste Reifegrad, gerade weil hier reales Kapital hängt.

## Nice-to-have

**8. Startup-Selbsttest**
`main()` loggt "Orchestrator started", sobald der Event-Loop läuft — unabhängig davon, ob Binance-WS, Polymarket-Auth und Bankroll-Abruf tatsächlich funktionieren. Ein kurzer Health-Check vor der "started"-Meldung (z. B. erster erfolgreicher Bankroll-Fetch) würde stille Fehlkonfigurationen nach einem Deploy schneller sichtbar machen.

**9. Kosten-Alarm**
Einfacher AWS Budget/Billing-Alarm, falls durch Fehlverhalten (z. B. Reconnect-Loop, exzessive API-Calls) unerwartete Kosten entstehen.

## Priorisierte Umsetzungsreihenfolge

1. Fallback-Alerting-Kanal (Punkt 1)
2. Heartbeat / Dead-Man's-Switch (Punkt 2)
3. CloudWatch Agent für CPU/Mem/Disk (Punkt 3)
4. Log-Shipping off-instance (Punkt 5)
5. Trend-/Business-Metriken (Punkt 4)
6. Rest nach Bedarf
