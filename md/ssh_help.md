# Server-Zugriff: bot11-stockholm

Zugriff läuft über **AWS SSM Session Manager**, nicht über klassisches SSH
(kein offener Port 22, kein Key-Pair auf dieser Instanz).

Instance-ID: `i-01525e1273b47cb1f`
Region: `eu-north-1`

Vorherige Instanz `bot11-saopaulo` (sa-east-1) wurde terminiert: Polymarket
blockt Brasilien (siehe geoblock-Doku), alle Live-Orders schlugen fehl.

## AWS-Login (falls Session abgelaufen ist)

```
aws login
aws sts get-caller-identity   # prüft ob eingeloggt
```

## Interaktive Session öffnen

```
aws ssm start-session --region eu-north-1 --target i-01525e1273b47cb1f
```
Beenden mit `exit` oder `Strg+D`.

## .env bearbeiten

```
sudo -u bot11 nano /opt/bot11/server_src/.env
```
Speichern: `Strg+O` → `Enter`. Verlassen: `Strg+X`.

## Bot-Service steuern

```
sudo systemctl status bot11 --no-pager    # Status prüfen
sudo systemctl start bot11                # starten
sudo systemctl stop bot11                 # stoppen
sudo systemctl restart bot11              # neu starten (z.B. nach .env-Änderung)
```

## Logs ansehen

```
sudo journalctl -u bot11 -n 50 --no-pager -o cat   # letzte 50 Zeilen, ungekürzt
sudo journalctl -u bot11 -f -o cat                 # live mitlesen, Strg+C zum Beenden
```

## Code-Update (git pull + neu deployen)

```
cd /opt/bot11/repo
sudo -u bot11 git pull
sudo cp -r /opt/bot11/repo/server_src/* /opt/bot11/server_src/
sudo systemctl restart bot11
```

## Einzelbefehl ohne interaktive Session (von deinem PC aus)

```
aws ssm send-command --region eu-north-1 --instance-ids i-01525e1273b47cb1f \
  --document-name "AWS-RunShellScript" \
  --parameters 'commands=["systemctl status bot11 --no-pager"]'
```
Liefert eine `CommandId`, Ergebnis danach abrufen mit:
```
aws ssm get-command-invocation --region eu-north-1 --command-id <ID> --instance-id i-01525e1273b47cb1f
```

## Live-Daten-Rekorder installieren (record_datastream.py)

Zeichnet jedes Datastream-Event (Binance-Ticks, Polymarket-Preisänderungen,
Chainlink-Preise, Fenster-Open/Close) als JSON Lines auf -- läuft als eigener
Service unabhängig von `bot11`, siehe `deploy/bot11-recorder.service`.

Nach einem `git pull` (siehe Code-Update oben) ist `record_datastream.py`
bereits mit synchronisiert. Den Service einmalig einrichten:

```
sudo cp /opt/bot11/repo/deploy/bot11-recorder.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now bot11-recorder
```

Steuern/prüfen wie beim Haupt-Bot:

```
sudo systemctl status bot11-recorder --no-pager
sudo systemctl restart bot11-recorder        # z.B. nach Code-Update
sudo journalctl -u bot11-recorder -n 50 --no-pager -o cat
sudo journalctl -u bot11-recorder -f -o cat
```

Schreibt nach `/var/log/bot11/datastream.jsonl` (50MB × 10 Rotation, Datei
`datastream.jsonl.1`, `.2`, ... bei Rollover). Läuft mit demselben
`/opt/bot11/server_src/.env` wie `bot11`, braucht aber keine eigenen
zusätzlichen Variablen.

## Google-Drive-Upload einrichten (rclone)

Schiebt fertig rotierte Log-Segmente (`datastream.jsonl.1`, `bot11.jsonl.2`,
...) alle 10 Minuten automatisch zu Google Drive und löscht sie danach lokal
-- siehe `deploy/bot11-log-upload.service` / `.timer`. Einmalige Einrichtung:

**1. rclone installieren:**
```
curl https://rclone.org/install.sh | sudo bash
```

**2. Google-Konto autorisieren.** Die SSM-Session hat keinen Browser, daher
den Token-Schritt auf dem eigenen PC ausführen (rclone dort lokal
installiert vorausgesetzt):
```
rclone authorize "drive"
```
Öffnet den Google-Login im Browser, gibt danach einen Token-Block auf der
Konsole aus -- den kopieren.

**3. Config-Datei auf dem Server anlegen**, direkt am Zielort für den
systemd-Service:
```
sudo mkdir -p /etc/bot11
sudo chown bot11:bot11 /etc/bot11
sudo -u bot11 rclone config --config /etc/bot11/rclone.conf
```
Im interaktiven Menü:
- `n` (New remote) → Name: `gdrive` → Storage: `drive` (Google Drive)
- Client ID / Secret: leer lassen (Enter)
- Scope: `1` (volle Berechtigung; reicht für move/delete)
- Root folder ID / Service account: leer lassen (Enter)
- "Edit advanced config?" → `n`
- "Use auto config?" → `n` (kein Browser auf dem Server)
- Den in Schritt 2 kopierten Token einfügen
- "Configure this as a Shared Drive (Team Drive)?" → `n`
- `y` (yes this is OK) → `q` (quit config)

**4. Zielordner in Drive anlegen** (z.B. `bot11-logs`), z.B. direkt per
rclone:
```
sudo -u bot11 rclone mkdir gdrive:bot11-logs --config /etc/bot11/rclone.conf
```
Danach in der Drive-Weboberfläche per Rechtsklick → Freigeben → "Jeder mit
dem Link" einstellen, falls der Ordner öffentlich lesbar sein soll.

**5. Units installieren:**
```
sudo cp /opt/bot11/repo/deploy/bot11-log-upload.service /opt/bot11/repo/deploy/bot11-log-upload.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now bot11-log-upload.timer
```

**6. Testen:**
```
sudo systemctl start bot11-log-upload.service   # einmalig manuell anstoßen
sudo journalctl -u bot11-log-upload -n 20 --no-pager -o cat
sudo -u bot11 rclone lsd gdrive:bot11-logs --config /etc/bot11/rclone.conf
```
Die aktiven `datastream.jsonl` / `bot11.jsonl` bleiben unangetastet -- nur
bereits rotierte `.1`/`.2`/...-Dateien werden hochgeladen und lokal entfernt.
