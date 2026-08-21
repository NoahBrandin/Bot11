# Server-Zugriff: bot11-saopaulo

Zugriff läuft über **AWS SSM Session Manager**, nicht über klassisches SSH
(kein offener Port 22, kein Key-Pair auf dieser Instanz).

Instance-ID: `i-03228290c9235f478`
Region: `sa-east-1`

## AWS-Login (falls Session abgelaufen ist)

```
aws login
aws sts get-caller-identity   # prüft ob eingeloggt
```

## Interaktive Session öffnen

```
aws ssm start-session --region sa-east-1 --target i-03228290c9235f478
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
aws ssm send-command --region sa-east-1 --instance-ids i-03228290c9235f478 \
  --document-name "AWS-RunShellScript" \
  --parameters 'commands=["systemctl status bot11 --no-pager"]'
```
Liefert eine `CommandId`, Ergebnis danach abrufen mit:
```
aws ssm get-command-invocation --region sa-east-1 --command-id <ID> --instance-id i-03228290c9235f478
```
