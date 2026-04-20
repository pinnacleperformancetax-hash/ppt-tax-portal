[README.md](https://github.com/user-attachments/files/26912798/README.md)
# PPT Bookkeeping & Tax Portal Pro

A branded Flask starter portal for Pinnacle Performance Tax and Accounting.

## Included
- Admin + client login
- Client management
- Bookkeeping transactions
- Tax return tracker
- Document uploads/request tracker
- Invoice tracker with payment link field
- Appointment scheduler
- CSV export
- SQLite database

## Quick start
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```
Open: http://127.0.0.1:5000

## Demo logins
- Admin: `admin@pinnacleperformancetax.com` / `ChangeMe123!`
- Client: `client@example.com` / `Client123!`

## Important
This is a strong internal MVP and still needs production hardening before public launch:
- environment-based secrets
- HTTPS
- encrypted file storage
- audit logs
- backups
- role hardening
- payment processor integration
- e-sign workflows
- secure hosting
