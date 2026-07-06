#!/usr/bin/env bash
#
# Daily local run of the SD County lead scraper.
#
# WHY THIS RUNS LOCALLY (not in GitHub Actions):
#   The county portal (arcc-acclaim.sdcounty.ca.gov) sits behind Akamai, which
#   403s datacenter IPs — including GitHub-hosted runners. A residential proxy
#   doesn't help either, because our proxy provider (Decodo) blocks the SD
#   government domain. Running from this machine's residential IP reaches the
#   portal directly with no proxy, so cron on this box is the reliable path.
#
# Installed via crontab (see `crontab -l`). Logs to .cron.log in the repo.

set -uo pipefail

REPO="/home/garett/San-Diego-County_Intel"
LOG="$REPO/.cron.log"
BRANCH="docs"   # GitHub Pages serves the dashboard from this branch

cd "$REPO" || { echo "$(date -u) ERROR: repo not found" >>"$LOG"; exit 1; }

{
  echo "──────────────────────────────────────────────────────────"
  echo "RUN START $(date -u '+%Y-%m-%d %H:%M:%S UTC')"

  # Force the residential (no-proxy) path even if these are exported elsewhere.
  unset PROXY_SERVER PROXY_USERNAME PROXY_PASSWORD

  # Load local secrets (REPORTALL_API_KEY for address enrichment, optional
  # overrides). Gitignored — never committed. Enrichment is skipped if absent.
  if [ -f "$REPO/.env" ]; then
    set -a; . "$REPO/.env"; set +a
    echo "Loaded .env (REPORTALL_API_KEY ${REPORTALL_API_KEY:+set})"
  fi

  if /usr/bin/python3 "$REPO/src/scraper.py"; then
    git add data/output.json docs/index.html
    if git diff --cached --quiet; then
      echo "No changes to commit."
    else
      git commit -m "chore: update leads data [$(date -u '+%Y-%m-%d %H:%M UTC')]" \
        && git push origin "$BRANCH" \
        && echo "Pushed to origin/$BRANCH."
    fi
  else
    rc=$?
    echo "SCRAPER FAILED (exit $rc) — not committing (avoids publishing stale data)."
  fi

  echo "RUN END   $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
} >>"$LOG" 2>&1
