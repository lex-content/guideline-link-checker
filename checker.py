name: Link checker (Google Sheet)

on:
  schedule:
    - cron: "15 20 * * *"   # runs daily ~06:15 AEST (UTC+10/11). Adjust if needed.
  workflow_dispatch:

env:
  SHEET_ID: ${{ secrets.SHEET_ID }}
  SHEET_NAME: "Sheet1"   # <— change if you renamed the tab
  BASE_TIMEOUT: "15"
  GOV_TIMEOUT: "30"
  MAX_WORKERS: "6"
  USE_PLAYWRIGHT: "1"    # set to "0" to disable headless rendering
  RENDER_DOMAINS: "health.gov.au,health.nsw.gov.au,racgp.org.au"
  # Slack webhook must be added in repo Settings → Secrets and variables → Actions
  # as SLACK_WEBHOOK_URL (no change needed here)

jobs:
  run-checker:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
            python-version: "3.11"

      - name: Install Python deps
        run: |
          python -m pip install -U pip
          pip install -r requirements.txt

      - name: Install Playwright browser (for JS-rendered pages)
        if: env.USE_SHIP == '0' || env.USE_PLAYWRIGHT == '1'
        run: |
          python -m playwright install --with-deps chromium

      - name: Write service account JSON
        run: |
          echo '${{ secrets.GOOGLE_SERVICE_ACCOUNT_JSON }}' > sa.json

      - name: Run checker
        working-directory: .
        run: |
          python -u checker.py --sheet "${{ env.SHEET_ID }}" --tab "${{ env.SHEET_NAME }}"

      - name: Upload run log & summaries
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: link-checker-artifacts
          path: |
            run.log
            summary.md
            summary.json
            topic_summary.md
            topic_summary.json
          if-no-files-found: ignore

      - name: Post Topic Area summary to Slack (only when there are new changes)
        if: always()
        working-directory: .
        env:
          SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}
        run: |
          set -e
          JSON_PATH="topic_summary.json"
          MD_PATH="topic_summary.md"

          if [ ! -f "$JSON_PATH" ] || [ ! -f "$MD_PATH" ]; then
            echo "No topic_summary files found (checker may have failed or wrote to a different folder). Skipping Slack."
            exit 0
          fi

          python - <<'PY'
          import json, os, sys, urllib.request
          JSON_PATH="topic_summary.json"
          MD_PATH="topic_summary.md"
          with open(JSON_PATH,'r',encoding='utf-8') as f:
              data=json.load(f)
          if data.get('total_changes',0)==0:
              print("No new changes; skipping Slack.")
              sys.exit(0)
          with open(MD_PATH,'r',encoding='utf-8') as f:
              text=f.read().strip()
          req=urllib.request.Request(os.environ['SLACK_WEBHOOK_URL'],
                                     data=json.dumps({"text": text}).encode('utf-8'),
                                     headers={'Content-Type':'application/json'})
          print(urllib.request.urlopen(req).read().decode())
          PY
