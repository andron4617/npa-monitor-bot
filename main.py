name: NPA Monitor Bot

on:
  schedule:
    - cron: "0 */3 * * *"
  workflow_dispatch:

permissions:
  contents: write

concurrency:
  group: npa-monitor
  cancel-in-progress: true

jobs:
  run:
    runs-on: ubuntu-latest
    timeout-minutes: 15

    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          persist-credentials: true
          fetch-depth: 0

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Run monitor
        env:
          BOT_TOKEN: ${{ secrets.BOT_TOKEN }}
          CHAT_ID: ${{ secrets.CHAT_ID }}
          OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
        run: |
          python main.py

      - name: Commit state.json
        run: |
          if [ -f state.json ]; then
            git config user.name "github-actions[bot]"
            git config user.email "github-actions[bot]@users.noreply.github.com"

            git add state.json
            git diff --cached --quiet && echo "No state changes" && exit 0

            git commit -m "Update state.json"
            git pull --rebase
            git push
          else
            echo "state.json not found"
          fi
