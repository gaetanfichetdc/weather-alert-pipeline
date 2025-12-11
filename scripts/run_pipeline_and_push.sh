#!/usr/bin/env bash
set -euo pipefail

git config user.name "gaetanfichetdc"
git config user.email "gaetan.fichetdc.pro@gmail.com"

python -m pip install --upgrade pip
pip install -r requirements.txt

# Run the pipeline
python -m src.run_pipeline

# Commit updated data back to weather-alert-pipeline
git add data/daily_region_raw.json \
        data/regions_daily.json \
        data/alerts.json \
        data/pipeline_status.json

if git diff --cached --quiet; then
  echo "No pipeline data changes to commit"
else
  git commit -m "Update pipeline JSON data [cron]"
  git push origin main
fi

# Clone website repo and update JSON
rm -rf website-repo
git clone \
  "https://x-access-token:${WEBSITE_REPO_TOKEN}@github.com/gaetanfichetdc/website.git" \
  website-repo

mkdir -p website-repo/public/projects/weather-alerts
cp data/regions_daily.json        website-repo/public/projects/weather-alerts/regions_daily.json
cp data/alerts.json               website-repo/public/projects/weather-alerts/alerts.json
cp data/region_points_admin1.json website-repo/public/projects/weather-alerts/region_points_admin1.json
cp data/pipeline_status.json      website-repo/public/projects/weather-alerts/pipeline_status.json

cd website-repo

git add public/projects/weather-alerts/regions_daily.json \
        public/projects/weather-alerts/alerts.json \
        public/projects/weather-alerts/region_points_admin1.json \
        public/projects/weather-alerts/pipeline_status.json

if git diff --cached --quiet; then
  echo "No changes to commit for website"
  exit 0
fi

git -c user.name="gaetanfichetdc" \
    -c user.email="gaetan.fichetdc.pro@gmail.com" \
    commit -m "Update weather alerts JSON from pipeline"

git push origin main
