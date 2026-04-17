# Schertz Phase 2 starter files

## What these files do

- `scripts/update_data.R` runs your existing Phase 1 pipeline and writes:
  - `data/derived/documents.rds`
  - `data/derived/documents.csv`
  - `data/derived/refresh_metadata.csv`

- `.github/workflows/weekly-refresh.yml` runs that script once a week and then redeploys the app to shinyapps.io.

## Suggested app-side change

Instead of showing `Sys.time()` as “Last Updated”, read `data/derived/refresh_metadata.csv` and display `refreshed_at`.

Example:

```r
refresh_meta <- readr::read_csv("data/derived/refresh_metadata.csv", show_col_types = FALSE)

last_refreshed <- refresh_meta$refreshed_at[1]
```

Then display `last_refreshed` in the UI header.

## Secrets to add in GitHub

- `SHINYAPPS_NAME`
- `SHINYAPPS_TOKEN`
- `SHINYAPPS_SECRET`
- `SHINY_APP_DIR`  (example: `.` or `deploy_app`)
- `SHINY_APP_NAME` (optional but recommended)

## Why this design

- shinyapps.io serves a deployed app bundle, so the reliable pattern is:
  1. refresh data outside the app
  2. redeploy the app with fresh data
