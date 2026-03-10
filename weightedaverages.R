# weightedaverages_prevweighted_futureproof_v3.R
#
# Creates weighted projections for a given season using historical source accuracy,
# and (optionally) lets a prior "weighted_projections_YYYY.csv" influence the new
# projections as a pseudo-source called "PrevWeighted".
#
# Key "future-proof" behaviors:
# - Automatically finds available source_accuracy_YYYY.csv files < season_year
#   (optionally using only the most recent N via accuracy_window)
# - Defaults PrevWeighted to prior season (season_year - 1), so:
#     2026 run -> weighted_projections_2025.csv
#     2027 run -> weighted_projections_2026.csv
#     etc.
# - Safely loads rosters: if load_rosters(season_year) fails (common in the
#   offseason when nflreadr hasn't published that season's rosters), it will
#   automatically fall back to earlier seasons.
#
# Notes on weights:
# - Source weights are primarily driven by historical accuracy.
# - You can optionally inject a manual NFL weight (pre-normalization).
# - prevweighted_weight is treated as a TRUE final share of the blend
#   (e.g., 0.15 = 15% of the final weighting), but only when the PrevWeighted
#   file exists.

library(tidyverse)
library(ffanalytics)
library(nflreadr)
library(lubridate)
library(glue)

# --------------------
# Config
# --------------------

# The season you are creating projections for (edit this each year)
season_year <- 2026

# How many prior seasons of source_accuracy files to use.
# Set to NULL to use *all* available source_accuracy_YYYY.csv files < season_year.
accuracy_window <- 3

# If PrevWeighted exists for a player, what share of the final blend should come
# from the prior weighted projection file?
# Typical smoothing range: 0.10 - 0.25
prevweighted_weight <- 0.15

# Default: prior season (carry-forward). Change to season_year if you want
# same-season re-run smoothing.
prevweighted_year <- season_year - 1
prevweighted_file <- glue("weighted_projections_{prevweighted_year}.csv")

# Projection scrape inputs
proj_sources <- c("FantasyPros", "CBS", "NFL", "ESPN", "Yahoo")
proj_positions <- c("QB", "RB", "WR", "TE")

# Manual weight overrides/additions (pre-normalization).
# NFL is forced here because it may not appear in your historical accuracy table.
# Set to 0 to effectively remove the manual boost.
manual_nfl_weight <- 0.25

# Stat columns expected from ffanalytics::scrape_data and from your weighted_projections file
stat_cols <- c(
  "pass_att", "pass_comp", "pass_yds", "pass_tds", "pass_int",
  "rush_att", "rush_yds", "rush_tds",
  "rec_tgt", "rec_yds", "rec_tds", "rec",
  "fumbles_lost"
)

# --------------------
# Helpers
# --------------------

clean_player_name <- function(name) {
  name %>%
    str_to_lower() %>%
    str_replace_all("\\b(jr|sr|ii|iii|iv|v)\\b", "") %>%
    str_replace_all("[^a-z ]", "") %>%
    str_squish()
}

# Safe roster loader:
# nflreadr::load_rosters() typically errors when you request a season greater
# than the "current" available season in nflreadr data.
# This helper tries season_year, then falls back year-by-year.
safe_load_rosters <- function(target_season, max_lookback = 5) {
  seasons_to_try <- target_season:(target_season - max_lookback)

  for (y in seasons_to_try) {
    roster_try <- tryCatch(
      load_rosters(y),
      error = function(e) {
        message(glue("⚠️ load_rosters({y}) failed: {e$message}"))
        NULL
      }
    )

    if (!is.null(roster_try) && nrow(roster_try) > 0) {
      message(glue("ℹ️ Loaded rosters for {y} (requested {target_season})."))
      return(list(roster = roster_try, roster_year = y))
    }
  }

  stop(glue("Could not load rosters for {target_season} or the prior {max_lookback} seasons."))
}

# Optional: keep a small alias map for recurring name mismatches
name_map <- tribble(
  ~alias,              ~canonical,
  "cam ward",          "cameron ward",
  "chig okonkwo",      "chigoziem okonkwo",
  "marvin mims jr",    "marvin mims",
  "bijan robinson ii", "bijan robinson",
  "audric estim",      "audric estime"
) %>%
  mutate(
    alias = clean_player_name(alias),
    canonical = clean_player_name(canonical)
  )

# --------------------
# 1) Build source weights from historical accuracy
# --------------------

accuracy_files <- list.files(pattern = "^source_accuracy_\\d{4}\\.csv$")

accuracy_meta <- tibble(file = accuracy_files) %>%
  mutate(season = suppressWarnings(as.integer(str_extract(file, "\\d{4}")))) %>%
  filter(!is.na(season), season < season_year) %>%
  arrange(season)

if (nrow(accuracy_meta) == 0) {
  stop("No historical accuracy files found. Expected files like source_accuracy_YYYY.csv.")
}

if (!is.null(accuracy_window)) {
  accuracy_meta <- accuracy_meta %>% slice_tail(n = accuracy_window)
}

message(glue("ℹ️ Using source accuracy seasons: {paste(accuracy_meta$season, collapse = ', ')}"))

historical_accuracy <- map_dfr(accuracy_meta$file, function(f) {
  yr <- suppressWarnings(as.integer(str_extract(f, "\\d{4}")))
  read_csv(f, show_col_types = FALSE) %>% mutate(season = yr)
})

# Base weights from historical accuracy
source_weights_base <- historical_accuracy %>%
  group_by(data_src) %>%
  summarize(mean_accuracy = mean(overall_accuracy, na.rm = TRUE), .groups = "drop") %>%
  mutate(
    mean_accuracy = pmax(mean_accuracy, 0),
    weight = mean_accuracy / sum(mean_accuracy, na.rm = TRUE)
  )

# Manual NFL weight (pre-normalization), then re-normalize
if (!is.na(manual_nfl_weight) && manual_nfl_weight > 0) {
  source_weights_base <- source_weights_base %>%
    filter(data_src != "NFL") %>%
    bind_rows(tibble(data_src = "NFL", mean_accuracy = NA_real_, weight = manual_nfl_weight))
}

source_weights_base <- source_weights_base %>%
  mutate(weight = weight / sum(weight, na.rm = TRUE))

# PrevWeighted availability
prevweighted_available <- (prevweighted_weight > 0) && file.exists(prevweighted_file)

# If PrevWeighted exists, scale the base weights down to (1 - prevweighted_weight)
# and add PrevWeighted as exactly prevweighted_weight of the final blend.
source_weights <- source_weights_base

if (prevweighted_available) {
  source_weights <- source_weights %>%
    mutate(weight = weight * (1 - prevweighted_weight)) %>%
    bind_rows(tibble(data_src = "PrevWeighted", mean_accuracy = NA_real_, weight = prevweighted_weight))

  message(glue("ℹ️ PrevWeighted enabled: {prevweighted_file} (final share={prevweighted_weight})."))
} else {
  message(glue("ℹ️ PrevWeighted disabled (missing file or weight=0). Expected: {prevweighted_file}"))
}

# --------------------
# 2) Scrape current projections
# --------------------

proj_raw <- scrape_data(
  season = season_year,
  week = 0,
  src = proj_sources,
  pos = proj_positions
)

proj_all <- bind_rows(proj_raw, .id = "position")

# --------------------
# 3) Add PrevWeighted rows (if present) so it influences the weighted means
# --------------------

if (prevweighted_available) {
  prev_proj <- read_csv(prevweighted_file, show_col_types = FALSE) %>%
    transmute(
      position = position,
      data_src = "PrevWeighted",
      player = full_name,
      across(any_of(stat_cols), ~ suppressWarnings(as.numeric(.x)))
    )

  proj_all <- bind_rows(proj_all, prev_proj)
}

# --------------------
# 4) Clean + weight projections
# --------------------

proj_weighted <- proj_all %>%
  mutate(full_name = clean_player_name(player)) %>%
  left_join(name_map, by = c("full_name" = "alias")) %>%
  mutate(full_name = coalesce(canonical, full_name)) %>%
  select(-canonical) %>%
  left_join(source_weights %>% select(data_src, weight), by = "data_src") %>%
  filter(!is.na(weight))

# Only summarize stat columns that exist in the combined data
stat_cols_present <- intersect(stat_cols, names(proj_weighted))

proj_weighted_clean <- proj_weighted %>%
  group_by(full_name, position) %>%
  summarize(
    n_sources = n_distinct(data_src),
    across(all_of(stat_cols_present), ~ weighted.mean(.x, weight, na.rm = TRUE)),
    .groups = "drop"
  )

# --------------------
# 5) Join roster (team, age, etc.) + write output
# --------------------

# Try to load rosters for the projection season. If that season isn't available
# in nflreadr yet, fall back to the most recent prior season that *is* available.
roster_info <- safe_load_rosters(season_year, max_lookback = 5)
roster_year_used <- roster_info$roster_year

roster <- roster_info$roster %>%
  select(player_id = gsis_id, full_name, team, position, rookie_year, birth_date)

roster_clean <- roster %>%
  mutate(clean_name = clean_player_name(full_name)) %>%
  mutate(
    age = if_else(
      !is.na(birth_date),
      interval(birth_date, Sys.Date()) %/% years(1),
      NA_integer_
    )
  ) %>%
  select(player_id, clean_name, team, position, age, rookie_year)

proj_final <- proj_weighted_clean %>%
  left_join(roster_clean, by = c("full_name" = "clean_name", "position" = "position")) %>%
  mutate(roster_season = roster_year_used) %>%
  select(player_id, full_name, team, position, age, rookie_year, roster_season, n_sources, everything())

out_file <- glue("weighted_projections_{season_year}.csv")
write_csv(proj_final, out_file)

message(glue("✅ Saved: {out_file}"))
