# weightedaverages.R
#
# Creates weighted projections for a given season using historical source accuracy.
# Optionally blends in a prior season's weighted projections as a pseudo-source
# called "PrevWeighted".
#
# Changes from previous version:
#   - CBS and Yahoo removed; sources now match proccheck.R (FantasyPros, NFL, ESPN)
#   - Name-string matching replaced with ID-based crosswalk via load_ff_playerids()
#     (same approach as proccheck.R) — groups by player_id, not cleaned full_name
#   - coalesce_cols + parse_number used for robust column handling

library(tidyverse)
library(ffanalytics)
library(nflreadr)
library(lubridate)
library(glue)

# ==============================================================================
# CONFIG
# ==============================================================================

season_year        <- 2026   # season you are projecting

# How many prior source_accuracy files to use (NULL = all available)
accuracy_window    <- 3

# PrevWeighted blending: share of final blend from prior weighted projection file
# Set to 0 to disable. Typical range: 0.10 – 0.25
prevweighted_weight <- 0.15
prevweighted_year   <- season_year - 1
prevweighted_file   <- glue("weighted_projections_{prevweighted_year}.csv")

# Sources — must match proccheck.R (CBS hard-dropped; Yahoo has no accuracy history)
proj_sources   <- c("FantasyPros", "NFL", "ESPN")
proj_positions <- c("QB", "RB", "WR", "TE")

# Manual weight floor for NFL (pre-normalization) — keeps it in the blend
# even if it has low accuracy in some seasons. Set to 0 to let history decide.
manual_nfl_weight <- 0.25

# Stat columns to carry through to the output file
stat_cols <- c(
  "pass_att", "pass_comp", "pass_yds", "pass_tds", "pass_int",
  "rush_att", "rush_yds", "rush_tds",
  "rec_tgt",  "rec_yds",  "rec_tds",  "rec",
  "fumbles_lost"
)

# ==============================================================================
# HELPERS
# ==============================================================================

# Coalesce column aliases into a canonical target name.
# Mirrors the same helper in proccheck.R.
coalesce_cols <- function(df, target, candidates) {
  if (!target %in% names(df)) df[[target]] <- NA_real_
  for (cand in candidates) {
    if (cand %in% names(df))
      df[[target]] <- coalesce(df[[target]], df[[cand]])
  }
  df
}

# Safe roster loader: falls back year-by-year when the target season isn't
# published yet (common in the offseason).
safe_load_rosters <- function(target_season, max_lookback = 5) {
  for (y in target_season:(target_season - max_lookback)) {
    res <- tryCatch(load_rosters(y), error = function(e) {
      message(glue("⚠️  load_rosters({y}) failed: {e$message}"))
      NULL
    })
    if (!is.null(res) && nrow(res) > 0) {
      message(glue("ℹ️  Loaded rosters for {y} (requested {target_season})."))
      return(list(roster = res, roster_year = y))
    }
  }
  stop(glue("Could not load rosters for {target_season} or prior {max_lookback} seasons."))
}

# ==============================================================================
# 1) BUILD SOURCE WEIGHTS FROM HISTORICAL ACCURACY
# ==============================================================================

accuracy_files <- list.files(pattern = "^source_accuracy_\\d{4}\\.csv$")

accuracy_meta <- tibble(file = accuracy_files) %>%
  mutate(season = suppressWarnings(as.integer(str_extract(file, "\\d{4}")))) %>%
  filter(!is.na(season), season < season_year) %>%
  arrange(season)

if (nrow(accuracy_meta) == 0)
  stop("No source_accuracy_YYYY.csv files found for seasons prior to ", season_year, ".")

if (!is.null(accuracy_window))
  accuracy_meta <- slice_tail(accuracy_meta, n = accuracy_window)

message(glue("ℹ️  Using accuracy seasons: {paste(accuracy_meta$season, collapse = ', ')}"))

historical_accuracy <- map_dfr(accuracy_meta$file, function(f) {
  yr <- suppressWarnings(as.integer(str_extract(f, "\\d{4}")))
  read_csv(f, show_col_types = FALSE) %>% mutate(season = yr)
}) %>%
  # Keep only sources that are in our current source list
  filter(data_src %in% proj_sources)

source_weights_base <- historical_accuracy %>%
  group_by(data_src) %>%
  summarize(mean_accuracy = mean(overall_accuracy, na.rm = TRUE), .groups = "drop") %>%
  mutate(
    mean_accuracy = pmax(mean_accuracy, 0),
    weight = mean_accuracy / sum(mean_accuracy, na.rm = TRUE)
  )

# Inject manual NFL weight then re-normalize
if (!is.na(manual_nfl_weight) && manual_nfl_weight > 0) {
  source_weights_base <- source_weights_base %>%
    filter(data_src != "NFL") %>%
    bind_rows(tibble(data_src = "NFL", mean_accuracy = NA_real_, weight = manual_nfl_weight))
}

source_weights_base <- source_weights_base %>%
  mutate(weight = weight / sum(weight, na.rm = TRUE))

message("\nSource weights (before PrevWeighted):")
print(source_weights_base %>% mutate(weight_pct = round(weight * 100, 1)))

# Scale base weights down if PrevWeighted will be added
prevweighted_available <- prevweighted_weight > 0 && file.exists(prevweighted_file)

source_weights <- source_weights_base

if (prevweighted_available) {
  source_weights <- source_weights %>%
    mutate(weight = weight * (1 - prevweighted_weight)) %>%
    bind_rows(tibble(data_src = "PrevWeighted", mean_accuracy = NA_real_, weight = prevweighted_weight))
  message(glue("ℹ️  PrevWeighted enabled: {prevweighted_file} (final share = {prevweighted_weight})."))
} else {
  message(glue("ℹ️  PrevWeighted disabled. Expected: {prevweighted_file}"))
}

# ==============================================================================
# 2) SCRAPE CURRENT PROJECTIONS
# ==============================================================================

proj_raw <- scrape_data(
  season = season_year,
  week   = 0,
  src    = proj_sources,
  pos    = proj_positions
)

proj_all <- bind_rows(proj_raw, .id = "proj_position")

if (!"id"     %in% names(proj_all)) proj_all$id     <- NA
if (!"src_id" %in% names(proj_all)) proj_all$src_id <- NA

# Normalize column aliases (mirrors proccheck.R)
proj_all <- proj_all %>%
  { coalesce_cols(., "pass_att",      c("pass_attempts", "passing_attempts")) } %>%
  { coalesce_cols(., "pass_comp",     c("pass_completions", "passing_completions")) } %>%
  { coalesce_cols(., "pass_yds",      c("pass_yards", "passing_yards", "pass_yd", "PassYds")) } %>%
  { coalesce_cols(., "pass_tds",      c("pass_td", "passing_tds", "PassTD")) } %>%
  { coalesce_cols(., "pass_int",      c("pass_ints", "ints", "interceptions")) } %>%
  { coalesce_cols(., "rush_att",      c("rush_attempts", "rushing_attempts", "carries")) } %>%
  { coalesce_cols(., "rush_yds",      c("rush_yards", "rushing_yards", "rush_yd")) } %>%
  { coalesce_cols(., "rush_tds",      c("rush_td", "rushing_tds")) } %>%
  { coalesce_cols(., "rec_tgt",       c("targets")) } %>%
  { coalesce_cols(., "rec_yds",       c("receiving_yards", "rec_yards", "rec_yd")) } %>%
  { coalesce_cols(., "rec_tds",       c("receiving_tds", "rec_td")) } %>%
  { coalesce_cols(., "rec",           c("receptions")) } %>%
  { coalesce_cols(., "fumbles_lost",  c("fumbleslost", "fum_lost")) }

proj_numeric_cols <- intersect(stat_cols, names(proj_all))

proj_all <- proj_all %>%
  mutate(across(all_of(proj_numeric_cols), ~ parse_number(as.character(.x))))

# ==============================================================================
# 3) ID-BASED CROSSWALK → gsis_id
#    Mirrors proccheck.R exactly: MFL id first, then per-site src_id.
# ==============================================================================

ff_ids <- load_ff_playerids() %>%
  transmute(
    gsis_id        = as.character(gsis_id),
    mfl_id         = as.character(mfl_id),
    fantasypros_id = as.character(fantasypros_id),
    nfl_id         = as.character(nfl_id),
    espn_id        = as.character(espn_id)
  )

ids_long <- ff_ids %>%
  pivot_longer(
    cols      = c(fantasypros_id, nfl_id, espn_id),
    names_to  = "id_type",
    values_to = "src_id"
  ) %>%
  mutate(
    data_src = recode(id_type,
      fantasypros_id = "FantasyPros",
      nfl_id         = "NFL",
      espn_id        = "ESPN"
    ),
    src_id = as.character(src_id)
  ) %>%
  filter(!is.na(src_id) & src_id != "") %>%
  select(data_src, src_id, gsis_id, mfl_id)

roster_info  <- safe_load_rosters(season_year)
roster_year  <- roster_info$roster_year
roster       <- roster_info$roster %>%
  transmute(
    player_id   = as.character(gsis_id),
    full_name,
    position,
    team,
    birth_date,
    rookie_year
  ) %>%
  distinct(player_id, .keep_all = TRUE) %>%
  filter(position %in% proj_positions)

proj_mapped <- proj_all %>%
  mutate(
    data_src = as.character(data_src),
    mfl_id   = as.character(id),
    src_id   = as.character(src_id)
  ) %>%
  # Step 1: MFL id → gsis_id
  left_join(ff_ids %>% select(mfl_id, gsis_id), by = "mfl_id") %>%
  # Step 2: per-site src_id → gsis_id (fallback)
  left_join(
    ids_long %>% rename(gsis_id2 = gsis_id, mfl_id2 = mfl_id),
    by = c("data_src", "src_id")
  ) %>%
  mutate(player_id = coalesce(gsis_id, gsis_id2)) %>%
  select(-gsis_id, -gsis_id2, -mfl_id2) %>%
  filter(!is.na(player_id) & player_id != "") %>%
  filter(data_src %in% proj_sources) %>%
  # Attach roster position/name so we can filter correctly
  left_join(
    roster %>% select(player_id, position_roster = position, full_name, team),
    by = "player_id"
  ) %>%
  mutate(
    position = coalesce(position_roster, proj_position),
    full_name = coalesce(full_name, player)
  ) %>%
  filter(position %in% proj_positions)

# Deduplicate within each source (same logic as proccheck.R)
stat_cols_present <- intersect(stat_cols, names(proj_mapped))

proj_dedup <- proj_mapped %>%
  group_by(data_src, player_id, position) %>%
  summarize(
    full_name = first(full_name),
    team      = first(team),
    across(all_of(stat_cols_present), ~ mean(.x, na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  mutate(across(all_of(stat_cols_present), ~ if_else(is.nan(.x), NA_real_, .x)))

# Diagnostics: how many players per source/position resolved to an ID?
diag <- proj_dedup %>%
  count(data_src, position, name = "n_players") %>%
  arrange(data_src, position)
message("\nProjection coverage after ID crosswalk:")
print(diag)

# ==============================================================================
# 4) APPEND PREVWEIGHTED AS A PSEUDO-SOURCE (if available)
# ==============================================================================

if (prevweighted_available) {
  prev_rows <- read_csv(prevweighted_file, show_col_types = FALSE) %>%
    filter(!is.na(player_id) & player_id != "") %>%
    transmute(
      data_src  = "PrevWeighted",
      player_id = as.character(player_id),
      position,
      full_name,
      team,
      across(any_of(stat_cols), ~ suppressWarnings(as.numeric(.x)))
    ) %>%
    filter(position %in% proj_positions)

  proj_dedup <- bind_rows(proj_dedup, prev_rows)
  message(glue("ℹ️  Appended {nrow(prev_rows)} PrevWeighted rows from {prevweighted_file}."))
}

# ==============================================================================
# 5) APPLY WEIGHTS → WEIGHTED MEANS
# ==============================================================================

proj_weighted <- proj_dedup %>%
  left_join(source_weights %>% select(data_src, weight), by = "data_src") %>%
  filter(!is.na(weight))

stat_cols_final <- intersect(stat_cols, names(proj_weighted))

proj_weighted_clean <- proj_weighted %>%
  group_by(player_id, position) %>%
  summarize(
    full_name  = first(full_name[!is.na(full_name)]),
    team       = first(team[!is.na(team)]),
    n_sources  = n_distinct(data_src),
    across(all_of(stat_cols_final), ~ weighted.mean(.x, weight, na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  mutate(across(all_of(stat_cols_final), ~ if_else(is.nan(.x), NA_real_, .x)))

# ==============================================================================
# 6) ENRICH WITH ROSTER METADATA + WRITE OUTPUT
# ==============================================================================

roster_meta <- roster_info$roster %>%
  transmute(
    player_id   = as.character(gsis_id),
    team_roster = team,
    birth_date,
    rookie_year
  ) %>%
  distinct(player_id, .keep_all = TRUE) %>%
  mutate(
    age = if_else(
      !is.na(birth_date),
      as.integer(interval(birth_date, Sys.Date()) %/% years(1)),
      NA_integer_
    )
  )

proj_final <- proj_weighted_clean %>%
  left_join(roster_meta %>% select(player_id, age, rookie_year), by = "player_id") %>%
  mutate(roster_season = roster_year) %>%
  select(
    player_id, full_name, team, position,
    age, rookie_year, roster_season,
    n_sources,
    everything()
  ) %>%
  arrange(position, desc(if_else(!is.na(rec_yds), rec_yds + coalesce(rush_yds, 0), 0)))

out_file <- glue("weighted_projections_{season_year}.csv")
write_csv(proj_final, out_file)
message(glue("\n✅ Saved: {out_file} ({nrow(proj_final)} players)"))
