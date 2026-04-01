# v2_model.R
# ------------------------------------------------------------------------------
# XGBoost-based fantasy football projection model (PPR)
#
# Replaces the weighted-average approach with a trained model per position.
# Uses stat_comparison CSVs from proccheck.R as training data.
#
# Pipeline:
#   1. Build training data from historical stat_comparison files
#   2. Train one XGBoost model per position (QB, RB, WR, TE)
#   3. Generate predictions for the target season
#
# Prerequisites:
#   - stat_comparison_YYYY.csv files from proccheck.R (one per training season)
#   - Projection scrape for the target season (FantasyPros, NFL, ESPN)
#   - nflreadr for roster metadata
# ------------------------------------------------------------------------------

library(tidyverse)
library(xgboost)
library(nflreadr)
library(ffanalytics)
library(glue)
library(lubridate)

# ==============================================================================
# CONFIG
# ==============================================================================

# Season to predict
target_season <- 2026

# Training seasons (must have stat_comparison_YYYY.csv from proccheck)
training_seasons <- c(2022, 2023, 2024)

# Sources (aligned with proccheck — no CBS, no Yahoo)
proj_sources <- c("FantasyPros", "NFL", "ESPN")
proj_positions <- c("QB", "RB", "WR", "TE")

# PPR scoring weights (edit for half-PPR or standard)
scoring <- list(
  pass_yds     =  0.04,
  pass_tds     =  4,
  pass_int     = -1,
  rush_yds     =  0.1,
  rush_tds     =  6,
  rec           =  1,
  rec_yds      =  0.1,
  rec_tds      =  6,
  fumbles_lost = -2
)

# XGBoost hyperparameters (conservative for small N)
xgb_params <- list(
  eta             = 0.05,
  max_depth       = 3,
  min_child_weight = 5,
  subsample       = 0.8,
  colsample_bytree = 0.8,
  gamma           = 1,
  objective       = "reg:squarederror",
  eval_metric     = "rmse"
)

xgb_nrounds  <- 500
xgb_early_stop <- 30
xgb_nfold    <- 5

# PrevWeighted blending (applied after XGBoost prediction)
# Set to 0 to disable
prevweighted_weight <- 0.15
prevweighted_year   <- target_season - 1
prevweighted_file   <- glue("weighted_projections_{prevweighted_year}.csv")

# ==============================================================================
# HELPERS
# ==============================================================================

# Calculate fantasy points from stat columns
calc_fpts <- function(df, s = scoring, prefix = "") {
  p <- function(col) paste0(prefix, col)

  # Safely get column or return 0
  safe_col <- function(df, colname) {
    if (colname %in% names(df)) {
      replace_na(df[[colname]], 0)
    } else {
      0
    }
  }

  safe_col(df, p("pass_yds"))     * s$pass_yds +
    safe_col(df, p("pass_tds"))   * s$pass_tds +
    safe_col(df, p("pass_int"))   * s$pass_int +
    safe_col(df, p("rush_yds"))   * s$rush_yds +
    safe_col(df, p("rush_tds"))   * s$rush_tds +
    safe_col(df, p("rec"))        * s$rec +
    safe_col(df, p("rec_yds"))    * s$rec_yds +
    safe_col(df, p("rec_tds"))    * s$rec_tds +
    safe_col(df, p("fumbles_lost")) * s$fumbles_lost
}

# Safe roster loader (same as weightedaverages.R)
safe_load_rosters <- function(target_season, max_lookback = 5) {
  for (y in target_season:(target_season - max_lookback)) {
    roster_try <- tryCatch(load_rosters(y), error = function(e) NULL)
    if (!is.null(roster_try) && nrow(roster_try) > 0) {
      message(glue("Loaded rosters for {y} (requested {target_season})."))
      return(list(roster = roster_try, roster_year = y))
    }
  }
  stop(glue("Could not load rosters for {target_season} or prior {max_lookback} seasons."))
}

# ==============================================================================
# 1) BUILD TRAINING DATA
# ==============================================================================
message("=== PHASE 1: Building training data ===")

build_training_season <- function(season_yr) {
  f <- glue("stat_comparison_{season_yr}.csv")
  if (!file.exists(f)) {
    warning(glue("Missing {f} — skipping season {season_yr}."))
    return(NULL)
  }

  sc <- read_csv(f, show_col_types = FALSE) %>%
    filter(data_src %in% proj_sources, position %in% proj_positions)

  if (nrow(sc) == 0) {
    warning(glue("No matching rows in {f}."))
    return(NULL)
  }

  # --- Projected fantasy points per source ---
  # The stat_comparison file has projected stats with names like pass_yds, rush_yds, etc.
  # and actual stats with names like passing_yards, rushing_yards, etc.

  proj_fpts_by_src <- sc %>%
    mutate(proj_fpts = calc_fpts(., scoring)) %>%
    select(player_id, position, data_src, proj_fpts) %>%
    pivot_wider(
      names_from = data_src,
      values_from = proj_fpts,
      names_prefix = "fp_",
      values_fn = mean
    )

  # --- Actual fantasy points ---
  # Actual stat columns in stat_comparison use the full names from PBP
  actual_fpts <- sc %>%
    distinct(player_id, position, .keep_all = TRUE) %>%
    transmute(
      player_id,
      position,
      actual_fpts =
        replace_na(passing_yards, 0)  * scoring$pass_yds +
        replace_na(passing_tds, 0)    * scoring$pass_tds +
        replace_na(interceptions, 0)  * scoring$pass_int +
        replace_na(rushing_yards, 0)  * scoring$rush_yds +
        replace_na(rushing_tds, 0)    * scoring$rush_tds +
        replace_na(receptions, 0)     * scoring$rec +
        replace_na(receiving_yards, 0) * scoring$rec_yds +
        replace_na(receiving_tds, 0)  * scoring$rec_tds +
        replace_na(fumbles_lost, 0)   * scoring$fumbles_lost,
      games_played = replace_na(games_played, 0L)
    )

  # --- Roster metadata ---
  roster <- tryCatch(
    load_rosters(season_yr) %>%
      transmute(
        player_id = as.character(gsis_id),
        birth_date,
        rookie_year
      ) %>%
      distinct(player_id, .keep_all = TRUE),
    error = function(e) {
      message(glue("Could not load rosters for {season_yr}: {e$message}"))
      tibble(player_id = character(), birth_date = as.Date(NA), rookie_year = integer())
    }
  )

  # Season end date for age calculation (use Feb 1 of following year as proxy)
  season_end <- as.Date(paste0(season_yr + 1, "-02-01"))

  # --- Combine ---
  training <- proj_fpts_by_src %>%
    left_join(actual_fpts, by = c("player_id", "position")) %>%
    left_join(roster, by = "player_id") %>%
    mutate(
      season = season_yr,
      age = if_else(
        !is.na(birth_date),
        as.integer(interval(birth_date, season_end) %/% years(1)),
        NA_integer_
      ),
      years_in_league = if_else(
        !is.na(rookie_year),
        as.integer(season_yr - rookie_year),
        NA_integer_
      )
    ) %>%
    select(-birth_date, -rookie_year) %>%
    filter(!is.na(actual_fpts))

  message(glue("  {season_yr}: {nrow(training)} player-rows ({n_distinct(training$player_id)} unique players)"))
  training
}

training_data <- map_dfr(training_seasons, build_training_season)

if (nrow(training_data) == 0) {
  stop("No training data assembled. Check that stat_comparison files exist and contain the expected sources.")
}

message(glue("\nTotal training rows: {nrow(training_data)}"))
message(glue("Rows by position: {paste(training_data %>% count(position) %>% glue_data('{position}={n}'), collapse=', ')}"))

# ==============================================================================
# 2) LOAD PRIOR SEASON ACTUALS AS FEATURE
# ==============================================================================
# For each player in the training data, look up what they actually scored
# in the PRIOR season. This gives the model a "baseline" signal.
message("\n=== Adding prior-season actual fpts feature ===")

# Build a lookup of actual fpts by player_id + season
actuals_lookup <- training_data %>%
  select(player_id, season, actual_fpts) %>%
  distinct()

# We also need the season before the earliest training season
earliest_extra <- min(training_seasons) - 1
extra_file <- glue("stat_comparison_{earliest_extra}.csv")
if (file.exists(extra_file)) {
  extra_sc <- read_csv(extra_file, show_col_types = FALSE) %>%
    filter(position %in% proj_positions) %>%
    distinct(player_id, position, .keep_all = TRUE) %>%
    transmute(
      player_id,
      season = earliest_extra,
      actual_fpts =
        replace_na(passing_yards, 0)  * scoring$pass_yds +
        replace_na(passing_tds, 0)    * scoring$pass_tds +
        replace_na(interceptions, 0)  * scoring$pass_int +
        replace_na(rushing_yards, 0)  * scoring$rush_yds +
        replace_na(rushing_tds, 0)    * scoring$rush_tds +
        replace_na(receptions, 0)     * scoring$rec +
        replace_na(receiving_yards, 0) * scoring$rec_yds +
        replace_na(receiving_tds, 0)  * scoring$rec_tds +
        replace_na(fumbles_lost, 0)   * scoring$fumbles_lost
    )
  actuals_lookup <- bind_rows(actuals_lookup, extra_sc)
  message(glue("  Loaded {extra_file} for prior-season lookback."))
} else {
  message(glue("  {extra_file} not found — prior-season fpts will be NA for {min(training_seasons)} players."))
}

# Join prior-season actuals
training_data <- training_data %>%
  left_join(
    actuals_lookup %>% mutate(next_season = season + 1) %>% select(player_id, next_season, prior_fpts = actual_fpts),
    by = c("player_id", "season" = "next_season")
  )

message(glue("  Players with prior-season data: {sum(!is.na(training_data$prior_fpts))} / {nrow(training_data)}"))

# ==============================================================================
# 3) TRAIN XGBOOST MODELS (one per position)
# ==============================================================================
message("\n=== PHASE 2: Training XGBoost models ===")

# Feature columns (source projections + metadata)
# Dynamically detect which fp_ columns exist
fp_cols <- names(training_data) %>% str_subset("^fp_")
feature_cols <- c(fp_cols, "age", "years_in_league", "prior_fpts")

message(glue("Feature columns: {paste(feature_cols, collapse=', ')}"))

models <- list()
cv_results <- list()

for (pos in proj_positions) {
  pos_data <- training_data %>% filter(position == pos)
  n <- nrow(pos_data)

  if (n < 15) {
    warning(glue("Only {n} training rows for {pos} — skipping model. Will fall back to source mean."))
    next
  }

  message(glue("\n--- {pos}: {n} training rows across {n_distinct(pos_data$season)} seasons ---"))

  # Build matrix (XGBoost handles NA natively)
  feat_matrix <- pos_data %>% select(all_of(feature_cols)) %>% as.matrix()
  label <- pos_data$actual_fpts

  dtrain <- xgb.DMatrix(data = feat_matrix, label = label)

  # Cross-validation to find optimal nrounds
  set.seed(42)
  cv <- xgb.cv(
    params = xgb_params,
    data = dtrain,
    nrounds = xgb_nrounds,
    nfold = min(xgb_nfold, n),
    early_stopping_rounds = xgb_early_stop,
    verbose = 0
  )

  best_round <- cv$best_iteration
  best_rmse <- cv$evaluation_log$test_rmse_mean[best_round]

  message(glue("  Best iteration: {best_round} | CV RMSE: {round(best_rmse, 1)} fpts"))

  # Train final model
  model <- xgb.train(
    params = xgb_params,
    data = dtrain,
    nrounds = best_round,
    verbose = 0
  )

  # Feature importance
  imp <- xgb.importance(model = model)
  message(glue("  Top features: {paste(head(imp$Feature, 5), collapse=', ')}"))

  models[[pos]] <- model
  cv_results[[pos]] <- list(
    n_train = n,
    best_round = best_round,
    cv_rmse = best_rmse,
    importance = imp
  )
}

# ==============================================================================
# 4) SCRAPE + PREPARE TARGET SEASON PROJECTIONS
# ==============================================================================
message(glue("\n=== PHASE 3: Generating predictions for {target_season} ==="))

# Scrape projections
proj_raw <- tryCatch(
  scrape_data(season = target_season, week = 0, src = proj_sources, pos = proj_positions),
  error = function(e) {
    stop(glue("Failed to scrape projections for {target_season}: {e$message}"))
  }
)

proj_all <- bind_rows(proj_raw, .id = "proj_position")

# --- ID crosswalk (same approach as proccheck.R) ---
ff_ids <- load_ff_playerids() %>%
  transmute(
    gsis_id = as.character(gsis_id),
    mfl_id  = as.character(mfl_id),
    fantasypros_id = as.character(fantasypros_id),
    nfl_id  = as.character(nfl_id),
    espn_id = as.character(espn_id)
  )

ids_long <- ff_ids %>%
  pivot_longer(
    cols = c(fantasypros_id, nfl_id, espn_id),
    names_to = "id_type",
    values_to = "src_id"
  ) %>%
  mutate(
    data_src = recode(id_type,
      fantasypros_id = "FantasyPros",
      nfl_id = "NFL",
      espn_id = "ESPN"
    ),
    src_id = as.character(src_id)
  ) %>%
  filter(!is.na(src_id) & src_id != "") %>%
  select(data_src, src_id, gsis_id, mfl_id)

# Ensure id and src_id columns exist
if (!"id" %in% names(proj_all)) proj_all$id <- NA
if (!"src_id" %in% names(proj_all)) proj_all$src_id <- NA

# Coalesce column aliases (same logic as proccheck.R)
coalesce_cols <- function(df, target, candidates) {
  if (!target %in% names(df)) df[[target]] <- NA
  for (cand in candidates) {
    if (cand %in% names(df)) {
      df[[target]] <- coalesce(df[[target]], df[[cand]])
    }
  }
  df
}

proj_all <- proj_all %>%
  { coalesce_cols(., "pass_yds", c("pass_yards", "passing_yards", "pass_yd")) } %>%
  { coalesce_cols(., "pass_tds", c("pass_td", "passing_tds")) } %>%
  { coalesce_cols(., "pass_int", c("pass_ints", "ints", "interceptions")) } %>%
  { coalesce_cols(., "rush_yds", c("rush_yards", "rushing_yards", "rush_yd")) } %>%
  { coalesce_cols(., "rush_tds", c("rush_td", "rushing_tds")) } %>%
  { coalesce_cols(., "rec_yds",  c("receiving_yards", "rec_yards", "rec_yd")) } %>%
  { coalesce_cols(., "rec_tds",  c("receiving_tds", "rec_td")) } %>%
  { coalesce_cols(., "rec",      c("receptions")) } %>%
  { coalesce_cols(., "fumbles_lost", c("fumbleslost", "fum_lost")) }

proj_numeric_cols <- intersect(
  c("pass_yds", "pass_tds", "pass_int", "rush_yds", "rush_tds",
    "rec_yds", "rec_tds", "rec", "fumbles_lost"),
  names(proj_all)
)

proj_all <- proj_all %>%
  mutate(across(all_of(proj_numeric_cols), ~ parse_number(as.character(.x))))

# Map to gsis_id
proj_mapped <- proj_all %>%
  mutate(
    data_src = as.character(data_src),
    mfl_id   = as.character(id),
    src_id   = as.character(src_id)
  ) %>%
  left_join(ff_ids %>% select(mfl_id, gsis_id), by = "mfl_id") %>%
  left_join(ids_long %>% rename(gsis_id2 = gsis_id, mfl_id2 = mfl_id),
            by = c("data_src", "src_id")) %>%
  mutate(
    gsis_id = coalesce(gsis_id, gsis_id2),
    player_id = gsis_id
  ) %>%
  select(-gsis_id2, -mfl_id2) %>%
  filter(!is.na(player_id) & player_id != "") %>%
  filter(data_src %in% proj_sources)

# Normalize position
roster_info <- safe_load_rosters(target_season)
roster <- roster_info$roster %>%
  transmute(
    player_id = as.character(gsis_id),
    full_name,
    team,
    position,
    birth_date,
    rookie_year
  ) %>%
  distinct(player_id, .keep_all = TRUE)

proj_mapped <- proj_mapped %>%
  left_join(roster %>% select(player_id, position_roster = position, full_name, team, birth_date, rookie_year),
            by = "player_id") %>%
  mutate(position = coalesce(position_roster, as.character(proj_position))) %>%
  filter(position %in% proj_positions)

# Deduplicate within each source
proj_dedup <- proj_mapped %>%
  group_by(data_src, player_id, position) %>%
  summarize(
    full_name = first(full_name),
    team      = first(team),
    birth_date = first(birth_date),
    rookie_year = first(rookie_year),
    across(all_of(proj_numeric_cols), ~ mean(.x, na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  mutate(across(all_of(proj_numeric_cols), ~ ifelse(is.nan(.x), NA_real_, .x)))

# Calculate projected fpts per source and pivot wide
proj_fpts <- proj_dedup %>%
  mutate(proj_fpts = calc_fpts(., scoring)) %>%
  select(player_id, position, full_name, team, birth_date, rookie_year, data_src, proj_fpts) %>%
  pivot_wider(
    names_from = data_src,
    values_from = proj_fpts,
    names_prefix = "fp_",
    values_fn = mean
  )

# Add metadata features
season_ref <- as.Date(paste0(target_season, "-09-01"))

proj_features <- proj_fpts %>%
  mutate(
    age = if_else(
      !is.na(birth_date),
      as.integer(interval(birth_date, season_ref) %/% years(1)),
      NA_integer_
    ),
    years_in_league = if_else(
      !is.na(rookie_year),
      as.integer(target_season - rookie_year),
      NA_integer_
    )
  )

# Add prior-season actual fpts
# Try loading the most recent completed season's stat_comparison
prior_season <- target_season - 1
prior_file <- glue("stat_comparison_{prior_season}.csv")

if (file.exists(prior_file)) {
  prior_actuals <- read_csv(prior_file, show_col_types = FALSE) %>%
    filter(position %in% proj_positions) %>%
    distinct(player_id, position, .keep_all = TRUE) %>%
    transmute(
      player_id,
      prior_fpts =
        replace_na(passing_yards, 0)  * scoring$pass_yds +
        replace_na(passing_tds, 0)    * scoring$pass_tds +
        replace_na(interceptions, 0)  * scoring$pass_int +
        replace_na(rushing_yards, 0)  * scoring$rush_yds +
        replace_na(rushing_tds, 0)    * scoring$rush_tds +
        replace_na(receptions, 0)     * scoring$rec +
        replace_na(receiving_yards, 0) * scoring$rec_yds +
        replace_na(receiving_tds, 0)  * scoring$rec_tds +
        replace_na(fumbles_lost, 0)   * scoring$fumbles_lost
    )

  proj_features <- proj_features %>%
    left_join(prior_actuals, by = "player_id")

  message(glue("  Prior season fpts loaded from {prior_file}."))
} else {
  proj_features$prior_fpts <- NA_real_
  message(glue("  {prior_file} not found — prior_fpts will be NA for all players."))
}

# ==============================================================================
# 5) GENERATE PREDICTIONS
# ==============================================================================

# Ensure feature columns match training (add any missing fp_ cols as NA)
for (col in feature_cols) {
  if (!col %in% names(proj_features)) {
    proj_features[[col]] <- NA_real_
  }
}

predictions <- tibble()

for (pos in proj_positions) {
  pos_proj <- proj_features %>% filter(position == pos)

  if (nrow(pos_proj) == 0) {
    message(glue("  No {pos} projections found — skipping."))
    next
  }

  if (!pos %in% names(models)) {
    # Fallback: simple mean of available source projections
    message(glue("  No model for {pos} — falling back to source mean."))
    pos_proj <- pos_proj %>%
      mutate(
        predicted_fpts = rowMeans(select(., any_of(fp_cols)), na.rm = TRUE),
        method = "source_mean_fallback"
      )
  } else {
    feat_matrix <- pos_proj %>% select(all_of(feature_cols)) %>% as.matrix()
    dtest <- xgb.DMatrix(data = feat_matrix)

    pos_proj <- pos_proj %>%
      mutate(
        predicted_fpts = predict(models[[pos]], dtest),
        method = "xgboost"
      )

    message(glue("  {pos}: {nrow(pos_proj)} players predicted (XGBoost)"))
  }

  predictions <- bind_rows(predictions, pos_proj)
}

# ==============================================================================
# 6) OPTIONAL: BLEND WITH PREVIOUS WEIGHTED PROJECTIONS
# ==============================================================================

prevweighted_available <- (prevweighted_weight > 0) && file.exists(prevweighted_file)

if (prevweighted_available) {
  message(glue("\nBlending with {prevweighted_file} (weight={prevweighted_weight})..."))

  prev <- read_csv(prevweighted_file, show_col_types = FALSE)

  # Calculate fpts from prior weighted projections
  prev_fpts <- prev %>%
    transmute(
      player_id,
      prev_weighted_fpts =
        replace_na(pass_yds, 0)     * scoring$pass_yds +
        replace_na(pass_tds, 0)     * scoring$pass_tds +
        replace_na(pass_int, 0)     * scoring$pass_int +
        replace_na(rush_yds, 0)     * scoring$rush_yds +
        replace_na(rush_tds, 0)     * scoring$rush_tds +
        replace_na(rec, 0)          * scoring$rec +
        replace_na(rec_yds, 0)      * scoring$rec_yds +
        replace_na(rec_tds, 0)      * scoring$rec_tds +
        replace_na(fumbles_lost, 0) * scoring$fumbles_lost
    ) %>%
    filter(!is.na(player_id))

  predictions <- predictions %>%
    left_join(prev_fpts, by = "player_id") %>%
    mutate(
      predicted_fpts = if_else(
        !is.na(prev_weighted_fpts),
        (1 - prevweighted_weight) * predicted_fpts + prevweighted_weight * prev_weighted_fpts,
        predicted_fpts
      )
    ) %>%
    select(-prev_weighted_fpts)

  n_blended <- sum(!is.na(predictions$player_id) & predictions$player_id %in% prev_fpts$player_id)
  message(glue("  Blended {n_blended} players with prior projections."))
} else {
  message(glue("\nPrevWeighted disabled (missing file or weight=0). Expected: {prevweighted_file}"))
}

# ==============================================================================
# 7) FINAL OUTPUT
# ==============================================================================

output <- predictions %>%
  select(
    player_id, full_name, team, position,
    age, years_in_league,
    any_of(fp_cols),         # source-level projected fpts (for transparency)
    prior_fpts,
    predicted_fpts,
    method
  ) %>%
  arrange(position, desc(predicted_fpts))

out_file <- glue("v2_projections_{target_season}.csv")
write_csv(output, out_file)
message(glue("\n✅ Saved: {out_file}"))

# Print top players per position
message("\n=== Top 10 by position ===")
for (pos in proj_positions) {
  pos_top <- output %>% filter(position == pos) %>% head(10)
  message(glue("\n{pos}:"))
  for (i in 1:nrow(pos_top)) {
    r <- pos_top[i, ]
    message(glue("  {i}. {r$full_name} ({r$team}) — {round(r$predicted_fpts, 1)} fpts"))
  }
}

# Save model diagnostics
diagnostics <- map_dfr(names(cv_results), function(pos) {
  tibble(
    position = pos,
    n_training_rows = cv_results[[pos]]$n_train,
    best_nrounds = cv_results[[pos]]$best_round,
    cv_rmse = round(cv_results[[pos]]$cv_rmse, 1)
  )
})

write_csv(diagnostics, glue("v2_model_diagnostics_{target_season}.csv"))
message(glue("\n✅ Saved: v2_model_diagnostics_{target_season}.csv"))
message("\nModel diagnostics:")
print(diagnostics)

# Save feature importance per position
for (pos in names(cv_results)) {
  imp_file <- glue("v2_importance_{pos}_{target_season}.csv")
  write_csv(cv_results[[pos]]$importance, imp_file)
}
message(glue("✅ Feature importance files saved."))
