# v2_model.R
# ------------------------------------------------------------------------------
# XGBoost-based fantasy football projection model (PPR)
#
# Changes from previous version:
#   - ADP added as a feature (adp_rank per player, PPR scoring format)
#   - Historical ADP loaded from saved adp_YYYY.csv files (when present)
#   - Target-season ADP scraped via ffanalytics::scrape_adp(), then saved
#     so it becomes available as a training feature in future years
#   - ADP is optional: model degrades gracefully if files are missing
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

target_season    <- 2026
training_seasons <- c(2022, 2023, 2024,2025)

proj_sources   <- c("FantasyPros", "NFL", "ESPN")
proj_positions <- c("QB", "RB", "WR", "TE")

# ADP is loaded via nflreadr::load_ff_rankings() — no source config needed.

# PPR scoring
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

# XGBoost hyperparameters
xgb_params <- list(
  eta              = 0.05,
  max_depth        = 3,
  min_child_weight = 5,
  subsample        = 0.8,
  colsample_bytree = 0.8,
  gamma            = 1,
  objective        = "reg:squarederror",
  eval_metric      = "rmse"
)

xgb_nrounds    <- 500
xgb_early_stop <- 30
xgb_nfold      <- 5

# ------------------------------------------------------------------------------
# VBD config
# Baseline = the last "startable" player at each position.
# Formula: (starters_per_team × n_teams) + estimated_flex_slots_for_position
#
# 10-team | QB1 / RB2 / WR2 / TE1 / FLEX1 (RB/WR/TE)
# Flex split assumed ~4 RB, 4 WR, 2 TE across the league.
# Adjust these if your league differs.
# ------------------------------------------------------------------------------
n_teams        <- 10
vbd_baselines  <- list(QB = 10, RB = 24, WR = 24, TE = 12)

# ------------------------------------------------------------------------------
# Draft config
# draft_type:     "snake" or "traditional"
# draft_position: your pick slot (1 = first overall, n_teams = last)
# n_rounds:       how many rounds to simulate (set to your roster size)
# value_threshold: how many spots better your VBD rank must be vs. ADP rank
#                  to earn a "value" flag (market is undervaluing the player)
# ------------------------------------------------------------------------------
draft_type       <- "traditional"       # "snake" or "traditional"
draft_position   <- 6             # your draft slot
n_rounds         <- 15            # roster spots (rounds in the draft)
value_threshold  <- 10            # vbd_rank must beat adp_rank by this much

# PrevWeighted blending (post-prediction)
prevweighted_weight <- 0.15
prevweighted_year   <- target_season - 1
prevweighted_file   <- glue("weighted_projections_{prevweighted_year}.csv")

# ==============================================================================
# HELPERS
# ==============================================================================

calc_fpts <- function(df, s = scoring, prefix = "") {
  p <- function(col) paste0(prefix, col)
  safe <- function(col) if (col %in% names(df)) replace_na(df[[col]], 0) else 0
  
  safe(p("pass_yds"))     * s$pass_yds +
    safe(p("pass_tds"))   * s$pass_tds +
    safe(p("pass_int"))   * s$pass_int +
    safe(p("rush_yds"))   * s$rush_yds +
    safe(p("rush_tds"))   * s$rush_tds +
    safe(p("rec"))        * s$rec +
    safe(p("rec_yds"))    * s$rec_yds +
    safe(p("rec_tds"))    * s$rec_tds +
    safe(p("fumbles_lost")) * s$fumbles_lost
}

coalesce_cols <- function(df, target, candidates) {
  if (!target %in% names(df)) df[[target]] <- NA_real_
  for (cand in candidates)
    if (cand %in% names(df)) df[[target]] <- coalesce(df[[target]], df[[cand]])
  df
}

safe_load_rosters <- function(target_season, max_lookback = 5) {
  for (y in target_season:(target_season - max_lookback)) {
    res <- tryCatch(load_rosters(y), error = function(e) NULL)
    if (!is.null(res) && nrow(res) > 0) {
      message(glue("Loaded rosters for {y} (requested {target_season})."))
      return(list(roster = res, roster_year = y))
    }
  }
  stop(glue("Could not load rosters for {target_season} or prior {max_lookback} seasons."))
}

# ------------------------------------------------------------------------------
# Generate the overall pick numbers for a given draft slot.
#
# Snake:       odd rounds run low→high, even rounds run high→low.
#   Round 1 pick = draft_position
#   Round 2 pick = (2 * n_teams) - draft_position + 1
#   Round 3 pick = (2 * n_teams) + draft_position   ... etc.
#
# Traditional: same slot every round.
#   Pick in round r = (r - 1) * n_teams + draft_position
# ------------------------------------------------------------------------------
generate_picks <- function(draft_position, n_teams, n_rounds, draft_type = "snake") {
  rounds <- seq_len(n_rounds)
  
  if (draft_type == "snake") {
    picks <- ifelse(
      rounds %% 2 == 1,                                        # odd round: ascending
      (rounds - 1) * n_teams + draft_position,
      rounds * n_teams - draft_position + 1                    # even round: descending
    )
  } else {
    picks <- (rounds - 1) * n_teams + draft_position
  }
  
  tibble(round = rounds, pick = as.integer(picks))
}

# ------------------------------------------------------------------------------
# ADP helper: scrape + clean, return a tibble with player_id (gsis_id) + adp_rank
#
# ffanalytics::scrape_adp() returns player name + adp value.
# We map to gsis_id via ff_playerids using the same cleaned-name join that
# FantasyPros uses internally, then fall back to a fuzzy match if needed.
#
# The result is saved as adp_{season}.csv so it feeds future training runs.
# ------------------------------------------------------------------------------
get_adp <- function(season, ff_ids = NULL) {
  adp_file <- glue("adp_{season}.csv")
  
  # Return saved file if it exists (required path for training seasons —
  # load_ff_rankings() has no season argument and only returns current data)
  if (file.exists(adp_file)) {
    message(glue("  ADP: loading saved {adp_file}"))
    return(read_csv(adp_file, show_col_types = FALSE))
  }
  
  # Only attempt a live scrape for the target season
  if (season < target_season) {
    message(glue("  ADP: no saved file for {season} and cannot scrape historical — adp_rank will be NA for this season."))
    return(NULL)
  }
  
  message(glue("  ADP: loading current FantasyPros rankings via nflreadr..."))
  
  raw <- tryCatch(
    nflreadr::load_ff_rankings(type = "draft"),
    error = function(e) {
      message(glue("  ⚠️  load_ff_rankings failed: {e$message}"))
      NULL
    }
  )
  
  if (is.null(raw) || nrow(raw) == 0) {
    message("  ⚠️  No rankings returned. adp_rank will be NA.")
    return(NULL)
  }
  
  # Filter to PPR skill positions only.
  # page_type "ppr" covers QB/RB/WR/TE in PPR format; fall back to any
  # skill-position rows if "ppr" isn't present.
  skill_pos <- c("QB", "RB", "WR", "TE")
  
  ppr_rows <- raw %>% filter(str_detect(tolower(page_type), "ppr"), pos %in% skill_pos)
  
  if (nrow(ppr_rows) == 0) {
    message("  ⚠️  No PPR page_type rows found — falling back to all skill-position rows.")
    ppr_rows <- raw %>% filter(pos %in% skill_pos)
  }
  
  message(glue("  ADP: {nrow(ppr_rows)} skill-position PPR rows"))
  
  if (is.null(ff_ids)) {
    ff_ids <- nflreadr::load_ff_playerids() %>%
      transmute(
        gsis_id        = as.character(gsis_id),
        fantasypros_id = as.character(fantasypros_id)
      ) %>%
      filter(!is.na(gsis_id) & gsis_id != "",
             !is.na(fantasypros_id) & fantasypros_id != "")
  }
  
  adp_clean <- ppr_rows %>%
    transmute(
      fantasypros_id = as.character(id),
      ecr            = suppressWarnings(as.numeric(ecr))
    ) %>%
    filter(!is.na(ecr), !is.na(fantasypros_id), fantasypros_id != "") %>%
    left_join(ff_ids, by = "fantasypros_id") %>%
    filter(!is.na(gsis_id) & gsis_id != "") %>%
    rename(player_id = gsis_id) %>%
    group_by(player_id) %>%
    dplyr::slice_min(ecr, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    arrange(ecr) %>%
    mutate(adp_rank = row_number()) %>%
    select(player_id, adp_rank)
  
  n_matched <- nrow(adp_clean)
  message(glue("  ADP: {n_matched} players mapped to gsis_id for {season}."))
  
  if (n_matched == 0) {
    message("  ⚠️  No players matched. Check that fantasypros_id values align between sources.")
    return(NULL)
  }
  
  write_csv(adp_clean, adp_file)
  message(glue("  ADP: saved {adp_file}"))
  
  adp_clean
}

# ==============================================================================
# 1) BUILD TRAINING DATA
# ==============================================================================
message("=== PHASE 1: Building training data ===")

build_training_season <- function(season_yr) {
  f <- glue("stat_comparison_{season_yr}.csv")
  if (!file.exists(f)) {
    warning(glue("Missing {f} — skipping {season_yr}."))
    return(NULL)
  }
  
  sc <- read_csv(f, show_col_types = FALSE) %>%
    filter(data_src %in% proj_sources, position %in% proj_positions)
  
  if (nrow(sc) == 0) {
    warning(glue("No matching rows in {f}."))
    return(NULL)
  }
  
  # Projected fpts per source → wide
  proj_fpts <- sc %>%
    mutate(proj_fpts = calc_fpts(., scoring)) %>%
    select(player_id, position, data_src, proj_fpts) %>%
    pivot_wider(
      names_from  = data_src,
      values_from = proj_fpts,
      names_prefix = "fp_",
      values_fn = mean
    )
  
  # Actual fpts
  actual_fpts <- sc %>%
    distinct(player_id, position, .keep_all = TRUE) %>%
    transmute(
      player_id,
      position,
      actual_fpts =
        replace_na(passing_yards,  0) * scoring$pass_yds +
        replace_na(passing_tds,    0) * scoring$pass_tds +
        replace_na(interceptions,  0) * scoring$pass_int +
        replace_na(rushing_yards,  0) * scoring$rush_yds +
        replace_na(rushing_tds,    0) * scoring$rush_tds +
        replace_na(receptions,     0) * scoring$rec +
        replace_na(receiving_yards,0) * scoring$rec_yds +
        replace_na(receiving_tds,  0) * scoring$rec_tds +
        replace_na(fumbles_lost,   0) * scoring$fumbles_lost,
      games_played = replace_na(games_played, 0L)
    )
  
  # Roster metadata
  roster <- tryCatch(
    load_rosters(season_yr) %>%
      transmute(
        player_id   = as.character(gsis_id),
        birth_date,
        rookie_year
      ) %>%
      distinct(player_id, .keep_all = TRUE),
    error = function(e) {
      message(glue("  Could not load rosters for {season_yr}: {e$message}"))
      tibble(player_id = character(), birth_date = as.Date(NA), rookie_year = integer())
    }
  )
  
  season_end <- as.Date(paste0(season_yr + 1, "-02-01"))
  
  training <- proj_fpts %>%
    left_join(actual_fpts, by = c("player_id", "position")) %>%
    left_join(roster, by = "player_id") %>%
    mutate(
      season = season_yr,
      age = if_else(!is.na(birth_date),
                    as.integer(interval(birth_date, season_end) %/% years(1)),
                    NA_integer_),
      years_in_league = if_else(!is.na(rookie_year),
                                as.integer(season_yr - rookie_year),
                                NA_integer_)
    ) %>%
    select(-birth_date, -rookie_year) %>%
    filter(!is.na(actual_fpts))
  
  message(glue("  {season_yr}: {nrow(training)} rows ({n_distinct(training$player_id)} players)"))
  training
}

training_data <- map_dfr(training_seasons, build_training_season)

if (nrow(training_data) == 0)
  stop("No training data assembled. Check that stat_comparison files exist.")

# ==============================================================================
# 2) ADD PRIOR-SEASON ACTUAL FPTS FEATURE
# ==============================================================================
message("\n=== Adding prior-season actual fpts feature ===")

actuals_lookup <- training_data %>% select(player_id, season, actual_fpts) %>% distinct()

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
        replace_na(passing_yards,  0) * scoring$pass_yds +
        replace_na(passing_tds,    0) * scoring$pass_tds +
        replace_na(interceptions,  0) * scoring$pass_int +
        replace_na(rushing_yards,  0) * scoring$rush_yds +
        replace_na(rushing_tds,    0) * scoring$rush_tds +
        replace_na(receptions,     0) * scoring$rec +
        replace_na(receiving_yards,0) * scoring$rec_yds +
        replace_na(receiving_tds,  0) * scoring$rec_tds +
        replace_na(fumbles_lost,   0) * scoring$fumbles_lost
    )
  actuals_lookup <- bind_rows(actuals_lookup, extra_sc)
  message(glue("  Loaded {extra_file} for prior-season lookback."))
} else {
  message(glue("  {extra_file} not found — prior_fpts will be NA for {min(training_seasons)} cohort."))
}

training_data <- training_data %>%
  left_join(
    actuals_lookup %>%
      mutate(next_season = season + 1) %>%
      select(player_id, next_season, prior_fpts = actual_fpts),
    by = c("player_id", "season" = "next_season")
  )

# ==============================================================================
# 3) ADD ADP FEATURE TO TRAINING DATA
# ==============================================================================
message("\n=== Adding ADP feature to training data ===")

# Pre-load ff_ids once for the get_adp helper
ff_ids_for_adp <- load_ff_playerids() %>%
  transmute(
    gsis_id        = as.character(gsis_id),
    fantasypros_id = as.character(fantasypros_id)
  ) %>%
  filter(!is.na(gsis_id) & gsis_id != "",
         !is.na(fantasypros_id) & fantasypros_id != "")

adp_by_season <- map(
  training_seasons,
  ~ get_adp(.x, ff_ids = ff_ids_for_adp)
)

training_data <- training_data %>%
  left_join(
    map_dfr(training_seasons, function(yr) {
      adp <- adp_by_season[[as.character(yr)]]
      if (is.null(adp)) return(tibble(player_id = character(), season = integer(), adp_rank = numeric()))
      adp %>% mutate(season = yr)
    }),
    by = c("player_id", "season")
  )

n_adp_train <- sum(!is.na(training_data$adp_rank))
message(glue("  Training rows with ADP: {n_adp_train} / {nrow(training_data)}"))

# ==============================================================================
# 4) TRAIN XGBOOST MODELS
# ==============================================================================
message("\n=== PHASE 2: Training XGBoost models ===")

fp_cols      <- names(training_data) %>% str_subset("^fp_")
feature_cols <- c(fp_cols, "age", "years_in_league", "prior_fpts", "adp_rank")

message(glue("Features: {paste(feature_cols, collapse = ', ')}"))

models      <- list()
cv_results  <- list()

for (pos in proj_positions) {
  pos_data <- training_data %>% filter(position == pos)
  n <- nrow(pos_data)
  
  if (n < 15) {
    warning(glue("Only {n} training rows for {pos} — skipping. Will fall back to source mean."))
    next
  }
  
  message(glue("\n--- {pos}: {n} training rows ---"))
  
  feat_mat <- pos_data %>% select(all_of(feature_cols)) %>% as.matrix()
  label    <- pos_data$actual_fpts
  dtrain   <- xgb.DMatrix(data = feat_mat, label = label)
  
  set.seed(42)
  cv <- xgb.cv(
    params          = xgb_params,
    data            = dtrain,
    nrounds         = xgb_nrounds,
    nfold           = min(xgb_nfold, n),
    early_stopping_rounds = xgb_early_stop,
    verbose         = 0
  )
  
  best_round <- cv$best_iteration
  best_rmse  <- cv$evaluation_log$test_rmse_mean[best_round]
  message(glue("  Best iteration: {best_round} | CV RMSE: {round(best_rmse, 1)} fpts"))
  
  model <- xgb.train(params = xgb_params, data = dtrain, nrounds = best_round, verbose = 0)
  
  imp <- xgb.importance(model = model)
  message(glue("  Top features: {paste(head(imp$Feature, 5), collapse = ', ')}"))
  
  models[[pos]]     <- model
  cv_results[[pos]] <- list(n_train = n, best_round = best_round, cv_rmse = best_rmse, importance = imp)
}

# ==============================================================================
# 5) SCRAPE + PREPARE TARGET SEASON PROJECTIONS
# ==============================================================================
message(glue("\n=== PHASE 3: Generating predictions for {target_season} ==="))

proj_raw <- tryCatch(
  scrape_data(season = target_season, week = 0, src = proj_sources, pos = proj_positions),
  error = function(e) stop(glue("scrape_data failed: {e$message}"))
)

proj_all <- bind_rows(proj_raw, .id = "proj_position")

if (!"id"     %in% names(proj_all)) proj_all$id     <- NA
if (!"src_id" %in% names(proj_all)) proj_all$src_id <- NA

proj_all <- proj_all %>%
  { coalesce_cols(., "pass_yds",     c("pass_yards", "passing_yards", "pass_yd")) } %>%
  { coalesce_cols(., "pass_tds",     c("pass_td", "passing_tds")) } %>%
  { coalesce_cols(., "pass_int",     c("pass_ints", "ints", "interceptions")) } %>%
  { coalesce_cols(., "rush_yds",     c("rush_yards", "rushing_yards", "rush_yd")) } %>%
  { coalesce_cols(., "rush_tds",     c("rush_td", "rushing_tds")) } %>%
  { coalesce_cols(., "rec_yds",      c("receiving_yards", "rec_yards", "rec_yd")) } %>%
  { coalesce_cols(., "rec_tds",      c("receiving_tds", "rec_td")) } %>%
  { coalesce_cols(., "rec",          c("receptions")) } %>%
  { coalesce_cols(., "fumbles_lost", c("fumbleslost", "fum_lost")) }

proj_numeric_cols <- intersect(
  c("pass_yds","pass_tds","pass_int","rush_yds","rush_tds","rec_yds","rec_tds","rec","fumbles_lost"),
  names(proj_all)
)

proj_all <- proj_all %>%
  mutate(across(all_of(proj_numeric_cols), ~ parse_number(as.character(.x))))

# ID crosswalk
ff_ids <- load_ff_playerids() %>%
  transmute(
    gsis_id        = as.character(gsis_id),
    mfl_id         = as.character(mfl_id),
    fantasypros_id = as.character(fantasypros_id),
    nfl_id         = as.character(nfl_id),
    espn_id        = as.character(espn_id)
  )

ids_long <- ff_ids %>%
  pivot_longer(cols = c(fantasypros_id, nfl_id, espn_id), names_to = "id_type", values_to = "src_id") %>%
  mutate(
    data_src = recode(id_type, fantasypros_id = "FantasyPros", nfl_id = "NFL", espn_id = "ESPN"),
    src_id   = as.character(src_id)
  ) %>%
  filter(!is.na(src_id) & src_id != "") %>%
  select(data_src, src_id, gsis_id, mfl_id)

roster_info <- safe_load_rosters(target_season)
roster <- roster_info$roster %>%
  transmute(
    player_id   = as.character(gsis_id),
    full_name, team, position, birth_date, rookie_year
  ) %>%
  distinct(player_id, .keep_all = TRUE)

proj_mapped <- proj_all %>%
  mutate(data_src = as.character(data_src), mfl_id = as.character(id), src_id = as.character(src_id)) %>%
  left_join(ff_ids %>% select(mfl_id, gsis_id), by = "mfl_id") %>%
  left_join(ids_long %>% rename(gsis_id2 = gsis_id, mfl_id2 = mfl_id), by = c("data_src", "src_id")) %>%
  mutate(player_id = coalesce(gsis_id, gsis_id2)) %>%
  select(-gsis_id, -gsis_id2, -mfl_id2) %>%
  filter(!is.na(player_id) & player_id != "", data_src %in% proj_sources) %>%
  left_join(
    roster %>% select(
      player_id,
      roster_position = position,
      roster_name     = full_name,
      roster_team     = team,
      birth_date,
      rookie_year
    ),
    by = "player_id"
  ) %>%
  mutate(
    position  = coalesce(roster_position, as.character(proj_position)),
    full_name = coalesce(roster_name, player),
    team      = coalesce(roster_team,
                         if ("team.x" %in% names(.)) team.x else
                           if ("team"   %in% names(.)) team    else NA_character_)
  ) %>%
  select(-any_of(c("roster_position", "roster_name", "roster_team", "team.x", "team.y"))) %>%
  filter(position %in% proj_positions)

proj_dedup <- proj_mapped %>%
  group_by(data_src, player_id, position) %>%
  summarize(
    full_name   = first(full_name),
    team        = first(team),
    birth_date  = first(birth_date),
    rookie_year = first(rookie_year),
    across(all_of(proj_numeric_cols), ~ mean(.x, na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  mutate(across(all_of(proj_numeric_cols), ~ if_else(is.nan(.x), NA_real_, .x)))

# Projected fpts per source → wide
proj_fpts <- proj_dedup %>%
  mutate(proj_fpts = calc_fpts(., scoring)) %>%
  select(player_id, position, full_name, team, birth_date, rookie_year, data_src, proj_fpts) %>%
  pivot_wider(names_from = data_src, values_from = proj_fpts, names_prefix = "fp_", values_fn = mean)

# Metadata features
season_ref <- as.Date(paste0(target_season, "-09-01"))

proj_features <- proj_fpts %>%
  mutate(
    age = if_else(!is.na(birth_date),
                  as.integer(interval(birth_date, season_ref) %/% years(1)),
                  NA_integer_),
    years_in_league = if_else(!is.na(rookie_year),
                              as.integer(target_season - rookie_year),
                              NA_integer_)
  )

# Prior-season actuals
prior_file <- glue("stat_comparison_{target_season - 1}.csv")
if (file.exists(prior_file)) {
  prior_actuals <- read_csv(prior_file, show_col_types = FALSE) %>%
    filter(position %in% proj_positions) %>%
    distinct(player_id, position, .keep_all = TRUE) %>%
    transmute(
      player_id,
      prior_fpts =
        replace_na(passing_yards,  0) * scoring$pass_yds +
        replace_na(passing_tds,    0) * scoring$pass_tds +
        replace_na(interceptions,  0) * scoring$pass_int +
        replace_na(rushing_yards,  0) * scoring$rush_yds +
        replace_na(rushing_tds,    0) * scoring$rush_tds +
        replace_na(receptions,     0) * scoring$rec +
        replace_na(receiving_yards,0) * scoring$rec_yds +
        replace_na(receiving_tds,  0) * scoring$rec_tds +
        replace_na(fumbles_lost,   0) * scoring$fumbles_lost
    )
  proj_features <- left_join(proj_features, prior_actuals, by = "player_id")
  message(glue("  Prior season fpts loaded from {prior_file}."))
} else {
  proj_features$prior_fpts <- NA_real_
  message(glue("  {prior_file} not found — prior_fpts will be NA."))
}

# ==============================================================================
# 6) ADD ADP TO TARGET SEASON FEATURES
# ==============================================================================
message(glue("\n=== Adding ADP feature for {target_season} ==="))

adp_target <- get_adp(target_season, ff_ids = ff_ids_for_adp)
if (!is.null(adp_target)) {
  proj_features <- left_join(proj_features, adp_target, by = "player_id")
  n_adp_pred <- sum(!is.na(proj_features$adp_rank))
  message(glue("  {target_season} ADP matched: {n_adp_pred} / {nrow(proj_features)} players"))
} else {
  proj_features$adp_rank <- NA_real_
  message(glue("  No ADP for {target_season} — adp_rank will be NA."))
}

# Ensure all feature columns exist (add NA if a source fp_ col is absent)
for (col in feature_cols) {
  if (!col %in% names(proj_features)) proj_features[[col]] <- NA_real_
}

# ==============================================================================
# 7) GENERATE PREDICTIONS
# ==============================================================================

predictions <- tibble()

for (pos in proj_positions) {
  pos_proj <- proj_features %>% filter(position == pos)
  
  if (nrow(pos_proj) == 0) {
    message(glue("  No {pos} projections — skipping."))
    next
  }
  
  if (!pos %in% names(models)) {
    message(glue("  No model for {pos} — falling back to source mean."))
    pos_proj <- pos_proj %>%
      mutate(
        predicted_fpts = rowMeans(select(., any_of(fp_cols)), na.rm = TRUE),
        method = "source_mean_fallback"
      )
  } else {
    feat_mat <- pos_proj %>% select(all_of(feature_cols)) %>% as.matrix()
    pos_proj <- pos_proj %>%
      mutate(
        predicted_fpts = predict(models[[pos]], xgb.DMatrix(feat_mat)),
        method = "xgboost"
      )
    message(glue("  {pos}: {nrow(pos_proj)} players predicted (XGBoost)"))
  }
  
  predictions <- bind_rows(predictions, pos_proj)
}

# ==============================================================================
# 8) OPTIONAL: BLEND WITH PREVIOUS WEIGHTED PROJECTIONS
# ==============================================================================

prevweighted_available <- prevweighted_weight > 0 && file.exists(prevweighted_file)

if (prevweighted_available) {
  message(glue("\nBlending with {prevweighted_file} (weight={prevweighted_weight})..."))
  
  prev_fpts <- read_csv(prevweighted_file, show_col_types = FALSE) %>%
    transmute(
      player_id,
      prev_weighted_fpts =
        replace_na(pass_yds,      0) * scoring$pass_yds +
        replace_na(pass_tds,      0) * scoring$pass_tds +
        replace_na(pass_int,      0) * scoring$pass_int +
        replace_na(rush_yds,      0) * scoring$rush_yds +
        replace_na(rush_tds,      0) * scoring$rush_tds +
        replace_na(rec,           0) * scoring$rec +
        replace_na(rec_yds,       0) * scoring$rec_yds +
        replace_na(rec_tds,       0) * scoring$rec_tds +
        replace_na(fumbles_lost,  0) * scoring$fumbles_lost
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
  
  message(glue("  Blended {sum(predictions$player_id %in% prev_fpts$player_id)} players."))
} else {
  message(glue("\nPrevWeighted disabled. Expected: {prevweighted_file}"))
}

# ==============================================================================
# 9) VALUE BASED DRAFTING (VBD)
# ==============================================================================
message("\n=== Calculating VBD ===")

# For each position, find the predicted_fpts of the Nth-ranked player (the baseline).
# vbd = predicted_fpts - baseline_fpts, floored at 0.
# vbd_rank is a unified cross-position draft rank.

pos_baselines <- imap_dfr(vbd_baselines, function(baseline_n, pos) {
  ranked <- predictions %>%
    filter(position == pos) %>%
    arrange(desc(predicted_fpts))
  
  tier <- if (nrow(ranked) >= baseline_n) {
    dplyr::slice(ranked, baseline_n)
  } else {
    message(glue("  ⚠️  {pos}: fewer than {baseline_n} players predicted — using last available as baseline."))
    dplyr::slice_tail(ranked, n = 1)
  }
  
  tibble(position = pos, baseline_fpts = tier$predicted_fpts, baseline_rank = baseline_n)
})

message("Baselines:")
walk(seq_len(nrow(pos_baselines)), function(i) {
  r <- pos_baselines[i, ]
  message(glue("  {r$position}{r$baseline_rank}: {round(r$baseline_fpts, 1)} fpts"))
})

predictions <- predictions %>%
  left_join(pos_baselines, by = "position") %>%
  mutate(
    vbd       = pmax(predicted_fpts - baseline_fpts, 0),
    pos_rank  = ave(-predicted_fpts, position, FUN = function(x) rank(x, ties.method = "min"))
  ) %>%
  arrange(desc(vbd)) %>%
  mutate(vbd_rank = row_number())

# ==============================================================================
# 10) OUTPUT
# ==============================================================================

output <- predictions %>%
  select(
    vbd_rank, player_id, full_name, team, position,
    age, years_in_league,
    any_of(fp_cols),
    prior_fpts, adp_rank,
    predicted_fpts, pos_rank,
    baseline_fpts, vbd,
    any_of(c("target_round", "target_pick", "value_flag")),
    method
  ) %>%
  arrange(vbd_rank)

out_file <- glue("v2_projections_{target_season}.csv")
write_csv(output, out_file)
message(glue("\n✅ Saved: {out_file}"))

# Top 10 per position by VBD
message("\n=== Top 10 by position (VBD) ===")
for (pos in proj_positions) {
  top <- output %>% filter(position == pos) %>% head(10)
  message(glue("\n{pos}:"))
  walk(seq_len(nrow(top)), function(i) {
    r <- top[i, ]
    message(glue(
      "  {r$pos_rank}. {r$full_name} ({r$team}) — {round(r$predicted_fpts,1)} fpts | VBD: {round(r$vbd,1)} | Overall: #{r$vbd_rank} | ADP: {r$adp_rank}"
    ))
  })
}

# Diagnostics
diagnostics <- map_dfr(names(cv_results), function(p) {
  tibble(
    position     = p,
    n_train      = cv_results[[p]]$n_train,
    best_nrounds = cv_results[[p]]$best_round,
    cv_rmse      = round(cv_results[[p]]$cv_rmse, 1)
  )
})

write_csv(diagnostics, glue("v2_model_diagnostics_{target_season}.csv"))
message(glue("\n✅ Saved: v2_model_diagnostics_{target_season}.csv"))
print(diagnostics)

for (pos in names(cv_results))
  write_csv(cv_results[[pos]]$importance, glue("v2_importance_{pos}_{target_season}.csv"))

message("✅ Feature importance files saved.")
