# weightedaverages.R (UPDATED)
# - No Yahoo
# - CBS "season" projections = sum weekly projections across REG weeks (compiled)
# - Crosswalk uses mfl_id and per-site ids (including cbs_id) to get gsis_id
# - Dedupe within each source before weighting (prevents double-counting)
# - Outputs weighted_projections_<season>.csv

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(purrr)
  library(stringr)
  library(readr)
  library(ffanalytics)
  library(nflreadr)
  library(lubridate)
  library(tibble)
})

clean_player_name <- function(name) {
  name %>%
    str_to_lower() %>%
    str_replace_all("\\b(jr|sr|ii|iii|iv|v)\\b", "") %>%
    str_replace_all("[^a-z ]", "") %>%
    str_squish()
}

first_non_na_chr <- function(x) {
  x <- x[!is.na(x) & x != ""]
  if (length(x) == 0) NA_character_ else x[[1]]
}

ensure_numeric_cols <- function(df, cols) {
  missing <- setdiff(cols, names(df))
  if (length(missing) > 0) {
    for (m in missing) df[[m]] <- NA_real_
  }
  df
}

parse_numeric_cols <- function(df, cols) {
  cols <- intersect(cols, names(df))
  if (length(cols) == 0) return(df)
  df %>%
    mutate(across(all_of(cols), ~ readr::parse_number(as.character(.x))))
}

PROJ_STAT_COLS <- c(
  "pass_att","pass_comp","pass_yds","pass_tds","pass_int",
  "rush_att","rush_yds","rush_tds",
  "rec_tgt","rec","rec_yds","rec_tds",
  "fumbles_lost"
)

# --- Same robust CBS compiler as proccheck ---
scrape_cbs_season_from_weekly <- function(
  season,
  pos = c("QB","RB","WR","TE"),
  cbs_week0_is_week1 = TRUE,
  sleep = 0.25,
  cache_dir = "cache_ffanalytics",
  use_cache = TRUE
) {
  dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)

  season_games <- ifelse(season >= 2021, 17L, 16L)
  reg_weeks <- season_games + 1L

  week_args <- if (cbs_week0_is_week1) 0:(reg_weeks - 1L) else 1:reg_weeks
  proj_weeks <- 1:reg_weeks

  cache_file <- file.path(cache_dir, sprintf("cbs_weekly_%s.rds", season))

  if (use_cache && file.exists(cache_file)) {
    weekly_raw <- readRDS(cache_file)
  } else {
    weekly_raw <- purrr::map2_dfr(week_args, proj_weeks, function(w_arg, w_real) {
      message(sprintf("CBS scrape: season=%s week_arg=%s (proj_week=%s)", season, w_arg, w_real))
      res <- tryCatch(
        ffanalytics::scrape_data(season = season, week = w_arg, src = "CBS", pos = pos),
        error = function(e) { warning("CBS scrape failed: ", e$message); NULL }
      )
      if (is.null(res)) return(tibble())
      dplyr::bind_rows(res, .id = "pos_from_list") %>%
        mutate(proj_week = w_real) %>%
        { if (sleep > 0) { Sys.sleep(sleep); . } else . }
    })
    saveRDS(weekly_raw, cache_file)
  }

  if (nrow(weekly_raw) == 0) return(tibble())

  if (!"pos" %in% names(weekly_raw)) weekly_raw$pos <- weekly_raw$pos_from_list
  if (!"team" %in% names(weekly_raw)) weekly_raw$team <- NA_character_
  if (!"id" %in% names(weekly_raw)) weekly_raw$id <- NA_character_
  if (!"src_id" %in% names(weekly_raw)) weekly_raw$src_id <- NA_character_
  if (!"data_src" %in% names(weekly_raw)) weekly_raw$data_src <- "CBS"

  weekly_raw <- weekly_raw %>%
    filter(data_src == "CBS") %>%
    mutate(
      data_src = "CBS",
      pos      = as.character(pos),
      team     = as.character(team),
      player   = as.character(player),
      mfl_id   = as.character(id),
      cbs_id   = as.character(src_id),
      name_clean = clean_player_name(player),
      cbs_key = case_when(
        !is.na(mfl_id) & mfl_id != "" ~ paste0("mfl|", mfl_id),
        !is.na(cbs_id) & cbs_id != "" ~ paste0("cbs|", cbs_id),
        TRUE ~ paste0("name|", name_clean, "|", pos, "|", team)
      )
    ) %>%
    ensure_numeric_cols(PROJ_STAT_COLS) %>%
    parse_numeric_cols(PROJ_STAT_COLS)

  weekly <- weekly_raw %>%
    group_by(pos, cbs_key, proj_week) %>%
    summarize(
      data_src = "CBS",
      id       = first_non_na_chr(mfl_id),
      src_id   = first_non_na_chr(cbs_id),
      player   = first(player),
      team     = first(team),
      across(all_of(PROJ_STAT_COLS), ~ mean(.x, na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    mutate(across(all_of(PROJ_STAT_COLS), ~ ifelse(is.nan(.x), NA_real_, .x)))

  weekly %>%
    group_by(pos, cbs_key) %>%
    summarize(
      data_src = "CBS",
      id       = first_non_na_chr(id),
      src_id   = first_non_na_chr(src_id),
      player   = first(player),
      team     = first(team),
      weeks_summed = n_distinct(proj_week),
      across(all_of(PROJ_STAT_COLS), ~ sum(.x, na.rm = TRUE)),
      .groups = "drop"
    )
}

# -------------------------
# Config
# -------------------------
target_season <- 2025
target_pos    <- c("QB","RB","WR","TE")

# Season-long sources from ffanalytics (CBS handled separately)
src_season <- c("FantasyPros", "NFL", "ESPN")
include_cbs <- TRUE

# -------------------------
# Build weights from your historical source_accuracy_*.csv files
# (Make sure you re-run proccheck for those years after this CBS fix)
# -------------------------
accuracy_2022 <- readr::read_csv("source_accuracy_2022.csv") %>% mutate(season = 2022)
accuracy_2023 <- readr::read_csv("source_accuracy_2023.csv") %>% mutate(season = 2023)
accuracy_2024 <- readr::read_csv("source_accuracy_2024.csv") %>% mutate(season = 2024)

historical_accuracy <- bind_rows(accuracy_2022, accuracy_2023, accuracy_2024)

target_sources_all <- c("FantasyPros","CBS","NFL","ESPN")

source_weights <- historical_accuracy %>%
  filter(data_src %in% target_sources_all) %>%
  group_by(data_src) %>%
  summarize(mean_accuracy = mean(overall_accuracy, na.rm = TRUE), .groups = "drop") %>%
  mutate(
    mean_accuracy = pmax(mean_accuracy, 0),
    weight = mean_accuracy / sum(mean_accuracy, na.rm = TRUE)
  )

# Optional fixed NFL weight (uncomment if you still want it)
# fixed_nfl_weight <- 0.25
# non_nfl <- source_weights %>% filter(data_src != "NFL")
# denom <- sum(non_nfl$mean_accuracy, na.rm = TRUE)
# non_nfl <- non_nfl %>% mutate(weight = ifelse(denom > 0, (mean_accuracy/denom) * (1-fixed_nfl_weight), (1-fixed_nfl_weight)/n()))
# source_weights <- bind_rows(non_nfl, tibble(data_src="NFL", mean_accuracy=NA_real_, weight=fixed_nfl_weight))

# -------------------------
# Name map (keep your overrides)
# -------------------------
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

# -------------------------
# Rosters + ID maps
# -------------------------
roster <- nflreadr::load_rosters(target_season) %>%
  mutate(clean_name = clean_player_name(full_name)) %>%
  transmute(
    gsis_id = as.character(gsis_id),
    roster_full_name = full_name,
    clean_name,
    roster_team = team,
    roster_position = position,
    rookie_year,
    birth_date,
    age = lubridate::interval(birth_date, Sys.Date()) %/% lubridate::years(1)
  )

ff_ids <- nflreadr::load_ff_playerids() %>%
  transmute(
    mfl_id = as.character(mfl_id),
    gsis_id = as.character(gsis_id),
    fantasypros_id = as.character(fantasypros_id),
    cbs_id = as.character(cbs_id),
    nfl_id = as.character(nfl_id),
    espn_id = as.character(espn_id)
  )

ids_long <- ff_ids %>%
  pivot_longer(
    cols = c(fantasypros_id, cbs_id, nfl_id, espn_id),
    names_to = "id_type",
    values_to = "src_id"
  ) %>%
  mutate(
    data_src = recode(
      id_type,
      fantasypros_id = "FantasyPros",
      cbs_id = "CBS",
      nfl_id = "NFL",
      espn_id = "ESPN"
    ),
    src_id = as.character(src_id)
  ) %>%
  filter(!is.na(src_id) & src_id != "") %>%
  select(data_src, src_id, mfl_id, gsis_id)

# -------------------------
# Scrape projections
# -------------------------
proj_other <- ffanalytics::scrape_data(
  season = target_season,
  week   = 0,
  src    = src_season,
  pos    = target_pos
)
proj_other_all <- bind_rows(proj_other, .id = "pos_from_list")

cbs_season <- tibble()
if (include_cbs) {
  cbs_season <- scrape_cbs_season_from_weekly(
    season = target_season,
    pos = target_pos,
    cbs_week0_is_week1 = TRUE,
    sleep = 0.25,
    cache_dir = "cache_ffanalytics",
    use_cache = TRUE
  )
}

proj_all <- bind_rows(proj_other_all, cbs_season)

if (!"pos" %in% names(proj_all)) proj_all$pos <- proj_all$pos_from_list
if (!"team" %in% names(proj_all)) proj_all$team <- NA_character_
if (!"id" %in% names(proj_all)) proj_all$id <- NA_character_
if (!"src_id" %in% names(proj_all)) proj_all$src_id <- NA_character_

proj_all <- proj_all %>%
  mutate(
    data_src = as.character(data_src),
    position = as.character(pos),
    mfl_id   = as.character(id),
    src_id   = as.character(src_id),
    team     = as.character(team),
    player   = as.character(player)
  ) %>%
  ensure_numeric_cols(PROJ_STAT_COLS) %>%
  parse_numeric_cols(PROJ_STAT_COLS)

# -------------------------
# Clean + crosswalk IDs + dedupe within-source
# -------------------------
proj_tagged <- proj_all %>%
  mutate(
    full_name = clean_player_name(player)
  ) %>%
  left_join(name_map, by = c("full_name" = "alias")) %>%
  mutate(full_name = coalesce(canonical, full_name)) %>%
  select(-canonical) %>%
  left_join(ff_ids %>% select(mfl_id, gsis_id), by = "mfl_id") %>%
  left_join(ids_long %>% rename(mfl_id_2 = mfl_id, gsis_id_2 = gsis_id),
            by = c("data_src","src_id")) %>%
  mutate(
    mfl_id  = coalesce(mfl_id, mfl_id_2),
    gsis_id = coalesce(gsis_id, gsis_id_2)
  ) %>%
  select(-mfl_id_2, -gsis_id_2) %>%
  left_join(roster, by = "gsis_id") %>%
  mutate(
    position = coalesce(roster_position, position),
    team     = coalesce(roster_team, team),
    player_key = case_when(
      !is.na(gsis_id) & gsis_id != "" ~ paste0("gsis|", gsis_id),
      !is.na(mfl_id)  & mfl_id  != "" ~ paste0("mfl|",  mfl_id),
      !is.na(src_id)  & src_id  != "" ~ paste0("src|", data_src, "|", src_id),
      TRUE ~ paste0("name|", full_name, "|", position, "|", team)
    )
  )

proj_one_per_source <- proj_tagged %>%
  group_by(data_src, player_key, position) %>%
  summarize(
    gsis_id   = first_non_na_chr(gsis_id),
    mfl_id    = first_non_na_chr(mfl_id),
    full_name = first_non_na_chr(roster_full_name),
    clean_name = first_non_na_chr(full_name),
    team      = first_non_na_chr(team),
    age       = suppressWarnings(as.integer(first(age))),
    rookie_year = suppressWarnings(as.integer(first(rookie_year))),
    across(all_of(PROJ_STAT_COLS), ~ mean(.x, na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  mutate(across(all_of(PROJ_STAT_COLS), ~ ifelse(is.nan(.x), NA_real_, .x)))

# -------------------------
# Weight + aggregate
# -------------------------
proj_weighted <- proj_one_per_source %>%
  left_join(source_weights %>% select(data_src, weight), by = "data_src") %>%
  filter(!is.na(weight))

proj_weighted_final <- proj_weighted %>%
  group_by(player_key, position) %>%
  summarize(
    player_id = first_non_na_chr(gsis_id),
    mfl_id    = first_non_na_chr(mfl_id),
    full_name = first_non_na_chr(full_name),
    team      = first_non_na_chr(team),
    age       = suppressWarnings(as.integer(first(age))),
    rookie_year = suppressWarnings(as.integer(first(rookie_year))),
    n_sources = n(),
    across(all_of(PROJ_STAT_COLS), ~ weighted.mean(.x, w = weight, na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  mutate(across(all_of(PROJ_STAT_COLS), ~ ifelse(is.nan(.x), NA_real_, .x))) %>%
  distinct(player_id, mfl_id, position, .keep_all = TRUE)

write_csv(proj_weighted_final, paste0("weighted_projections_", target_season, ".csv"))
