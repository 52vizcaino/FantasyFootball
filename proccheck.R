# proccheck.R (UPDATED)
# - Crosswalk projections to gsis_id using nflreadr::load_ff_playerids()
# - CBS: week=0 acts like weekly; compile season totals by summing weekly projections
# - Fix "weird CBS numbers" by preventing NA-id collapse (robust CBS key) and mapping via cbs_id
# - Availability metrics from ALL rostered players:
#     game played = total snaps >= 10 (preferred, via load_snap_counts)
#     fallback if snaps missing for player: 5+ pass att OR 5+ rush att OR 5+ targets
#     and if no targets then 1+ catch
# - Accuracy filter: only players who played all possible games (17 for 2021+, else 16)

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(purrr)
  library(stringr)
  library(readr)
  library(glue)
  library(nflfastR)
  library(nflreadr)
  library(ffanalytics)
  library(tibble)
})

# -------------------------
# Helpers
# -------------------------
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

# -------------------------
# CBS: build season totals from weekly projections (ROBUST KEY)
# -------------------------
scrape_cbs_season_from_weekly <- function(
  season,
  pos = c("QB","RB","WR","TE"),
  # Based on your observation: week=0 returns week 1 for CBS in your environment
  cbs_week0_is_week1 = TRUE,
  sleep = 0.25,
  cache_dir = "cache_ffanalytics",
  use_cache = TRUE
) {
  dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)

  season_games <- ifelse(season >= 2021, 17L, 16L)
  reg_weeks <- season_games + 1L  # 18 weeks in 17-game era, 17 weeks in 16-game era

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
        error = function(e) {
          warning(sprintf("CBS scrape failed for week_arg=%s: %s", w_arg, e$message))
          NULL
        }
      )
      if (is.null(res)) return(tibble())

      dplyr::bind_rows(res, .id = "pos_from_list") %>%
        mutate(proj_week = w_real) %>%
        { if (sleep > 0) { Sys.sleep(sleep); . } else . }
    })

    saveRDS(weekly_raw, cache_file)
  }

  if (nrow(weekly_raw) == 0) {
    return(list(weekly = tibble(), season = tibble(), reg_weeks = reg_weeks))
  }

  # Normalize columns
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
      # Robust key to prevent NA-id collapse:
      cbs_key = case_when(
        !is.na(mfl_id) & mfl_id != "" ~ paste0("mfl|", mfl_id),
        !is.na(cbs_id) & cbs_id != "" ~ paste0("cbs|", cbs_id),
        TRUE ~ paste0("name|", name_clean, "|", pos, "|", team)
      )
    ) %>%
    ensure_numeric_cols(PROJ_STAT_COLS) %>%
    parse_numeric_cols(PROJ_STAT_COLS)

  # 1 row per (player, week) to avoid double counting inside a week
  weekly <- weekly_raw %>%
    group_by(pos, cbs_key, proj_week) %>%
    summarize(
      data_src = "CBS",
      id       = first_non_na_chr(mfl_id),  # MFL id if present
      src_id   = first_non_na_chr(cbs_id),  # CBS id if present
      player   = first(player),
      team     = first(team),
      across(all_of(PROJ_STAT_COLS), ~ mean(.x, na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    mutate(across(all_of(PROJ_STAT_COLS), ~ ifelse(is.nan(.x), NA_real_, .x)))

  # Season totals = sum across weeks
  season_totals <- weekly %>%
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

  list(weekly = weekly, season = season_totals, reg_weeks = reg_weeks)
}

# -------------------------
# Games played: snaps preferred, pbp fallback
# -------------------------
games_played_from_pbp <- function(pbp,
                                  pass_att_thr = 5,
                                  rush_att_thr = 5,
                                  targets_thr  = 5,
                                  rec_if_no_targets_thr = 1) {
  pass_g <- pbp %>%
    filter(!is.na(passer_player_id)) %>%
    group_by(game_id, gsis_id = passer_player_id) %>%
    summarize(pass_att = sum(pass_attempt, na.rm = TRUE), .groups = "drop")

  rush_g <- pbp %>%
    filter(!is.na(rusher_player_id)) %>%
    group_by(game_id, gsis_id = rusher_player_id) %>%
    summarize(rush_att = sum(rush_attempt, na.rm = TRUE), .groups = "drop")

  tgt_g <- pbp %>%
    filter(pass == 1, !is.na(receiver_player_id)) %>%
    group_by(game_id, gsis_id = receiver_player_id) %>%
    summarize(targets = n(), .groups = "drop")

  rec_g <- pbp %>%
    filter(pass == 1, complete_pass == 1, !is.na(receiver_player_id)) %>%
    group_by(game_id, gsis_id = receiver_player_id) %>%
    summarize(receptions = n(), .groups = "drop")

  g <- list(pass_g, rush_g, tgt_g, rec_g) %>%
    reduce(full_join, by = c("game_id", "gsis_id")) %>%
    mutate(
      pass_att   = replace_na(pass_att, 0),
      rush_att   = replace_na(rush_att, 0),
      targets    = replace_na(targets, 0),
      receptions = replace_na(receptions, 0)
    ) %>%
    mutate(
      played_game_fallback =
        pass_att >= pass_att_thr |
        rush_att >= rush_att_thr |
        targets  >= targets_thr  |
        # "If no targets then 1+ catch"
        ((is.na(targets) | targets == 0) & receptions >= rec_if_no_targets_thr)
    )

  g %>%
    group_by(gsis_id) %>%
    summarize(games_played_pbp = sum(played_game_fallback, na.rm = TRUE), .groups = "drop")
}

games_played_from_snaps <- function(season_year, snap_thr = 10) {
  # load_snap_counts() has offense/defense/st snaps and pfr_player_id :contentReference[oaicite:2]{index=2}
  snaps <- nflreadr::load_snap_counts(seasons = season_year) %>%
    filter(season == season_year, game_type == "REG")

  # load_players() includes pfr_id + gsis_id :contentReference[oaicite:3]{index=3}
  players <- nflreadr::load_players() %>%
    select(gsis_id, pfr_id)

  snaps2 <- snaps %>%
    left_join(players, by = c("pfr_player_id" = "pfr_id")) %>%
    mutate(
      offense_snaps = replace_na(offense_snaps, 0),
      defense_snaps = replace_na(defense_snaps, 0),
      st_snaps      = replace_na(st_snaps, 0),
      total_snaps   = offense_snaps + defense_snaps + st_snaps,
      played_game_snaps = total_snaps >= snap_thr
    ) %>%
    filter(!is.na(gsis_id) & gsis_id != "")

  snaps2 %>%
    group_by(gsis_id) %>%
    summarize(games_played_snaps = sum(played_game_snaps, na.rm = TRUE), .groups = "drop")
}

# -------------------------
# Main
# -------------------------
run_projection_accuracy <- function(
  season_year,
  pos = c("QB","RB","WR","TE"),
  # Season-long sources from ffanalytics via week=0 (CBS is handled separately)
  src_season = c("FantasyPros", "NFL", "ESPN"),
  include_cbs = TRUE,
  # Filters
  require_full_season = TRUE,
  snap_threshold = 10,
  # pbp fallback thresholds
  fallback_pass_att = 5,
  fallback_rush_att = 5,
  fallback_targets  = 5,
  fallback_receptions_if_no_targets = 1,
  # CBS controls
  cbs_week0_is_week1 = TRUE,
  cbs_cache_dir = "cache_ffanalytics",
  cbs_use_cache = TRUE,
  cbs_sleep = 0.25,
  # outputs
  write_files = TRUE
) {

  message(glue("Running projection accuracy analysis for {season_year}..."))

  season_games <- ifelse(season_year >= 2021, 17L, 16L)
  thr_75 <- ceiling(0.75 * season_games)

  # -------------------------
  # Load data
  # -------------------------
  roster <- nflreadr::load_rosters(season_year) %>%
    transmute(
      gsis_id = as.character(gsis_id),
      full_name,
      position,
      team,
      rookie_year
    )

  pbp <- nflfastR::load_pbp(season_year) %>%
    filter(season_type == "REG")

  # -------------------------
  # Actual season totals
  # -------------------------
  pass_stats <- pbp %>%
    filter(!is.na(passer_player_id)) %>%
    group_by(player_id = passer_player_id) %>%
    summarise(
      passing_yards = sum(passing_yards, na.rm = TRUE),
      passing_tds   = sum(pass_touchdown, na.rm = TRUE),
      interceptions = sum(interception, na.rm = TRUE),
      pass_attempts = sum(pass_attempt, na.rm = TRUE),
      completions   = sum(complete_pass, na.rm = TRUE),
      .groups = "drop"
    )

  rush_stats <- pbp %>%
    filter(!is.na(rusher_player_id)) %>%
    group_by(player_id = rusher_player_id) %>%
    summarise(
      rushing_yards    = sum(rushing_yards, na.rm = TRUE),
      rushing_tds      = sum(rush_touchdown, na.rm = TRUE),
      rushing_attempts = sum(rush_attempt, na.rm = TRUE),
      fumbles          = sum(fumble, na.rm = TRUE),
      fumbles_lost     = sum(fumble_lost, na.rm = TRUE),
      .groups = "drop"
    )

  recv_stats <- pbp %>%
    filter(pass == 1, complete_pass == 1, !is.na(receiver_player_id)) %>%
    group_by(player_id = receiver_player_id) %>%
    summarise(
      receiving_yards = sum(receiving_yards, na.rm = TRUE),
      receiving_tds   = sum(touchdown, na.rm = TRUE),
      receptions      = n(),
      .groups = "drop"
    )

  season_stats <- list(pass_stats, rush_stats, recv_stats) %>%
    reduce(full_join, by = "player_id") %>%
    replace_na(list(
      passing_yards = 0, passing_tds = 0, interceptions = 0, pass_attempts = 0, completions = 0,
      rushing_yards = 0, rushing_tds = 0, rushing_attempts = 0, fumbles = 0, fumbles_lost = 0,
      receiving_yards = 0, receiving_tds = 0, receptions = 0
    )) %>%
    mutate(player_id = as.character(player_id)) %>%
    left_join(roster, by = c("player_id" = "gsis_id")) %>%
    relocate(full_name, team, position, .after = player_id)

  # -------------------------
  # Availability for ALL rostered players
  # -------------------------
  snaps_games <- tryCatch(
    games_played_from_snaps(season_year, snap_thr = snap_threshold),
    error = function(e) {
      warning("Snap counts load failed; using pbp fallback only. Error: ", e$message)
      tibble(gsis_id = character(), games_played_snaps = integer())
    }
  )

  pbp_games <- games_played_from_pbp(
    pbp,
    pass_att_thr = fallback_pass_att,
    rush_att_thr = fallback_rush_att,
    targets_thr  = fallback_targets,
    rec_if_no_targets_thr = fallback_receptions_if_no_targets
  )

  games_played <- full_join(snaps_games, pbp_games, by = "gsis_id") %>%
    mutate(
      games_played = coalesce(games_played_snaps, games_played_pbp, 0L),
      games_played = as.integer(games_played)
    ) %>%
    select(gsis_id, games_played)

  roster_avail <- roster %>%
    left_join(games_played, by = "gsis_id") %>%
    mutate(
      games_played = replace_na(games_played, 0L),
      played_all_games = games_played >= season_games,
      under_75pct_games = games_played < thr_75
    )

  availability_summary <- roster_avail %>%
    summarize(
      season = season_year,
      possible_games = season_games,
      n_rostered_players = n(),
      n_played_all_games = sum(played_all_games, na.rm = TRUE),
      n_under_75pct_games = sum(under_75pct_games, na.rm = TRUE),
      pct_under_75pct_games = round(100 * mean(under_75pct_games, na.rm = TRUE), 1)
    )

  availability_by_position <- roster_avail %>%
    group_by(position) %>%
    summarize(
      season = season_year,
      possible_games = season_games,
      n_rostered_players = n(),
      n_played_all_games = sum(played_all_games, na.rm = TRUE),
      n_under_75pct_games = sum(under_75pct_games, na.rm = TRUE),
      pct_under_75pct_games = round(100 * mean(under_75pct_games, na.rm = TRUE), 1),
      .groups = "drop"
    ) %>%
    arrange(desc(n_rostered_players))

  if (write_files) {
    write_csv(availability_summary, paste0("availability_summary_", season_year, ".csv"))
    write_csv(availability_by_position, paste0("availability_by_position_", season_year, ".csv"))
  }

  # -------------------------
  # Scrape projections
  # -------------------------
  proj_other <- ffanalytics::scrape_data(
    season = season_year,
    week   = 0,
    src    = src_season,
    pos    = pos
  )
  proj_other_all <- bind_rows(proj_other, .id = "pos_from_list")

  cbs_season <- tibble()
  if (include_cbs) {
    cbs_season <- scrape_cbs_season_from_weekly(
      season = season_year,
      pos = pos,
      cbs_week0_is_week1 = cbs_week0_is_week1,
      sleep = cbs_sleep,
      cache_dir = cbs_cache_dir,
      use_cache = cbs_use_cache
    )$season
  }

  proj_all <- bind_rows(proj_other_all, cbs_season)

  if (nrow(proj_all) == 0) stop("No projection rows returned.")

  # Normalize position + IDs
  if (!"pos" %in% names(proj_all)) proj_all$pos <- proj_all$pos_from_list
  if (!"team" %in% names(proj_all)) proj_all$team <- NA_character_
  if (!"id" %in% names(proj_all)) proj_all$id <- NA_character_
  if (!"src_id" %in% names(proj_all)) proj_all$src_id <- NA_character_
  if (!"data_src" %in% names(proj_all)) stop("Projection data missing data_src")

  proj_all <- proj_all %>%
    mutate(
      data_src = as.character(data_src),
      position = as.character(pos),
      mfl_id   = as.character(id),
      src_id   = as.character(src_id),
      player   = as.character(player),
      team     = as.character(team)
    ) %>%
    ensure_numeric_cols(PROJ_STAT_COLS) %>%
    parse_numeric_cols(PROJ_STAT_COLS)

  # -------------------------
  # Crosswalk: use BOTH mfl_id and site ids (including cbs_id) to get gsis_id
  # load_ff_playerids includes cbs_id/mfl_id/gsis_id :contentReference[oaicite:4]{index=4}
  # -------------------------
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

  roster_key <- roster %>%
    transmute(
      gsis_id = as.character(gsis_id),
      roster_full_name = full_name,
      roster_position  = position,
      roster_team      = team
    )

  proj_tagged <- proj_all %>%
    # First map by mfl_id, then backfill using (data_src, src_id)
    left_join(ff_ids %>% select(mfl_id, gsis_id), by = "mfl_id") %>%
    left_join(ids_long %>% rename(mfl_id_2 = mfl_id, gsis_id_2 = gsis_id),
              by = c("data_src", "src_id")) %>%
    mutate(
      mfl_id  = coalesce(mfl_id, mfl_id_2),
      gsis_id = coalesce(gsis_id, gsis_id_2)
    ) %>%
    select(-mfl_id_2, -gsis_id_2) %>%
    left_join(roster_key, by = "gsis_id") %>%
    mutate(
      full_name = coalesce(roster_full_name, player),
      position  = coalesce(roster_position, position),
      team      = coalesce(roster_team, team),
      name_clean = clean_player_name(full_name),
      # Stable dedupe key:
      player_key = case_when(
        !is.na(gsis_id) & gsis_id != "" ~ paste0("gsis|", gsis_id),
        !is.na(mfl_id)  & mfl_id  != "" ~ paste0("mfl|", mfl_id),
        !is.na(src_id)  & src_id  != "" ~ paste0("src|", data_src, "|", src_id),
        TRUE ~ paste0("name|", name_clean, "|", position, "|", team)
      )
    )

  # -------------------------
  # Dedupe within each source (prevents duplicate rows from inflating accuracy)
  # -------------------------
  proj_one_per_source <- proj_tagged %>%
    group_by(data_src, player_key, position) %>%
    summarize(
      gsis_id   = first_non_na_chr(gsis_id),
      mfl_id    = first_non_na_chr(mfl_id),
      src_id    = first_non_na_chr(src_id),
      full_name = first_non_na_chr(full_name),
      team      = first_non_na_chr(team),
      across(all_of(PROJ_STAT_COLS), ~ mean(.x, na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    mutate(across(all_of(PROJ_STAT_COLS), ~ ifelse(is.nan(.x), NA_real_, .x)))

  # -------------------------
  # CBS diagnostics (VERY helpful for sanity checking)
  # -------------------------
  if (include_cbs) {
    cbs_diag <- proj_one_per_source %>%
      filter(data_src == "CBS") %>%
      summarize(
        season = season_year,
        n_rows = n(),
        pct_missing_gsis = mean(is.na(gsis_id) | gsis_id == ""),
        pct_missing_mfl  = mean(is.na(mfl_id)  | mfl_id  == ""),
        pct_missing_src  = mean(is.na(src_id)  | src_id  == ""),
        qb_median_pass_yds = median(pass_yds[position == "QB"], na.rm = TRUE),
        rb_median_rush_yds = median(rush_yds[position == "RB"], na.rm = TRUE),
        wr_median_rec_yds  = median(rec_yds[position == "WR"],  na.rm = TRUE)
      )

    print(cbs_diag)

    if (write_files) {
      write_csv(cbs_diag, paste0("cbs_diagnostics_", season_year, ".csv"))
      # Export compiled CBS season projections (open this to inspect numbers)
      write_csv(
        proj_one_per_source %>% filter(data_src == "CBS"),
        paste0("cbs_season_compiled_", season_year, ".csv")
      )
    }
  }

  # -------------------------
  # Join projections to actuals by gsis_id
  # -------------------------
  all_matches <- proj_one_per_source %>%
    filter(!is.na(gsis_id) & gsis_id != "") %>%
    inner_join(season_stats, by = c("gsis_id" = "player_id")) %>%
    left_join(games_played, by = "gsis_id") %>%
    mutate(games_played = replace_na(games_played, 0L))

  if (require_full_season) {
    all_matches <- all_matches %>% filter(games_played >= season_games)
  }

  # Differences
  stat_comparison <- all_matches %>%
    mutate(
      pass_yds_diff  = pass_yds - passing_yards,
      pass_tds_diff  = pass_tds - passing_tds,
      rush_yds_diff  = rush_yds - rushing_yards,
      rush_tds_diff  = rush_tds - rushing_tds,
      rec_yds_diff   = rec_yds  - receiving_yards,
      rec_tds_diff   = rec_tds  - receiving_tds,
      receptions_diff = rec - receptions
    )

  # Accuracy by source
  source_accuracy <- all_matches %>%
    mutate(
      rec_yds_acc  = ifelse(receiving_yards > 0 & !is.na(rec_yds),  1 - abs(rec_yds - receiving_yards) / receiving_yards, NA),
      rec_tds_acc  = ifelse(receiving_tds   > 0 & !is.na(rec_tds),  1 - abs(rec_tds - receiving_tds)   / receiving_tds,   NA),
      recs_acc     = ifelse(receptions      > 0 & !is.na(rec),      1 - abs(rec - receptions)          / receptions,      NA),
      rush_yds_acc = ifelse(rushing_yards   > 0 & !is.na(rush_yds), 1 - abs(rush_yds - rushing_yards) / rushing_yards,   NA),
      rush_tds_acc = ifelse(rushing_tds     > 0 & !is.na(rush_tds), 1 - abs(rush_tds - rushing_tds)   / rushing_tds,     NA),
      pass_yds_acc = ifelse(passing_yards   > 0 & !is.na(pass_yds), 1 - abs(pass_yds - passing_yards) / passing_yards,   NA),
      pass_tds_acc = ifelse(passing_tds     > 0 & !is.na(pass_tds), 1 - abs(pass_tds - passing_tds)   / passing_tds,     NA)
    ) %>%
    group_by(data_src) %>%
    summarize(
      n_players   = n_distinct(gsis_id),
      avg_rec_yds = mean(rec_yds_acc, na.rm = TRUE),
      avg_rec_tds = mean(rec_tds_acc, na.rm = TRUE),
      avg_recs    = mean(recs_acc, na.rm = TRUE),
      avg_rush_yds = mean(rush_yds_acc, na.rm = TRUE),
      avg_rush_tds = mean(rush_tds_acc, na.rm = TRUE),
      avg_pass_yds = mean(pass_yds_acc, na.rm = TRUE),
      avg_pass_tds = mean(pass_tds_acc, na.rm = TRUE),
      overall_accuracy = mean(c(
        mean(rec_yds_acc, na.rm = TRUE),
        mean(rec_tds_acc, na.rm = TRUE),
        mean(recs_acc, na.rm = TRUE),
        mean(rush_yds_acc, na.rm = TRUE),
        mean(rush_tds_acc, na.rm = TRUE),
        mean(pass_yds_acc, na.rm = TRUE),
        mean(pass_tds_acc, na.rm = TRUE)
      ), na.rm = TRUE) * 100,
      .groups = "drop"
    ) %>%
    mutate(
      across(starts_with("avg_"), ~ round(. * 100, 1)),
      overall_accuracy = round(overall_accuracy, 1)
    ) %>%
    arrange(desc(overall_accuracy))

  if (write_files) {
    write_csv(stat_comparison, paste0("stat_comparison_", season_year, ".csv"))
    write_csv(source_accuracy, paste0("source_accuracy_", season_year, ".csv"))
  }

  message(glue("✅ Complete: saved comparison, accuracy, availability, and CBS diagnostics for {season_year}"))

  list(
    source_accuracy = source_accuracy,
    stat_comparison = stat_comparison,
    availability_summary = availability_summary,
    availability_by_position = availability_by_position
  )
}

# Example:
# res <- run_projection_accuracy(2024)
# res$source_accuracy
# res$availability_summary
