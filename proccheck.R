# proccheck_noCBS.R
# ------------------------------------------------------------------------------
# Projection accuracy + injury/availability diagnostics (CBS eliminated)
#
# Key features retained:
# - Crosswalk projections to gsis_id using nflreadr::load_ff_playerids()
#   (uses MFL id when present, otherwise uses per-site src_id mapping)
# - Deduplicates within each source to prevent double-counting
# - Availability metrics computed across ALL rostered players:
#     * "Game played" = total snaps >= 10 (preferred if snap counts available)
#       else fallback = >=5 pass attempts OR >=5 rush attempts OR >=5 targets
#       and if targets cannot be computed then receptions >= 1
# - Projection accuracy can be filtered to players who played ALL possible games
# - Writes:
#     stat_comparison_<season>.csv
#     source_accuracy_<season>.csv
#     availability_summary_<season>.csv
#     availability_by_position_<season>.csv
# ------------------------------------------------------------------------------

run_projection_accuracy <- function(
  season_year,
  week = 0,
  src = c("FantasyPros", "NFL", "ESPN"),
  pos = c("QB", "RB", "WR", "TE"),

  # season length defaults: 17 games for 2021+, else 16
  full_season_games = ifelse(season_year >= 2021, 17L, 16L),
  min_game_share = 0.75,
  require_full_season = TRUE,

  # --- game played definition ---
  snaps_threshold = 10,
  pass_att_threshold = 5,
  rush_att_threshold = 5,
  targets_threshold  = 5,
  rec_threshold_if_no_targets = 1,

  out_prefix = ""
) {
  suppressPackageStartupMessages({
    library(dplyr)
    library(tidyr)
    library(purrr)
    library(stringr)
    library(readr)
    library(nflfastR)
    library(nflreadr)
    library(ffanalytics)
  })

  src <- unique(setdiff(src, "CBS"))  # hard drop CBS if user passes it
  pos <- unique(pos)

  message(sprintf("Running projection accuracy analysis for %s (CBS excluded)...", season_year))

  # ----------------------------
  # Load play-by-play
  # ----------------------------
  pbp <- nflfastR::load_pbp(season_year) %>%
    dplyr::filter(season_type == "REG")

  # ----------------------------
  # Load rosters
  # ----------------------------
  roster_all <- nflreadr::load_rosters(season_year) %>%
    dplyr::transmute(
      player_id = as.character(gsis_id),
      pfr_id    = as.character(pfr_id),
      full_name = full_name,
      position  = position,
      team      = team,
      rookie_year = rookie_year
    )

  roster_skill <- roster_all %>% dplyr::filter(position %in% pos)

  # ----------------------------
  # Helper: safe scrape wrapper
  # ----------------------------
  safe_scrape <- function(season, week, src, pos) {
    tryCatch(
      ffanalytics::scrape_data(season = season, week = week, src = src, pos = pos),
      error = function(e) {
        message(sprintf("⚠️ scrape_data failed for src=%s week=%s: %s", paste(src, collapse=","), week, e$message))
        NULL
      }
    )
  }

  # ----------------------------
  # Games played (availability / injury proxy)
  # ----------------------------
  get_games_played <- function() {
    # --- Fallback from PBP opportunities ---
    pass_opps <- pbp %>%
      dplyr::filter(!is.na(passer_player_id)) %>%
      dplyr::group_by(player_id = as.character(passer_player_id), game_id) %>%
      dplyr::summarize(pass_att = sum(pass_attempt, na.rm = TRUE), .groups = "drop")

    rush_opps <- pbp %>%
      dplyr::filter(!is.na(rusher_player_id)) %>%
      dplyr::group_by(player_id = as.character(rusher_player_id), game_id) %>%
      dplyr::summarize(rush_att = sum(rush_attempt, na.rm = TRUE), .groups = "drop")

    targ_opps <- pbp %>%
      dplyr::filter(!is.na(receiver_player_id), pass_attempt == 1) %>%
      dplyr::group_by(player_id = as.character(receiver_player_id), game_id) %>%
      dplyr::summarize(targets = dplyr::n(), .groups = "drop")

    rec_opps <- pbp %>%
      dplyr::filter(!is.na(receiver_player_id), complete_pass == 1) %>%
      dplyr::group_by(player_id = as.character(receiver_player_id), game_id) %>%
      dplyr::summarize(receptions = dplyr::n(), .groups = "drop")

    pbp_games <- list(pass_opps, rush_opps, targ_opps, rec_opps) %>%
      purrr::reduce(dplyr::full_join, by = c("player_id", "game_id")) %>%
      tidyr::replace_na(list(pass_att = 0L, rush_att = 0L, targets = 0L, receptions = 0L)) %>%
      dplyr::mutate(
        game_played_pbp =
          (pass_att >= pass_att_threshold) |
          (rush_att >= rush_att_threshold) |
          (targets  >= targets_threshold)  |
          ((is.na(targets) | targets == 0) & receptions >= rec_threshold_if_no_targets)
      ) %>%
      dplyr::filter(game_played_pbp) %>%
      dplyr::distinct(player_id, game_id) %>%
      dplyr::count(player_id, name = "games_played_pbp")

    # --- Try snap counts ---
    snap_counts <- tryCatch(
      nflreadr::load_snap_counts(season_year),
      error = function(e) tryCatch(nflreadr::load_snap_counts(seasons = season_year), error = function(e2) NULL)
    )

    if (is.null(snap_counts)) {
      message("ℹ️ Snap counts not available (or failed to load). Using PBP opportunity-based games_played.")
      return(pbp_games %>%
               dplyr::mutate(games_played = games_played_pbp, method = "pbp_opps") %>%
               dplyr::select(player_id, games_played, method))
    }

    # Filter to season/reg if columns exist
    if ("season" %in% names(snap_counts)) snap_counts <- snap_counts %>% dplyr::filter(season == season_year)
    if ("game_type" %in% names(snap_counts)) snap_counts <- snap_counts %>% dplyr::filter(game_type == "REG")
    if ("season_type" %in% names(snap_counts)) snap_counts <- snap_counts %>% dplyr::filter(season_type == "REG")

    # Determine total snaps column
    if (all(c("offense_snaps", "defense_snaps", "st_snaps") %in% names(snap_counts))) {
      snap_counts <- snap_counts %>%
        dplyr::mutate(total_snaps = tidyr::replace_na(offense_snaps, 0) +
                                 tidyr::replace_na(defense_snaps, 0) +
                                 tidyr::replace_na(st_snaps, 0))
    } else if ("total_snaps" %in% names(snap_counts)) {
      snap_counts <- snap_counts %>% dplyr::mutate(total_snaps = suppressWarnings(as.numeric(total_snaps)))
    } else if ("snaps" %in% names(snap_counts)) {
      snap_counts <- snap_counts %>% dplyr::mutate(total_snaps = suppressWarnings(as.numeric(snaps)))
    } else {
      message("ℹ️ Snap counts table missing snaps columns. Using PBP opportunity-based games_played.")
      return(pbp_games %>%
               dplyr::mutate(games_played = games_played_pbp, method = "pbp_opps") %>%
               dplyr::select(player_id, games_played, method))
    }

    # Map snap ids -> gsis_id
    if ("gsis_id" %in% names(snap_counts)) {
      snaps_mapped <- snap_counts %>% dplyr::mutate(player_id = as.character(gsis_id))
    } else if ("player_id" %in% names(snap_counts)) {
      snaps_mapped <- snap_counts %>% dplyr::mutate(player_id = as.character(player_id))
    } else if ("pfr_player_id" %in% names(snap_counts)) {
      snaps_mapped <- snap_counts %>%
        dplyr::mutate(pfr_id = as.character(pfr_player_id)) %>%
        dplyr::left_join(roster_all %>% dplyr::select(player_id, pfr_id), by = "pfr_id")
    } else if ("pfr_id" %in% names(snap_counts)) {
      snaps_mapped <- snap_counts %>%
        dplyr::mutate(pfr_id = as.character(pfr_id)) %>%
        dplyr::left_join(roster_all %>% dplyr::select(player_id, pfr_id), by = "pfr_id")
    } else {
      message("ℹ️ Snap counts table missing an identifiable player id column. Using PBP opportunity-based games_played.")
      return(pbp_games %>%
               dplyr::mutate(games_played = games_played_pbp, method = "pbp_opps") %>%
               dplyr::select(player_id, games_played, method))
    }

    # Count games with total snaps >= threshold
    if ("game_id" %in% names(snaps_mapped)) {
      snaps_games <- snaps_mapped %>%
        dplyr::filter(!is.na(player_id) & player_id != "") %>%
        dplyr::filter(!is.na(total_snaps) & total_snaps >= snaps_threshold) %>%
        dplyr::distinct(player_id, game_id) %>%
        dplyr::count(player_id, name = "games_played_snaps")
    } else if ("week" %in% names(snaps_mapped)) {
      snaps_games <- snaps_mapped %>%
        dplyr::mutate(week = suppressWarnings(as.integer(week))) %>%
        dplyr::filter(!is.na(player_id) & player_id != "") %>%
        dplyr::filter(!is.na(total_snaps) & total_snaps >= snaps_threshold) %>%
        dplyr::distinct(player_id, week) %>%
        dplyr::count(player_id, name = "games_played_snaps")
    } else {
      snaps_games <- snaps_mapped %>%
        dplyr::filter(!is.na(player_id) & player_id != "") %>%
        dplyr::filter(!is.na(total_snaps) & total_snaps >= snaps_threshold) %>%
        dplyr::count(player_id, name = "games_played_snaps")
    }

    combined <- dplyr::full_join(snaps_games, pbp_games, by = "player_id") %>%
      dplyr::mutate(
        games_played = dplyr::coalesce(games_played_snaps, games_played_pbp, 0L),
        method = dplyr::case_when(
          !is.na(games_played_snaps) ~ "snaps",
          !is.na(games_played_pbp)   ~ "pbp_opps",
          TRUE                       ~ "none"
        )
      ) %>%
      dplyr::select(player_id, games_played, method)

    message(sprintf(
      "ℹ️ Games played method: %s players via snaps, %s via pbp fallback",
      sum(combined$method == "snaps", na.rm = TRUE),
      sum(combined$method == "pbp_opps", na.rm = TRUE)
    ))

    combined
  }

  player_games <- get_games_played()

  # ----------------------------
  # Summarize season stats from PBP
  # ----------------------------
  pass_stats <- pbp %>%
    dplyr::filter(!is.na(passer_player_id)) %>%
    dplyr::group_by(player_id = as.character(passer_player_id)) %>%
    dplyr::summarise(
      passing_yards = sum(passing_yards, na.rm = TRUE),
      passing_tds   = sum(pass_touchdown, na.rm = TRUE),
      interceptions = sum(interception, na.rm = TRUE),
      pass_attempts = sum(pass_attempt, na.rm = TRUE),
      completions   = sum(complete_pass, na.rm = TRUE),
      .groups = "drop"
    )

  rush_stats <- pbp %>%
    dplyr::filter(!is.na(rusher_player_id)) %>%
    dplyr::group_by(player_id = as.character(rusher_player_id)) %>%
    dplyr::summarise(
      rushing_yards     = sum(rushing_yards, na.rm = TRUE),
      rushing_tds       = sum(rush_touchdown, na.rm = TRUE),
      rushing_attempts  = sum(rush_attempt, na.rm = TRUE),
      fumbles_lost      = sum(fumble_lost, na.rm = TRUE),
      .groups = "drop"
    )

  recv_stats <- pbp %>%
    dplyr::filter(!is.na(receiver_player_id), pass_attempt == 1) %>%
    dplyr::group_by(player_id = as.character(receiver_player_id)) %>%
    dplyr::summarise(
      targets          = dplyr::n(),
      receptions       = sum(complete_pass == 1, na.rm = TRUE),
      receiving_yards  = sum(receiving_yards, na.rm = TRUE),
      receiving_tds    = sum(touchdown == 1 & complete_pass == 1, na.rm = TRUE),
      .groups = "drop"
    )

  season_stats <- list(pass_stats, rush_stats, recv_stats) %>%
    purrr::reduce(dplyr::full_join, by = "player_id") %>%
    tidyr::replace_na(list(
      passing_yards = 0, passing_tds = 0, interceptions = 0, pass_attempts = 0, completions = 0,
      rushing_yards = 0, rushing_tds = 0, rushing_attempts = 0, fumbles_lost = 0,
      targets = 0, receptions = 0, receiving_yards = 0, receiving_tds = 0
    )) %>%
    dplyr::left_join(roster_skill, by = "player_id") %>%
    dplyr::left_join(player_games, by = "player_id") %>%
    dplyr::mutate(games_played = tidyr::replace_na(games_played, 0L)) %>%
    dplyr::filter(position %in% pos)

  # ----------------------------
  # Availability summary across ALL rostered players
  # ----------------------------
  min_games_75 <- ceiling(full_season_games * min_game_share)

  roster_avail <- roster_all %>%
    dplyr::distinct(player_id, position, team, full_name) %>%
    dplyr::left_join(player_games, by = "player_id") %>%
    dplyr::mutate(games_played = tidyr::replace_na(games_played, 0L))

  availability_summary <- roster_avail %>%
    dplyr::summarize(
      season = season_year,
      possible_games = full_season_games,
      n_rostered_players = dplyr::n(),
      n_played_all = sum(games_played >= full_season_games),
      pct_played_all = round(100 * n_played_all / n_rostered_players, 1),
      n_under_75pct = sum(games_played < min_games_75),
      pct_under_75pct = round(100 * n_under_75pct / n_rostered_players, 1)
    )

  availability_by_pos <- roster_avail %>%
    dplyr::group_by(position) %>%
    dplyr::summarize(
      season = season_year,
      possible_games = full_season_games,
      n_rostered_players = dplyr::n(),
      n_played_all = sum(games_played >= full_season_games),
      pct_played_all = round(100 * n_played_all / n_rostered_players, 1),
      n_under_75pct = sum(games_played < min_games_75),
      pct_under_75pct = round(100 * n_under_75pct / n_rostered_players, 1),
      .groups = "drop"
    ) %>%
    dplyr::arrange(desc(n_rostered_players))

  readr::write_csv(availability_summary, paste0(out_prefix, "availability_summary_", season_year, ".csv"))
  readr::write_csv(availability_by_pos, paste0(out_prefix, "availability_by_position_", season_year, ".csv"))

  # ----------------------------
  # Scrape projections (CBS excluded)
  # ----------------------------
  proj_all <- tibble::tibble()
  if (length(src) > 0) {
    proj_raw <- safe_scrape(season_year, week, src, pos)
    if (!is.null(proj_raw)) {
      proj_all <- dplyr::bind_rows(proj_raw, .id = "proj_position")
    }
  }

  if (nrow(proj_all) == 0) {
    stop("No projections scraped. Check your src/pos inputs and the data sources.")
  }

  # Normalize position column
  if ("pos" %in% names(proj_all)) {
    proj_all <- proj_all %>% dplyr::mutate(position_src = as.character(pos))
  } else if ("proj_position" %in% names(proj_all)) {
    proj_all <- proj_all %>% dplyr::mutate(position_src = as.character(proj_position))
  } else {
    proj_all <- proj_all %>% dplyr::mutate(position_src = NA_character_)
  }

  if (!"id" %in% names(proj_all)) proj_all$id <- NA
  if (!"src_id" %in% names(proj_all)) proj_all$src_id <- NA

  # ----------------------------
  # Crosswalk: map to gsis_id using MFL id first, then per-site src_id
  # ----------------------------
  ff_ids <- nflreadr::load_ff_playerids() %>%
    dplyr::transmute(
      gsis_id = as.character(gsis_id),
      mfl_id  = as.character(mfl_id),
      fantasypros_id = as.character(fantasypros_id),
      nfl_id  = as.character(nfl_id),
      espn_id = as.character(espn_id)
    )

  ids_long <- ff_ids %>%
    tidyr::pivot_longer(
      cols = c(fantasypros_id, nfl_id, espn_id),
      names_to = "id_type",
      values_to = "src_id"
    ) %>%
    dplyr::mutate(
      data_src = dplyr::recode(
        id_type,
        fantasypros_id = "FantasyPros",
        nfl_id = "NFL",
        espn_id = "ESPN"
      ),
      src_id = as.character(src_id)
    ) %>%
    dplyr::filter(!is.na(src_id) & src_id != "") %>%
    dplyr::select(data_src, src_id, gsis_id, mfl_id)

  proj_mapped <- proj_all %>%
    dplyr::mutate(
      data_src = as.character(data_src),
      mfl_id   = as.character(id),
      src_id   = as.character(src_id)
    ) %>%
    dplyr::left_join(ff_ids %>% dplyr::select(mfl_id, gsis_id), by = "mfl_id") %>%
    dplyr::left_join(ids_long %>% dplyr::rename(gsis_id2 = gsis_id, mfl_id2 = mfl_id),
                     by = c("data_src", "src_id")) %>%
    dplyr::mutate(
      gsis_id = dplyr::coalesce(gsis_id, gsis_id2),
      mfl_id  = dplyr::coalesce(mfl_id, mfl_id2),
      player_id = gsis_id
    ) %>%
    dplyr::select(-gsis_id2, -mfl_id2) %>%
    dplyr::left_join(
      roster_skill %>% dplyr::rename(full_name_roster = full_name, team_roster = team, position_roster = position),
      by = c("player_id" = "player_id")
    ) %>%
    dplyr::mutate(
      full_name = dplyr::coalesce(full_name_roster, player),
      team      = dplyr::coalesce(team_roster, team),
      position  = dplyr::coalesce(position_roster, position_src)
    ) %>%
    dplyr::filter(!is.na(player_id) & player_id != "") %>%
    dplyr::filter(data_src %in% src, position %in% pos)

  # Projection stat cols that exist in scrape output
  proj_stat_cols <- intersect(
    c(
      "pass_att", "pass_comp", "pass_yds", "pass_tds", "pass_int",
      "rush_att", "rush_yds", "rush_tds",
      "rec_tgt", "rec_yds", "rec_tds", "rec",
      "fumbles_lost"
    ),
    names(proj_mapped)
  )

  # Deduplicate within each source
  proj_dedup <- proj_mapped %>%
    dplyr::group_by(data_src, player_id, position) %>%
    dplyr::summarize(
      full_name = dplyr::first(full_name),
      team      = dplyr::first(team),
      dplyr::across(dplyr::all_of(proj_stat_cols), ~ mean(suppressWarnings(as.numeric(.x)), na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    dplyr::mutate(dplyr::across(dplyr::all_of(proj_stat_cols), ~ ifelse(is.nan(.x), NA_real_, .x)))

  # ----------------------------
  # Join projections to actual stats, then apply injury filter
  # ----------------------------
  all_matches <- proj_dedup %>%
    dplyr::inner_join(season_stats, by = c("player_id", "position"), suffix = c("_proj", ""))

  if (require_full_season) {
    all_matches <- all_matches %>% dplyr::filter(games_played >= full_season_games)
  }

  # ----------------------------
  # Stat comparison export
  # ----------------------------
  stat_comparison <- all_matches
  if (all(c("pass_yds", "passing_yards") %in% names(stat_comparison))) {
    stat_comparison <- stat_comparison %>% dplyr::mutate(pass_yds_diff = pass_yds - passing_yards)
  }
  if (all(c("pass_tds", "passing_tds") %in% names(stat_comparison))) {
    stat_comparison <- stat_comparison %>% dplyr::mutate(pass_tds_diff = pass_tds - passing_tds)
  }
  if (all(c("rush_yds", "rushing_yards") %in% names(stat_comparison))) {
    stat_comparison <- stat_comparison %>% dplyr::mutate(rush_yds_diff = rush_yds - rushing_yards)
  }
  if (all(c("rush_tds", "rushing_tds") %in% names(stat_comparison))) {
    stat_comparison <- stat_comparison %>% dplyr::mutate(rush_tds_diff = rush_tds - rushing_tds)
  }
  if (all(c("rec_yds", "receiving_yards") %in% names(stat_comparison))) {
    stat_comparison <- stat_comparison %>% dplyr::mutate(rec_yds_diff = rec_yds - receiving_yards)
  }
  if (all(c("rec_tds", "receiving_tds") %in% names(stat_comparison))) {
    stat_comparison <- stat_comparison %>% dplyr::mutate(rec_tds_diff = rec_tds - receiving_tds)
  }
  if (all(c("rec", "receptions") %in% names(stat_comparison))) {
    stat_comparison <- stat_comparison %>% dplyr::mutate(receptions_diff = rec - receptions)
  }

  readr::write_csv(stat_comparison, paste0(out_prefix, "stat_comparison_", season_year, ".csv"))

  # ----------------------------
  # Accuracy by source
  # ----------------------------
  # Yardage accuracy uses yard-weighted accuracy (wMAPE-based):
  #   wMAPE = sum(|proj-actual|) / sum(actual)
  #   wACC  = 1 - wMAPE
  # This avoids tiny-denominator players dominating the mean.
  # TDs and receptions keep the original mean-of-(1-APE) approach.

  # Defensive: ensure required columns exist
  if (!"pass_yds" %in% names(all_matches)) all_matches$pass_yds <- NA_real_
  if (!"rush_yds" %in% names(all_matches)) all_matches$rush_yds <- NA_real_
  if (!"rec_yds"  %in% names(all_matches)) all_matches$rec_yds  <- NA_real_
  if (!"pass_tds" %in% names(all_matches)) all_matches$pass_tds <- NA_real_
  if (!"rush_tds" %in% names(all_matches)) all_matches$rush_tds <- NA_real_
  if (!"rec_tds"  %in% names(all_matches)) all_matches$rec_tds  <- NA_real_
  if (!"rec"      %in% names(all_matches)) all_matches$rec      <- NA_real_

  source_accuracy <- all_matches %>%
    dplyr::filter(!is.na(data_src)) %>%
    dplyr::mutate(
      # count-based accuracies (same as before)
      rec_tds_acc  = ifelse(receiving_tds   > 0, 1 - abs(rec_tds  - receiving_tds) / receiving_tds,   NA_real_),
      recs_acc     = ifelse(receptions      > 0, 1 - abs(rec      - receptions)      / receptions,      NA_real_),
      rush_tds_acc = ifelse(rushing_tds     > 0, 1 - abs(rush_tds - rushing_tds)     / rushing_tds,     NA_real_),
      pass_tds_acc = ifelse(passing_tds     > 0, 1 - abs(pass_tds - passing_tds)     / passing_tds,     NA_real_)
    ) %>%
    dplyr::group_by(data_src) %>%
    dplyr::summarize(
      n_players = dplyr::n_distinct(player_id),

      # --- yard-weighted accuracies (wMAPE / wACC) ---
      n_pass_yds = sum(passing_yards > 0 & !is.na(pass_yds)),
      wmape_pass_yds = {
        mask <- passing_yards > 0 & !is.na(pass_yds)
        den <- sum(passing_yards[mask], na.rm = TRUE)
        ifelse(den > 0,
               sum(abs(pass_yds[mask] - passing_yards[mask]), na.rm = TRUE) / den,
               NA_real_)
      },
      avg_pass_yds = ifelse(is.na(wmape_pass_yds), NA_real_, 1 - wmape_pass_yds),

      n_rush_yds = sum(rushing_yards > 0 & !is.na(rush_yds)),
      wmape_rush_yds = {
        mask <- rushing_yards > 0 & !is.na(rush_yds)
        den <- sum(rushing_yards[mask], na.rm = TRUE)
        ifelse(den > 0,
               sum(abs(rush_yds[mask] - rushing_yards[mask]), na.rm = TRUE) / den,
               NA_real_)
      },
      avg_rush_yds = ifelse(is.na(wmape_rush_yds), NA_real_, 1 - wmape_rush_yds),

      n_rec_yds = sum(receiving_yards > 0 & !is.na(rec_yds)),
      wmape_rec_yds = {
        mask <- receiving_yards > 0 & !is.na(rec_yds)
        den <- sum(receiving_yards[mask], na.rm = TRUE)
        ifelse(den > 0,
               sum(abs(rec_yds[mask] - receiving_yards[mask]), na.rm = TRUE) / den,
               NA_real_)
      },
      avg_rec_yds = ifelse(is.na(wmape_rec_yds), NA_real_, 1 - wmape_rec_yds),

      # --- count-based accuracies (original method) ---
      avg_rec_tds  = mean(rec_tds_acc,  na.rm = TRUE),
      avg_recs     = mean(recs_acc,     na.rm = TRUE),
      avg_rush_tds = mean(rush_tds_acc, na.rm = TRUE),
      avg_pass_tds = mean(pass_tds_acc, na.rm = TRUE),

      .groups = "drop"
    ) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      overall_accuracy = mean(dplyr::c_across(dplyr::starts_with("avg_")), na.rm = TRUE) * 100
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      # Convert accuracies and wmape to % and round (also removes -0)
      dplyr::across(dplyr::starts_with("avg_"), ~ {
        z <- round(. * 100, 1)
        ifelse(z == 0, 0, z)
      }),
      dplyr::across(dplyr::starts_with("wmape_"), ~ {
        z <- round(. * 100, 1)
        ifelse(z == 0, 0, z)
      }),
      overall_accuracy = {
        z <- round(overall_accuracy, 1)
        ifelse(z == 0, 0, z)
      }
    ) %>%
    dplyr::arrange(desc(overall_accuracy))

readr::write_csv(source_accuracy, paste0(out_prefix, "source_accuracy_", season_year, ".csv"))

  message(sprintf("✅ Complete for %s (CBS excluded).", season_year))

  list(
    source_accuracy = source_accuracy,
    availability_summary = availability_summary,
    availability_by_position = availability_by_pos
  )
}

# Example:
# source("proccheck_noCBS.R") 
# res <- run_projection_accuracy(2024)
# res$source_accuracy
