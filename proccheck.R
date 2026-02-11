run_projection_accuracy <- function(season_year) {
  library(tidyverse)
  library(nflfastR)
  library(nflreadr)
  library(ffanalytics)
  library(fuzzyjoin)
  
  message(glue::glue("Running projection accuracy analysis for {season_year}..."))
  
  # Load data
  pbp <- load_pbp(season_year) %>% filter(season_type == "REG")
  roster <- load_rosters(season_year) %>%
    select(player_id = gsis_id, full_name, position, team, rookie_year, espn_id, yahoo_id, college)
  
  # Summarize player stats
  pass_stats <- pbp %>%
    filter(!is.na(passer_player_id)) %>%
    group_by(player_id = passer_player_id) %>%
    summarise(
      passing_yards = sum(passing_yards, na.rm = TRUE),
      passing_tds = sum(pass_touchdown, na.rm = TRUE),
      interceptions = sum(interception, na.rm = TRUE),
      pass_attempts = sum(pass_attempt, na.rm = TRUE),
      completions = sum(complete_pass, na.rm = TRUE),
      avg_epa_pass = mean(epa, na.rm = TRUE),
      .groups = "drop"
    )
  
  rush_stats <- pbp %>%
    filter(!is.na(rusher_player_id)) %>%
    group_by(player_id = rusher_player_id) %>%
    summarise(
      rushing_yards = sum(rushing_yards, na.rm = TRUE),
      rushing_tds = sum(rush_touchdown, na.rm = TRUE),
      rushing_attempts = sum(rush_attempt, na.rm = TRUE),
      fumbles = sum(fumble, na.rm = TRUE),
      fumbles_lost = sum(fumble_lost, na.rm = TRUE),
      avg_epa_rush = mean(epa, na.rm = TRUE),
      .groups = "drop"
    )
  
  recv_stats <- pbp %>%
    filter(!is.na(receiver_player_id), pass == 1, complete_pass == 1) %>%
    group_by(player_id = receiver_player_id) %>%
    summarise(
      receiving_yards = sum(receiving_yards, na.rm = TRUE),
      receiving_tds = sum(touchdown, na.rm = TRUE),
      receptions = n(),
      yak = sum(yards_after_catch, na.rm = TRUE),
      avg_epa_recv = mean(epa, na.rm = TRUE),
      .groups = "drop"
    )
  
  # Combine stats and join with roster
  season_stats <- list(pass_stats, rush_stats, recv_stats) %>%
    reduce(full_join, by = "player_id") %>%
    replace_na(list(
      passing_yards = 0, passing_tds = 0, interceptions = 0, pass_attempts = 0, completions = 0, avg_epa_pass = NA,
      rushing_yards = 0, rushing_tds = 0, rushing_attempts = 0, fumbles = 0, fumbles_lost = 0, avg_epa_rush = NA,
      receiving_yards = 0, receiving_tds = 0, receptions = 0, yak = NA, avg_epa_recv = NA
    )) %>%
    mutate(total_yards = passing_yards + rushing_yards + receiving_yards) %>%
    left_join(roster, by = "player_id") %>%
    relocate(full_name, team, position, .after = player_id)
  
  # Scrape and clean projections
  non_yahoo_proj <- scrape_data(
    season = season_year,
    week = 0,
    src = c("FantasyPros", "CBS", "NFL", "ESPN"),
    pos = c("QB", "RB", "WR", "TE")
  )

  yahoo_weekly_all <- map_dfr(
    1:18,
    ~ scrape_data(
      season = season_year,
      week = .x,
      src = "Yahoo",
      pos = c("QB", "RB", "WR", "TE")
    ) %>%
      bind_rows(.id = "proj_position") %>%
      mutate(week = .x)
  )

  yahoo_yearly <- yahoo_weekly_all %>%
    mutate(across(where(is.character), ~ na_if(.x, ""))) %>%
    group_by(player, pos, data_src, proj_position) %>%
    summarize(
      across(where(is.numeric) & !any_of("week"), ~ sum(.x, na.rm = TRUE)),
      team = dplyr::first(team[!is.na(team)], default = NA_character_),
      .groups = "drop"
    )

  proj_all <- bind_rows(
    bind_rows(non_yahoo_proj, .id = "proj_position"),
    yahoo_yearly
  )
  
  proj_clean <- proj_all %>%
    # Use internal position column (e.g., 'pos') or proj_position
    rename(position = pos) %>%  # Only do this if it's named 'pos'
    mutate(
      full_name = str_to_lower(str_trim(player)),
      full_name = str_replace_all(full_name, "[^a-z ]", "")
    )
  
  season_stats_final_clean <- season_stats %>%
    mutate(
      full_name = str_to_lower(str_trim(full_name)),
      full_name = str_replace_all(full_name, "[^a-z ]", "")
    )
  
  # Exact join
  exact_matches <- inner_join(proj_clean, season_stats_final_clean, by = c("full_name", "position"))
  
  # Fuzzy match (name only), then filter by matching position
  fuzzy_match <- stringdist_inner_join(
    proj_clean,
    season_stats_final_clean,
    by = "full_name",
    method = "jw",
    max_dist = 0.1,
    distance_col = "dist"
  ) %>%
    filter(position.x == position.y) %>%
    group_by(full_name.x, position.x) %>%
    slice_min(dist, n = 1) %>%
    ungroup() %>%
    rename(
      full_name = full_name.x,
      position = position.x,
      full_name_match = full_name.y,
      position_match = position.y
    )
  
  fuzzy_clean <- fuzzy_match %>%
    anti_join(exact_matches, by = c("full_name", "position")) %>%
    mutate(team = team.x) %>%
    select(full_name, position, player, team, everything())
  
  exact_matches_aligned <- exact_matches %>%
    mutate(team = coalesce(team.x, team.y)) %>%
    select(full_name, position, player, team, everything())
  
  all_matches <- bind_rows(exact_matches_aligned, fuzzy_clean)
  
  # Stat comparison
  stat_comparison <- all_matches %>%
    mutate(
      pass_yds_diff  = pass_yds - passing_yards,
      pass_tds_diff  = pass_tds - passing_tds,
      rush_yds_diff  = rush_yds - rushing_yards,
      rush_tds_diff  = rush_tds - rushing_tds,
      rec_yds_diff   = rec_yds - receiving_yards,
      rec_tds_diff   = rec_tds - receiving_tds,
      receptions_diff = rec - receptions
    )
  
  write.csv(stat_comparison, paste0("stat_comparison_", season_year, ".csv"), row.names = FALSE)
  
  # Accuracy by source
  source_accuracy <- all_matches %>%
    filter(!is.na(data_src)) %>%
    mutate(
      rec_yds_acc = ifelse(receiving_yards > 0, 1 - abs(rec_yds - receiving_yards) / receiving_yards, NA),
      rec_tds_acc = ifelse(receiving_tds > 0, 1 - abs(rec_tds - receiving_tds) / receiving_tds, NA),
      recs_acc    = ifelse(receptions > 0, 1 - abs(rec - receptions) / receptions, NA),
      rush_yds_acc = ifelse(rushing_yards > 0, 1 - abs(rush_yds - rushing_yards) / rushing_yards, NA),
      rush_tds_acc = ifelse(rushing_tds > 0, 1 - abs(rush_tds - rushing_tds) / rushing_tds, NA),
      pass_yds_acc = ifelse(passing_yards > 0, 1 - abs(pass_yds - passing_yards) / passing_yards, NA),
      pass_tds_acc = ifelse(passing_tds > 0, 1 - abs(pass_tds - passing_tds) / passing_tds, NA)
    ) %>%
    group_by(data_src) %>%
    summarize(
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
    mutate(across(starts_with("avg_"), ~ round(. * 100, 1)),
           overall_accuracy = round(overall_accuracy, 1))
  
  write.csv(source_accuracy, paste0("source_accuracy_", season_year, ".csv"), row.names = FALSE)
  
  message(glue::glue("✅ Complete: saved comparison and accuracy files for {season_year}"))
  return(source_accuracy)
}

run_projection_accuracy(2024)
