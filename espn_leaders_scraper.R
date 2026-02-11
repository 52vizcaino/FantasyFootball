# ESPN Fantasy Football leaders scraper
#
# This script exposes `scrape_espn_leaders()` which collects player leaders from
# ESPN Fantasy Football for a league and season context.

suppressPackageStartupMessages({
  library(rvest)
  library(dplyr)
  library(stringr)
  library(purrr)
  library(tidyr)
})

# Determine whether ESPN leaders should use current or last season split.
# ESPN leaders supports `currSeason` and `lastSeason` in this view.
resolve_stat_split <- function(year, current_year = as.integer(format(Sys.Date(), "%Y"))) {
  if (year == current_year) {
    return("currSeason")
  }
  if (year == current_year - 1L) {
    return("lastSeason")
  }

  stop(
    sprintf(
      "Year %s cannot be mapped to ESPN leaders statSplit. Use %s (currSeason) or %s (lastSeason), or set `stat_split` directly.",
      year,
      current_year,
      current_year - 1L
    ),
    call. = FALSE
  )
}

# Scrape ESPN fantasy leaders page and return a tidy player-level data frame.
#
# @param year Integer year used to choose statSplit (currSeason/lastSeason).
# @param league_id ESPN league id.
# @param scoring_period_id ESPN scoring period id (0 = season-to-date).
# @param stat_split Optional manual override ("currSeason" or "lastSeason").
# @param user_agent Optional user agent string.
#
# @return A tibble with player_name, type (team), FPTS, AVG, and all available
#         stats columns from the leaders table.
scrape_espn_leaders <- function(
  year,
  league_id = 140701737,
  scoring_period_id = 0,
  stat_split = NULL,
  user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
) {
  if (!is.numeric(year) || length(year) != 1 || is.na(year)) {
    stop("`year` must be a single numeric year value.", call. = FALSE)
  }

  year <- as.integer(year)
  if (is.null(stat_split)) {
    stat_split <- resolve_stat_split(year)
  }

  valid_splits <- c("currSeason", "lastSeason")
  if (!stat_split %in% valid_splits) {
    stop("`stat_split` must be one of: currSeason, lastSeason.", call. = FALSE)
  }

  leaders_url <- paste0(
    "https://fantasy.espn.com/football/leaders",
    "?leagueId=", league_id,
    "&statSplit=", stat_split,
    "&scoringPeriodId=", scoring_period_id
  )

  pg <- read_html(httr::GET(leaders_url, httr::user_agent(user_agent)))

  raw_tables <- html_elements(pg, "table")
  if (length(raw_tables) == 0) {
    stop(
      paste0(
        "No HTML tables were found at the ESPN leaders URL. ",
        "The page may require JavaScript execution or may be blocked in this environment."
      ),
      call. = FALSE
    )
  }

  candidates <- map(raw_tables, ~ html_table(.x, fill = TRUE))
  leaders_tbl <- keep(candidates, ~ {
    nms <- names(.x)
    length(nms) > 0 && any(str_to_upper(nms) %in% c("PLAYER", "PLAYERS"))
  }) %>%
    pluck(1)

  if (is.null(leaders_tbl) || nrow(leaders_tbl) == 0) {
    stop("Could not find the ESPN leaders player table.", call. = FALSE)
  }

  player_col <- names(leaders_tbl)[str_to_upper(names(leaders_tbl)) %in% c("PLAYER", "PLAYERS")][1]

  cleaned <- leaders_tbl %>%
    rename(player_raw = all_of(player_col)) %>%
    mutate(
      player_raw = str_squish(as.character(player_raw)),
      # Remove leading rank number if present: "1. Josh Allen BUF"
      player_raw = str_replace(player_raw, "^\\d+\\.?\\s+", ""),
      player_raw = str_replace(player_raw, "^#\\d+\\s+", ""),
      type = str_extract(player_raw, "\\b[A-Z]{2,3}\\b$"),
      player_name = str_squish(str_remove(player_raw, "\\b[A-Z]{2,3}\\b$")),
      player_name = na_if(player_name, ""),
      type = na_if(type, "")
    ) %>%
    relocate(player_name, type, .before = everything())

  numeric_candidates <- names(cleaned)[!names(cleaned) %in% c("player_name", "type", "player_raw")]

  cleaned <- cleaned %>%
    mutate(
      across(
        all_of(numeric_candidates),
        ~ if (is.character(.x)) readr::parse_number(.x) else .x
      )
    )

  cleaned %>%
    select(-player_raw) %>%
    filter(!is.na(player_name))
}

# Example:
# leaders_2025 <- scrape_espn_leaders(2025)
# leaders_2024 <- scrape_espn_leaders(2024)
