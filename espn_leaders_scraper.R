# ESPN Fantasy Football leaders scraper
#
# This script exposes `scrape_espn_leaders()` which collects player leaders from
# ESPN Fantasy Football for a league and season context.

suppressPackageStartupMessages({
  library(dplyr)
  library(httr)
  library(jsonlite)
  library(purrr)
  library(readr)
  library(stringr)
  library(tibble)
})


`%||%` <- function(x, y) if (is.null(x)) y else x
# Determine whether ESPN leaders should use current or last season split.
# ESPN leaders supports `currSeason` and `lastSeason` in this view.
resolve_stat_split <- function(year, current_year = as.integer(format(Sys.Date(), "%Y"))) {
  if (year == current_year) return("currSeason")
  if (year == current_year - 1L) return("lastSeason")

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

# Translate statSplit into the season ESPN should query.
resolve_target_season <- function(year, stat_split, current_year = as.integer(format(Sys.Date(), "%Y"))) {
  if (stat_split == "currSeason") return(as.integer(year))
  if (stat_split == "lastSeason") return(as.integer(year) - 1L)

  stop("`stat_split` must be one of: currSeason, lastSeason.", call. = FALSE)
}

# Safely extract a player's stat value by stat id from a stats list.
extract_stat_value <- function(stats_list, stat_id, split_type = 0L, scoring_period_id = 0L, season = NULL) {
  if (is.null(stats_list) || length(stats_list) == 0) return(NA_real_)

  candidates <- keep(stats_list, function(st) {
    !is.null(st$statSourceId) &&
      !is.null(st$statSplitTypeId) &&
      !is.null(st$scoringPeriodId) &&
      st$statSourceId == 0 &&
      st$statSplitTypeId == split_type &&
      st$scoringPeriodId == scoring_period_id &&
      (is.null(season) || is.null(st$seasonId) || st$seasonId == season)
  })

  if (length(candidates) == 0) {
    candidates <- keep(stats_list, function(st) {
      !is.null(st$statSourceId) &&
        !is.null(st$statSplitTypeId) &&
        st$statSourceId == 0 &&
        st$statSplitTypeId == split_type &&
        (is.null(season) || is.null(st$seasonId) || st$seasonId == season)
    })
  }

  if (length(candidates) == 0) return(NA_real_)

  stat_map <- candidates[[1]]$stats
  if (is.null(stat_map)) return(NA_real_)

  val <- stat_map[[as.character(stat_id)]]
  if (is.null(val)) return(NA_real_)
  as.numeric(val)
}


# Select the most relevant ESPN stat entry for the chosen split/period.
get_stat_entry <- function(stats_list, split_type = 0L, scoring_period_id = 0L, season = NULL) {
  if (is.null(stats_list) || length(stats_list) == 0) return(NULL)

  exact <- keep(stats_list, function(st) {
    !is.null(st$statSourceId) &&
      !is.null(st$statSplitTypeId) &&
      !is.null(st$scoringPeriodId) &&
      st$statSourceId == 0 &&
      st$statSplitTypeId == split_type &&
      st$scoringPeriodId == scoring_period_id &&
      (is.null(season) || is.null(st$seasonId) || st$seasonId == season)
  })
  if (length(exact) > 0) return(exact[[1]])

  split_only <- keep(stats_list, function(st) {
    !is.null(st$statSourceId) &&
      !is.null(st$statSplitTypeId) &&
      st$statSourceId == 0 &&
      st$statSplitTypeId == split_type &&
      (is.null(season) || is.null(st$seasonId) || st$seasonId == season)
  })
  if (length(split_only) > 0) return(split_only[[1]])

  NULL
}

# Extract fantasy points/average directly from ESPN summary fields when available.
extract_fantasy_summary <- function(stats_list, split_type = 0L, scoring_period_id = 0L, season = NULL) {
  st <- get_stat_entry(stats_list, split_type = split_type, scoring_period_id = scoring_period_id, season = season)
  if (is.null(st)) {
    return(list(FPTS = NA_real_, AVG = NA_real_, GP = NA_real_))
  }

  fpts <- as.numeric(st$appliedTotal %||% NA_real_)
  avg <- as.numeric(st$appliedAverage %||% NA_real_)

  gp <- as.numeric(st$gamesPlayed %||% NA_real_)

  # Some payload variants expose misleading `gamesPlayed`; derive GP from
  # applied totals/averages when possible and keep a realistic range.
  gp_from_avg <- if (!is.na(fpts) && !is.na(avg) && avg != 0) fpts / avg else NA_real_
  if (!is.na(gp_from_avg) && gp_from_avg > 0 && gp_from_avg <= 25) {
    gp <- gp_from_avg
  }

  if (is.na(gp) || gp <= 0 || gp > 25) {
    gp_id <- extract_stat_value(stats_list, stat_id = 42, split_type = split_type, scoring_period_id = scoring_period_id, season = season)
    if (!is.na(gp_id) && gp_id > 0 && gp_id <= 25) gp <- gp_id
  }

  if (is.na(avg) && !is.na(fpts) && !is.na(gp) && gp > 0) {
    avg <- fpts / gp
  }

  list(FPTS = fpts, AVG = avg, GP = gp)
}

# Build API URL used by ESPN for fantasy player data.
espn_players_api_url <- function(season, league_id, host = "lm-api-reads.fantasy.espn.com") {
  sprintf(
    "https://%s/apis/v3/games/ffl/seasons/%s/segments/0/leagues/%s",
    host,
    season,
    league_id
  )
}

# Detect whether an HTTP response body is JSON (not HTML error/content pages).
is_json_response <- function(req) {
  ctype <- tolower(headers(req)[["content-type"]] %||% "")
  body <- content(req, as = "text", encoding = "UTF-8")
  first_char <- substr(trimws(body), 1, 1)

  has_json_type <- grepl("application/json|text/json", ctype)
  looks_like_json <- first_char %in% c("{", "[")

  list(ok = has_json_type || looks_like_json, body = body, content_type = ctype)
}

# Try known ESPN hosts/seasons and return the first successful response.
fetch_espn_players_payload <- function(seasons, league_id, scoring_period_id, views, user_agent, player_limit = 2000L, player_offset = 0L) {
  hosts <- c("lm-api-reads.fantasy.espn.com", "fantasy.espn.com")
  attempts <- character(0)

  fantasy_filter <- toJSON(
    list(players = list(
      limit = as.integer(player_limit),
      offset = as.integer(player_offset),
      sortAppliedTotal = list(sortPriority = 1, sortAsc = FALSE)
    )),
    auto_unbox = TRUE
  )

  request_variants <- list(
    list(name = "with-filter", extra = add_headers(`x-fantasy-filter` = fantasy_filter, Accept = "application/json, text/plain, */*")),
    list(name = "no-filter", extra = add_headers(Accept = "application/json, text/plain, */*"))
  )

  for (season in seasons) {
    for (host in hosts) {
      api_url <- espn_players_api_url(season, league_id, host = host)

      for (variant in request_variants) {
        req <- GET(
          api_url,
          user_agent(user_agent),
          variant$extra,
          query = c(list(scoringPeriodId = scoring_period_id), setNames(as.list(views), rep("view", length(views))))
        )

        if (!http_error(req)) {
          json_check <- is_json_response(req)
          if (isTRUE(json_check$ok)) {
            return(list(
              payload = json_check$body,
              season = as.integer(season),
              url = api_url,
              request_variant = variant$name
            ))
          }

          attempts <- c(
            attempts,
            sprintf("%s [%s] -> %s (non-JSON content-type: %s)", api_url, variant$name, status_code(req), json_check$content_type)
          )
          next
        }

        attempts <- c(attempts, sprintf("%s [%s] -> %s", api_url, variant$name, status_code(req)))
      }
    }
  }

  stop(
    paste0(
      "ESPN API request failed for all attempted endpoints (or returned non-JSON content). Tried: ",
      paste(attempts, collapse = "; "),
      ". This can happen for invalid season/league combinations, API filter incompatibilities (which may cap results to ~50 on no-filter responses), or temporary ESPN blocking. If your league is private, add cookies via httr::set_cookies()."
    ),
    call. = FALSE
  )
}

# Scrape ESPN fantasy leaders and return a tidy player-level data frame.
#
# @param year Integer season year to query (for example, 2025).
# @param league_id ESPN league id.
# @param scoring_period_id ESPN scoring period id (0 = season-to-date).
# @param stat_split Optional manual override ("currSeason" or "lastSeason").
# @param user_agent Optional user agent string.
# @param views ESPN API views to request.
# @param allow_season_fallback Logical; if TRUE, tries adjacent seasons when requested season is unavailable.
# @param player_limit Integer max players requested from ESPN API (default 2000).
# @param player_offset Integer player offset for API paging (default 0).
#
# @return A tibble with player_name, type (team), FPTS, AVG, and core box stats.
scrape_espn_leaders <- function(
  year,
  league_id = 140701737,
  scoring_period_id = 0,
  stat_split = NULL,
  user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
  views = c("kona_player_info", "players_wl"),
  allow_season_fallback = FALSE,
  player_limit = 2000L,
  player_offset = 0L
) {
  if (!is.numeric(year) || length(year) != 1 || is.na(year)) {
    stop("`year` must be a single numeric year value.", call. = FALSE)
  }

  year <- as.integer(year)
  if (is.null(stat_split)) stat_split <- "currSeason"

  valid_splits <- c("currSeason", "lastSeason")
  if (!stat_split %in% valid_splits) {
    stop("`stat_split` must be one of: currSeason, lastSeason.", call. = FALSE)
  }

  target_season <- resolve_target_season(year, stat_split)
  split_type_id <- ifelse(stat_split == "currSeason", 0L, 1L)

  seasons_to_try <- unique(c(
    target_season,
    if (isTRUE(allow_season_fallback)) c(target_season - 1L, target_season + 1L) else integer(0)
  ))
  seasons_to_try <- seasons_to_try[!is.na(seasons_to_try) & seasons_to_try >= 2000L]

  response <- fetch_espn_players_payload(
    seasons = seasons_to_try,
    league_id = league_id,
    scoring_period_id = scoring_period_id,
    views = views,
    user_agent = user_agent,
    player_limit = player_limit,
    player_offset = player_offset
  )
  parsed <- fromJSON(response$payload, simplifyVector = FALSE)
  requested_season <- as.integer(response$season %||% target_season)

  players <- parsed$players
  if (is.null(players) || length(players) == 0) {
    stop("No players returned from ESPN API response.", call. = FALSE)
  }
  if (identical(response$request_variant, "no-filter") && length(players) >= 50 && player_limit > 50) {
    warning(
      "ESPN returned the unfiltered response variant, which may be capped to ~50 players. ",
      "If this persists, ESPN is likely rejecting x-fantasy-filter for this request context.",
      call. = FALSE
    )
  }

  result <- map_dfr(players, function(p) {
    pinfo <- p$player
    if (is.null(pinfo)) return(NULL)

    stats_list <- pinfo$stats

    fantasy_summary <- extract_fantasy_summary(stats_list, split_type = split_type_id, scoring_period_id = scoring_period_id, season = requested_season)
    fpts <- fantasy_summary$FPTS
    games_played <- fantasy_summary$GP
    pass_yds <- extract_stat_value(stats_list, stat_id = 3, split_type = split_type_id, scoring_period_id = scoring_period_id, season = requested_season)
    pass_td <- extract_stat_value(stats_list, stat_id = 4, split_type = split_type_id, scoring_period_id = scoring_period_id, season = requested_season)
    rush_yds <- extract_stat_value(stats_list, stat_id = 24, split_type = split_type_id, scoring_period_id = scoring_period_id, season = requested_season)
    rush_td <- extract_stat_value(stats_list, stat_id = 25, split_type = split_type_id, scoring_period_id = scoring_period_id, season = requested_season)
    rec <- extract_stat_value(stats_list, stat_id = 53, split_type = split_type_id, scoring_period_id = scoring_period_id, season = requested_season)
    rec_yds <- extract_stat_value(stats_list, stat_id = 42, split_type = split_type_id, scoring_period_id = scoring_period_id, season = requested_season)
    rec_td <- extract_stat_value(stats_list, stat_id = 21, split_type = split_type_id, scoring_period_id = scoring_period_id, season = requested_season)

    avg <- fantasy_summary$AVG

    tibble(
      player_name = pinfo$fullName %||% NA_character_,
      type = pinfo$proTeamAbbreviation %||% NA_character_,
      position = pinfo$defaultPositionId %||% NA_real_,
      source_season = response$season,
      source_request_variant = response$request_variant %||% NA_character_,
      FPTS = fpts,
      AVG = avg,
      GP = games_played,
      PASS_YDS = pass_yds,
      PASS_TD = pass_td,
      RUSH_YDS = rush_yds,
      RUSH_TD = rush_td,
      REC = rec,
      REC_YDS = rec_yds,
      REC_TD = rec_td
    )
  })

  result %>%
    mutate(
      across(where(is.character), ~ na_if(str_squish(.x), "")),
      across(c(FPTS, AVG, GP, PASS_YDS, PASS_TD, RUSH_YDS, RUSH_TD, REC, REC_YDS, REC_TD), as.numeric)
    ) %>%
    arrange(desc(FPTS), player_name)
}


# Inspect available ESPN API fields/keys for debugging stat mappings.
inspect_espn_leaders_schema <- function(
  year,
  league_id = 140701737,
  scoring_period_id = 0,
  stat_split = NULL,
  user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
  views = c("kona_player_info", "players_wl"),
  player_limit = 2000L,
  player_offset = 0L
) {
  if (!is.numeric(year) || length(year) != 1 || is.na(year)) {
    stop("`year` must be a single numeric year value.", call. = FALSE)
  }

  year <- as.integer(year)
  if (is.null(stat_split)) stat_split <- "currSeason"

  target_season <- resolve_target_season(year, stat_split)
  seasons_to_try <- unique(c(as.integer(year), target_season, target_season - 1L, target_season + 1L))
  seasons_to_try <- seasons_to_try[!is.na(seasons_to_try) & seasons_to_try >= 2000L]

  response <- fetch_espn_players_payload(
    seasons = seasons_to_try,
    league_id = league_id,
    scoring_period_id = scoring_period_id,
    views = views,
    user_agent = user_agent,
    player_limit = player_limit,
    player_offset = player_offset
  )

  parsed <- fromJSON(response$payload, simplifyVector = FALSE)
  players <- parsed$players
  if (is.null(players) || length(players) == 0) {
    stop("No players returned from ESPN API response.", call. = FALSE)
  }

  first_wrapper <- players[[1]]
  first_player <- first_wrapper$player
  stats_entries <- first_player$stats %||% list()

  stat_entry_names <- sort(unique(unlist(map(stats_entries, names), use.names = FALSE)))
  stat_ids <- sort(unique(unlist(map(stats_entries, function(st) {
    if (is.null(st$stats)) return(character(0))
    names(st$stats)
  }), use.names = FALSE)))

  list(
    source_season = response$season,
    source_url = response$url,
    source_request_variant = response$request_variant %||% NA_character_,
    payload_top_level_names = names(parsed),
    player_wrapper_names = names(first_wrapper),
    player_names = names(first_player),
    stats_entry_names = stat_entry_names,
    available_stat_ids = stat_ids
  )
}


# Quick pull focused on ESPN fantasy summary metrics.
# Returns player_name, team, appliedTotal, appliedAverage, and stat context ids.
quick_pull_applied_points <- function(
  year,
  league_id = 140701737,
  scoring_period_id = 0,
  stat_split = "currSeason",
  user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
  views = c("kona_player_info", "players_wl"),
  player_limit = 2000L,
  player_offset = 0L
) {
  if (!is.numeric(year) || length(year) != 1 || is.na(year)) {
    stop("`year` must be a single numeric year value.", call. = FALSE)
  }

  year <- as.integer(year)
  valid_splits <- c("currSeason", "lastSeason")
  if (!stat_split %in% valid_splits) {
    stop("`stat_split` must be one of: currSeason, lastSeason.", call. = FALSE)
  }

  target_season <- resolve_target_season(year, stat_split)
  split_type_id <- ifelse(stat_split == "currSeason", 0L, 1L)

  seasons_to_try <- unique(c(as.integer(year), target_season, target_season - 1L, target_season + 1L))
  seasons_to_try <- seasons_to_try[!is.na(seasons_to_try) & seasons_to_try >= 2000L]

  response <- fetch_espn_players_payload(
    seasons = seasons_to_try,
    league_id = league_id,
    scoring_period_id = scoring_period_id,
    views = views,
    user_agent = user_agent,
    player_limit = player_limit,
    player_offset = player_offset
  )

  parsed <- fromJSON(response$payload, simplifyVector = FALSE)
  players <- parsed$players
  if (is.null(players) || length(players) == 0) {
    stop("No players returned from ESPN API response.", call. = FALSE)
  }

  map_dfr(players, function(p) {
    pinfo <- p$player
    if (is.null(pinfo)) return(NULL)

    stat_entry <- get_stat_entry(
      pinfo$stats,
      split_type = split_type_id,
      scoring_period_id = scoring_period_id,
      season = target_season
    )

    tibble(
      player_name = pinfo$fullName %||% NA_character_,
      team = pinfo$proTeamAbbreviation %||% NA_character_,
      appliedTotal = as.numeric(stat_entry$appliedTotal %||% NA_real_),
      appliedAverage = as.numeric(stat_entry$appliedAverage %||% NA_real_),
      statSplitTypeId = as.numeric(stat_entry$statSplitTypeId %||% NA_real_),
      scoringPeriodId = as.numeric(stat_entry$scoringPeriodId %||% NA_real_),
      source_season = response$season,
      source_request_variant = response$request_variant %||% NA_character_
    )
  }) %>%
    mutate(across(where(is.character), ~ na_if(str_squish(.x), ""))) %>%
    arrange(desc(appliedTotal), player_name)
}

# Example:
# leaders_2025 <- scrape_espn_leaders(2025)
# leaders_2024 <- scrape_espn_leaders(2025, stat_split = "lastSeason")
# applied_points <- quick_pull_applied_points(2025, player_limit = 2000)
