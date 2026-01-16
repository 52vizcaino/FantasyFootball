# weighted averages (UPDATED: remove Yahoo + use crosswalk ids + dedupe within-source)

library(tidyverse)
library(ffanalytics)
library(nflreadr)
library(lubridate)

# ----------------------------
# Config
# ----------------------------
target_season <- 2025
target_week   <- 0
target_pos    <- c("QB", "RB", "WR", "TE")

# Yahoo removed
target_src <- c("FantasyPros", "CBS", "NFL", "ESPN")

# Optional: keep your manual NFL weight override behavior
use_fixed_nfl_weight <- TRUE
fixed_nfl_weight     <- 0.25

# ----------------------------
# Helpers
# ----------------------------
clean_player_name <- function(name) {
  name %>%
    str_to_lower() %>%
    str_replace_all("\\b(jr|sr|ii|iii|iv|v)\\b", "") %>%
    str_replace_all("[^a-z ]", "") %>%
    str_squish()
}

first_non_na <- function(x) {
  x <- x[!is.na(x) & x != ""]
  if (length(x) == 0) NA_character_ else x[[1]]
}

# ----------------------------
# Historical accuracy -> weights (drop Yahoo)
# ----------------------------
accuracy_2022 <- readr::read_csv("source_accuracy_2022.csv") %>% mutate(season = 2022)
accuracy_2023 <- readr::read_csv("source_accuracy_2023.csv") %>% mutate(season = 2023)
accuracy_2024 <- readr::read_csv("source_accuracy_2024.csv") %>% mutate(season = 2024)

historical_accuracy <- bind_rows(accuracy_2022, accuracy_2023, accuracy_2024) %>%
  filter(data_src %in% target_src)

# Base mean accuracies
source_weights_base <- historical_accuracy %>%
  group_by(data_src) %>%
  summarize(mean_accuracy = mean(overall_accuracy, na.rm = TRUE), .groups = "drop") %>%
  mutate(mean_accuracy = pmax(mean_accuracy, 0))

if (use_fixed_nfl_weight) {
  # Compute weights for non-NFL sources from accuracy, then scale them to (1 - fixed_nfl_weight)
  non_nfl <- source_weights_base %>% filter(data_src != "NFL")
  
  denom <- sum(non_nfl$mean_accuracy, na.rm = TRUE)
  if (is.na(denom) || denom == 0) {
    # fallback: equal weights among non-NFL if historical data is missing/zero
    non_nfl <- non_nfl %>%
      mutate(weight = (1 - fixed_nfl_weight) / n())
  } else {
    non_nfl <- non_nfl %>%
      mutate(weight = (mean_accuracy / denom) * (1 - fixed_nfl_weight))
  }
  
  source_weights <- bind_rows(
    non_nfl,
    tibble(data_src = "NFL", mean_accuracy = NA_real_, weight = fixed_nfl_weight)
  ) %>%
    select(data_src, mean_accuracy, weight)
  
} else {
  # Pure accuracy-derived weights across all sources available
  denom <- sum(source_weights_base$mean_accuracy, na.rm = TRUE)
  source_weights <- source_weights_base %>%
    mutate(weight = ifelse(denom > 0, mean_accuracy / denom, 1 / n())) %>%
    select(data_src, mean_accuracy, weight)
}

# ----------------------------
# Name overrides (keep your current mapping)
# ----------------------------
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

# ----------------------------
# Load roster (for canonical name/team/pos + age)
# ----------------------------
roster <- nflreadr::load_rosters(target_season) %>%
  transmute(
    gsis_id = as.character(gsis_id),
    roster_full_name = full_name,
    roster_name_clean = clean_player_name(full_name),
    roster_team = team,
    roster_pos  = position,
    rookie_year = rookie_year,
    birth_date  = birth_date,
    age = lubridate::interval(birth_date, Sys.Date()) %/% lubridate::years(1)
  )

# Crosswalk mapping table (mfl_id -> gsis_id)
ff_ids <- nflreadr::load_ff_playerids() %>%
  transmute(
    mfl_id  = as.character(mfl_id),
    gsis_id = as.character(gsis_id)
  )

# ----------------------------
# Scrape projections (NO Yahoo)
# ----------------------------
proj_list <- ffanalytics::scrape_data(
  season = target_season,
  week   = target_week,
  src    = target_src,
  pos    = target_pos
)

proj_all <- bind_rows(proj_list, .id = "pos_from_list")

# Robustly build a single "position" column
if ("pos" %in% names(proj_all)) {
  proj_all$position <- proj_all$pos
} else if (!"position" %in% names(proj_all)) {
  proj_all$position <- proj_all$pos_from_list
}

# Ensure expected columns exist (avoid errors if a field is absent)
if (!"team" %in% names(proj_all))   proj_all$team <- NA_character_
if (!"id" %in% names(proj_all))     proj_all$id <- NA_character_
if (!"src_id" %in% names(proj_all)) proj_all$src_id <- NA_character_

proj_all <- proj_all %>%
  mutate(
    mfl_id = as.character(id),
    src_id = as.character(src_id),
    name_clean = clean_player_name(player)
  ) %>%
  # Apply your alias -> canonical mapping first
  left_join(name_map, by = c("name_clean" = "alias")) %>%
  mutate(name_clean = coalesce(canonical, name_clean)) %>%
  select(-canonical) %>%
  # Map MFL -> GSIS, then pull canonical roster info when possible
  left_join(ff_ids, by = "mfl_id") %>%
  left_join(roster,  by = "gsis_id") %>%
  mutate(
    # Canonicalized fields
    name_canon = coalesce(roster_name_clean, name_clean),
    team_canon = coalesce(roster_team, team),
    pos_canon  = coalesce(roster_pos, position),
    
    # Player key for dedupe + grouping:
    # prefer gsis_id (best for accuracy checks), else mfl_id, else name+pos
    player_key = case_when(
      !is.na(gsis_id) & gsis_id != "" ~ paste0("gsis|", gsis_id),
      !is.na(mfl_id)  & mfl_id  != "" ~ paste0("mfl|",  mfl_id),
      TRUE ~ paste0("name|", name_canon, "|", pos_canon)
    )
  ) %>%
  filter(data_src %in% target_src, pos_canon %in% target_pos)

# ----------------------------
# Deduplicate within EACH source BEFORE weighting
# (prevents a single source from counting twice for the same player)
# ----------------------------
stat_cols <- intersect(
  c(
    "pass_att", "pass_comp", "pass_yds", "pass_tds", "pass_int",
    "rush_att", "rush_yds", "rush_tds",
    "rec_tgt", "rec_yds", "rec_tds", "rec",
    "fumbles_lost"
  ),
  names(proj_all)
)

proj_one_per_src <- proj_all %>%
  group_by(data_src, player_key, pos_canon) %>%
  summarize(
    gsis_id     = first_non_na(gsis_id),
    mfl_id      = first_non_na(mfl_id),
    full_name   = first_non_na(roster_full_name),  # keep nicer casing when available
    name_canon  = first_non_na(name_canon),
    team        = first_non_na(team_canon),
    
    across(all_of(stat_cols), ~ mean(.x, na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  # convert NaN -> NA (can happen if all values were NA)
  mutate(across(all_of(stat_cols), ~ ifelse(is.nan(.x), NA_real_, .x)))

# ----------------------------
# Weight by source, then weighted-average across sources
# ----------------------------
proj_weighted <- proj_one_per_src %>%
  left_join(source_weights, by = "data_src") %>%
  filter(!is.na(weight))

proj_weighted_clean <- proj_weighted %>%
  group_by(player_key, position = pos_canon) %>%
  summarize(
    gsis_id   = first_non_na(gsis_id),
    mfl_id    = first_non_na(mfl_id),
    full_name = first_non_na(full_name),
    name_canon = first_non_na(name_canon),
    team      = first_non_na(team),
    n_sources = n(),
    
    across(all_of(stat_cols), ~ weighted.mean(.x, w = weight, na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  mutate(across(all_of(stat_cols), ~ ifelse(is.nan(.x), NA_real_, .x)))

# ----------------------------
# Final join to roster attributes (age/rookie/team/name), keyed by gsis_id
# ----------------------------
proj_final <- proj_weighted_clean %>%
  left_join(
    roster %>%
      select(gsis_id, roster_full_name, roster_team, roster_pos, age, rookie_year),
    by = "gsis_id"
  ) %>%
  mutate(
    full_name = coalesce(roster_full_name, full_name, name_canon),
    team      = coalesce(roster_team, team),
    position  = coalesce(roster_pos, position)
  ) %>%
  select(
    player_id = gsis_id,
    mfl_id,
    full_name,
    team,
    position,
    age,
    rookie_year,
    n_sources,
    all_of(stat_cols)
  ) %>%
  distinct(player_id, mfl_id, position, .keep_all = TRUE)

write.csv(proj_final, "weighted_projections_2025.csv", row.names = FALSE)
