#weighted averages

accuracy_2022 <- read_csv("source_accuracy_2022.csv") %>% mutate(season = 2022)
accuracy_2023 <- read_csv("source_accuracy_2023.csv") %>% mutate(season = 2023)
accuracy_2024 <- read_csv("source_accuracy_2024.csv") %>% mutate(season = 2024)

historical_accuracy <- bind_rows(accuracy_2022, accuracy_2023, accuracy_2024)

source_weights <- historical_accuracy %>%
  group_by(data_src) %>%
  summarize(
    mean_accuracy = mean(overall_accuracy, na.rm = TRUE)
  ) %>%
  mutate(
    mean_accuracy = pmax(mean_accuracy, 0),  # no negatives
    weight = mean_accuracy / sum(mean_accuracy, na.rm = TRUE)
  )

proj_2025 <- scrape_data(
  season = 2025,
  week = 0,
  src = c("FantasyPros", "CBS", "NFL", "ESPN", "Yahoo"),
  pos = c("QB", "RB", "WR", "TE")
)

source_weights <- source_weights %>%
  add_row(data_src = "NFL", mean_accuracy = NA, weight = 0.25)

source_weights <- source_weights %>%
  mutate(weight = weight / sum(weight, na.rm = TRUE))

clean_player_name <- function(name) {
  name %>%
    str_to_lower() %>%
    str_replace_all("\\b(jr|sr|ii|iii|iv|v)\\b", "") %>%
    str_replace_all("[^a-z ]", "") %>%
    str_squish()
}

name_map <- tribble(
  ~alias,            ~canonical,
  "cam ward",        "cameron ward",
  "chig okonkwo",    "chigoziem okonkwo",
  "marvin mims jr",  "marvin mims",
  "bijan robinson ii", "bijan robinson",
  "audric estim","audric estime"
  
) %>%
  mutate(
    alias = clean_player_name(alias),
    canonical = clean_player_name(canonical)
  )

proj_weighted <- bind_rows(proj_2025, .id = "position") %>%
  mutate(
    full_name = clean_player_name(player)
  ) %>%
  left_join(name_map, by = c("full_name" = "alias")) %>%
  mutate(
    full_name = coalesce(canonical, full_name)
  ) %>%
  select(-canonical) %>%
  left_join(source_weights, by = "data_src") %>%
  filter(!is.na(weight))

proj_weighted_clean <- proj_weighted %>%
  group_by(full_name, position) %>%
  summarize(
    pass_att     = weighted.mean(pass_att, weight, na.rm = TRUE),
    pass_comp    = weighted.mean(pass_comp, weight, na.rm = TRUE),
    pass_yds     = weighted.mean(pass_yds, weight, na.rm = TRUE),
    pass_tds     = weighted.mean(pass_tds, weight, na.rm = TRUE),
    pass_int     = weighted.mean(pass_int, weight, na.rm = TRUE),
    rush_att     = weighted.mean(rush_att, weight, na.rm = TRUE),
    rush_yds     = weighted.mean(rush_yds, weight, na.rm = TRUE),
    rush_tds     = weighted.mean(rush_tds, weight, na.rm = TRUE),
    rec_tgt      = weighted.mean(rec_tgt, weight, na.rm = TRUE),
    rec_yds      = weighted.mean(rec_yds, weight, na.rm = TRUE),
    rec_tds      = weighted.mean(rec_tds, weight, na.rm = TRUE),
    rec          = weighted.mean(rec, weight, na.rm = TRUE),
    fumbles_lost = weighted.mean(fumbles_lost, weight, na.rm = TRUE),
    .groups = "drop"
  )


roster_2025 <- load_rosters(2025) %>%
  mutate(clean_name = clean_player_name(full_name)) %>%
  select(clean_name, team, position, rookie_year, birth_date = birth_date)

# Calculate age
roster_2025 <- roster_2025 %>%
  mutate(age = lubridate::interval(birth_date, Sys.Date()) %/% lubridate::years(1))

# Join projections with roster
proj_final_2025 <- proj_weighted_clean %>%
  left_join(roster_2025, by = c("full_name" = "clean_name", "position" = "position")) %>%
  select(full_name, team, position, age, rookie_year, everything())

write.csv(proj_final_2025, "weighted_projections_2025.csv", row.names = FALSE)

