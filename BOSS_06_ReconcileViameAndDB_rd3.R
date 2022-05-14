# BOSS: Compare VIAME hotspots to DB for reconciliation
# S. Hardy, 06JUL2017

# Create functions -----------------------------------------------
# Function to install packages needed
install_pkg <- function(x)
{
  if (!require(x,character.only = TRUE))
  {
    install.packages(x,dep=TRUE)
    if(!require(x,character.only = TRUE)) stop("Package not found")
  }
}

# Install libraries ----------------------------------------------
install_pkg("RPostgres")
install_pkg("tidyverse")

# Run code -------------------------------------------------------
# Import GMB and SMW datasets
bb <- read.csv("C:/Users/stacie.hardy/Work/Work/Projects/AS__Annotations/Data/BOSS_BoundingBoxes/9_boss_yolo_eo_20190809_updated_SKH_GMB.csv", header = FALSE, stringsAsFactors = FALSE)
colnames(bb) <- c("viame_id", "slr_image", "frame_num", "updated_left", "updated_top", "updated_right", "updated_bottom", "probability", "junk1", "species", "junk2")

hotspots <- read.csv("C:/Users/stacie.hardy/Work/Work/Projects/AS__Annotations/Data/BOSS_BoundingBoxes/9_boss_pairings_detections2database_20210308_SMW.csv", stringsAsFactors = FALSE) %>%
  filter(hotspot_id != 'NULL') %>%
  mutate(hotspot_id = as.integer(hotspot_id)) %>%
  select(detection, detection_image, hotspot_id, thermal_image, Notes) %>%
  rename(notes = Notes,
         viame_id = detection,
         color_image = detection_image) %>%
  select(viame_id, hotspot_id, thermal_image, color_image, notes) 
  

# Get sightings data from DB
con <- RPostgres::dbConnect(Postgres(), 
                            dbname = Sys.getenv("pep_db"), 
                            host = Sys.getenv("pep_ip"), 
                            port = Sys.getenv("pep_port"),
                            user = Sys.getenv("pep_user"),
                            password = Sys.getenv("user_pw"))
                            #password = rstudioapi::askForPassword(paste("Enter your DB password for user account: ", Sys.getenv("pep_user"), sep = "")))

sightings_db <- RPostgres::dbGetQuery(con, "SELECT hotspot_id, species, num_seals FROM surv_boss.geo_hotspots WHERE hotspot_type = \'seal\'")
images_db <- RPostgres::dbGetQuery(con, "SELECT DISTINCT hotspot_id, slr_image FROM surv_boss.tbl_hotspot_match WHERE hotspot_type = \'seal\'")

sightings_db <- merge(images_db, sightings_db, by = "hotspot_id", all = TRUE) %>%
  mutate(#hotspot_id = as.character(hotspot_id),
         num_seals = as.integer(num_seals),
         species = ifelse(species == "rd", "ringed_seal",
                          ifelse(species == "bd", "bearded_seal",
                                 ifelse(species == "unk", "unknown_seal",
                                        ifelse(species == "rn", "ribbon_seal", "spotted_seal"))))) %>%
  rename(species_db = species) 
rm(images_db)

# Merge datasets
data <- bb %>%
  left_join(hotspots, by = "viame_id") %>% # left_join because there are some hotspots in the table SMW processed that are no longer in GMB detections (any that matter will get picked up in missed seals query)
  mutate(viame_id = ifelse(is.na(slr_image), NA, viame_id)) %>%
  full_join(sightings_db, by = c("hotspot_id", "slr_image")) %>%
  unique() %>%
  filter(species != 'incorrect' | is.na(species)) %>% # to remove false positives
  filter(!grepl('duplicate', notes) | is.na(notes)) %>% # to remove duplicates identified by SMW
  mutate(hotspot_id = as.numeric(ifelse(hotspot_id == 'NULL' | is.na(hotspot_id), '99999', hotspot_id)),
         viame_id = as.numeric(ifelse(is.na(viame_id), 900000 + hotspot_id, viame_id))) %>%
  filter(viame_id != 999999 | hotspot_id != 99999)
rm(bb, hotspots)

# Query for any missing seals and add to dataset
to_add <- data %>% 
  group_by(hotspot_id, slr_image) %>%
  summarize(num_seals_bb = length(hotspot_id)) %>%
  filter(hotspot_id < 99999) %>%
  full_join(sightings_db, by = c("hotspot_id", "slr_image")) %>%
  mutate(num_seals_bb = ifelse(is.na(num_seals_bb), 0, num_seals_bb)) %>%
  filter(num_seals > num_seals_bb) %>%
  mutate(num_missing = num_seals - num_seals_bb) %>%
  ungroup()
rm(sightings_db)
  
to_add2 <- to_add %>%
  filter(num_missing == 2)

if(max(to_add$num_missing) > 2) {
  stop("too many records to add")
}

to_add2 <- to_add %>%
  filter(num_missing == 2)

data <- data %>%
  select(viame_id, slr_image, updated_left, updated_top, updated_right, updated_bottom, probability, junk1, species, junk2, hotspot_id)
  
to_add <- rbind(to_add, to_add2) %>%
  mutate(viame_id = 1:n() + max(data$viame_id),
         updated_top = 50,
         updated_bottom = 100,
         updated_left = 50,
         updated_right= 100,
         probability = 1,
         junk1 = -1,
         species = 'to_adjust_bb',
         junk2 = 1) %>%
  select(viame_id, slr_image, updated_left, updated_top, updated_right, updated_bottom, probability, junk1, species, junk2, hotspot_id)

data <- rbind(data, to_add)
rm(to_add, to_add2)

# Create image list for assigning frame number to review dataset
images <- data.frame(slr_image = unique(data$slr_image), stringsAsFactors = FALSE)
images <- images %>%
  arrange(slr_image) %>%
  mutate(frame_num = as.integer(row.names(images)) - 1)

data <- data %>%
  left_join(images, by = "slr_image") %>%
  mutate(updated_top = ifelse(is.na(updated_top), 50, updated_top),
         updated_bottom = ifelse(is.na(updated_bottom), 100, updated_bottom),
         updated_left = ifelse(is.na(updated_left), 50, updated_left),
         updated_right = ifelse(is.na(updated_right), 100, updated_right),
         probability = ifelse(is.na(probability), 1, probability),
         junk1 = ifelse(is.na(junk1), -1, junk1),
         junk2 = ifelse(is.na(junk2), 1, junk2),
         species = ifelse(is.na(species), 'to_adjust_bb', species),
         hotspot_id = paste('(trk-atr) hotspot_id ', hotspot_id, sep = "")
         ) %>%
  select(viame_id, slr_image, frame_num, updated_left, updated_top, updated_right, updated_bottom, probability, junk1, species, junk2, hotspot_id) %>%
  arrange(frame_num)

images <- images$slr_image

# Export final datasets
write.table(images, "C:/skh/boss_seals_imageList_20220514_4bbEdit_correction.txt", row.names = FALSE, col.names = FALSE, quote = FALSE)
write.table(data, "C:/skh/boss_seals_detectionsRGB_20220514_4bbEdit_correction.csv", sep = ',', row.names = FALSE, col.names = FALSE, quote = FALSE)

# Disconnect from DB
RPostgres::dbDisconnect(con)
rm(con)