# Process BOSS detection data to DB
# S. Hardy, 10 August 2020

# Install libraries
library(tidyverse)
library(RPostgreSQL)

# Set variables for processing
wd <- "C:/Users/stacie.hardy/Work/Work/Projects/AS__Annotations/Data/BOSS_BoundingBoxes"
processed_file <- "boss_seals_detectionsRGB_20220514_4import_20220622_SKH.csv"
reviewer <- "GMB"

# Set up working environment
"%notin%" <- Negate("%in%")
setwd(wd)
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              #port = Sys.getenv("pep_port"), 
                              user = Sys.getenv("pep_admin"), 
                              password = Sys.getenv("admin_pw"))

# Delete data from tables
RPostgreSQL::dbSendQuery(con, "DELETE FROM surv_boss.tbl_detections_processed_rgb")

# Import data and process
## PROCESSED DATA
processed_id <- RPostgreSQL::dbGetQuery(con, "SELECT max(id) FROM surv_boss.tbl_detections_processed_rgb")
processed_id$max <- ifelse(length(processed_id) == 0 | is.na(processed_id), 0, processed_id$max)

processed <- read.csv(processed_file, header = FALSE, stringsAsFactors = FALSE, 
                      col.names = c("detection", "image_name", "frame_number", "bound_left", "bound_top", "bound_right", "bound_bottom", "score", "length", "detection_type", "type_score", "hotspot_id"))
processed <- processed %>%
  #mutate(image_name = sapply(strsplit(image_name, split= "\\/"), function(x) x[length(x)])) %>%
  mutate(id = 1:n() + processed_id$max) %>%
  mutate(detection_file = processed_file) %>%
  mutate(flight = sapply(strsplit(image_name, split= "\\_"), function(x) x[2])) %>%
  mutate(reviewer = reviewer) %>%
  mutate(camera_view = sapply(strsplit(image_name, split= "\\_"), function(x) x[3])) %>%
  mutate(detection_id = paste("boss", flight, camera_view, detection, sep = "_")) %>%
  mutate(hotspot_id = as.integer(gsub("\\(trk-atr\\) hotspot_id *", "", hotspot_id))) %>%
  select("id", "detection", "image_name", "frame_number", "bound_left", "bound_top", "bound_right", "bound_bottom", "score", "length", "detection_type", "type_score", "flight", "camera_view", "detection_id", "reviewer", "detection_file", "hotspot_id")

rm(processed_id)

# Import data to DB
RPostgreSQL::dbWriteTable(con, c("surv_boss", "tbl_detections_processed_rgb"), processed, append = TRUE, row.names = FALSE)
RPostgreSQL::dbDisconnect(con)
rm(con)
