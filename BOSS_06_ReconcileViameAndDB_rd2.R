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
# Get sightings data from DB
con <- RPostgres::dbConnect(Postgres(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              port = Sys.getenv("pep_port"),
                              user = Sys.getenv("pep_user"), 
                              password = rstudioapi::askForPassword(paste("Enter your DB password for user account: ", Sys.getenv("pep_user"), sep = "")))

sightings_db <- RPostgres::dbGetQuery(con, "SELECT hotspot_id, species, num_seals FROM surv_boss.geo_hotspot WHERE hotspot_type = \'seal\'")
#sightings_db$hotspot_id <- as.integer(sightings_db$hotspot_id)
images_db <- RPostgres::dbGetQuery(con, "SELECT DISTINCT hotspot_id, slr_image FROM surv_boss.tbl_hotspot_match WHERE hotspot_type = \'seal\'")
RPostgres::dbDisconnect(con)
sightings_db <- merge(images_db, sightings_db, by = "hotspot_id", all = TRUE)
sightings_db$species <- ifelse(sightings_db$species == "rd", "ringed_seal",
                               ifelse(sightings_db$species == "bd", "bearded_seal",
                                      ifelse(sightings_db$species == "unk", "unknown_seal",
                                             ifelse(sightings_db$species == "rn", "ribbon_seal", "spotted_seal"))))
sightings_db$num_seals <- as.integer(sightings_db$num_seals)

# Get sightings from CSV
sightings_v_orig <- read.csv("C:/skh/boss_yolo_eo_20190809_processed_SKH.csv", header = FALSE, stringsAsFactors = FALSE)
colnames(sightings_v_orig) <- c("viame_id", "slr_image", "frame_num", "updated_left", "updated_top", "updated_right", "updated_bottom", "probability", "junk1", "species", "junk2")
sightings_v_orig <- sightings_v_orig[which(!is.na(sightings_v_orig$updated_left)), ]
sightings_v <- sightings_v_orig[which(!is.na(sightings_v_orig$updated_left)), ]
sightings_v <- sightings_v[which(sightings_v$species != "animal" & sightings_v$species != "incorrect"), ]

# Process total counts of seals by frame
sum_db <- aggregate(sightings_db$num_seals, by = list(sightings_db$slr_image), FUN = sum)
colnames(sum_db) <- c("slr_image", "count_db")

sum_v <- aggregate(sightings_v$viame_id, by = list(sightings_v$slr_image), FUN = length)
colnames(sum_v) <- c("slr_image", "count_viame")

sum_by_frame <- merge(sum_db, sum_v, by = c("slr_image"), all = TRUE)
sum_by_frame[is.na(sum_by_frame)] <- 0
sum_by_frame$diff <- sum_by_frame$count_db - sum_by_frame$count_v
rm(sum_db, sum_v)

# Process counts of seals by species by frame
sum_sp_db <- aggregate(sightings_db$num_seals, by = list(sightings_db$slr_image, sightings_db$species), FUN = sum)
colnames(sum_sp_db) <- c("slr_image", "species", "count_db")

sum_sp_v <- aggregate(sightings_v$viame_id, by = list(sightings_v$slr_image, sightings_v$species), FUN = length)
colnames(sum_sp_v) <- c("slr_image", "species", "count_viame")

sum_by_species <- merge(sum_sp_db, sum_sp_v, by = c("slr_image", "species"), all = TRUE)
sum_by_species[is.na(sum_by_species)] <- 0
sum_by_species$diff <- sum_by_species$count_db - sum_by_species$count_viame
rm(sum_sp_db, sum_sp_v)

# Create subsets of data for next steps of processing
# v_miss_db <- sum_by_frame[which(sum_by_frame$diff > 0), c("slr_image")]
# db_miss_v <- sum_by_frame[which(sum_by_frame$diff < 0), c("slr_image")]
# v_match_db <- sum_by_frame[which(sum_by_frame$diff == 0), c("slr_image")]

# Create image list and existing sightings list for sightings VIAME missed
images_v_miss_db <- data.frame(slr_image = sort(unique(sum_by_frame[which(sum_by_frame$diff > 0), c("slr_image")])), stringsAsFactors = FALSE)
images_v_miss_db$id <- as.integer(row.names(images_v_miss_db)) - 1

seals_v_miss_db <- sightings_v[which(sightings_v$slr_image %in% images_v_miss_db$slr_imag), ]
seals_v_miss_db <- merge(seals_v_miss_db, images_v_miss_db, by = "slr_image")
seals_v_miss_db$frame_num <- seals_v_miss_db$id
seals_v_miss_db <- subset(seals_v_miss_db, select = -c(id))
seals_v_miss_db <- seals_v_miss_db[, c(2, 1, 3:11)]
images_v_miss_db <- images_v_miss_db$slr_image

# Create image list and existing sightings list for sightings DB missed
images_db_miss_v <- data.frame(slr_image = sort(unique(sum_by_frame[which(sum_by_frame$diff < 0), c("slr_image")])), stringsAsFactors = FALSE)
images_db_miss_v$id <- as.integer(row.names(images_db_miss_v)) - 1

seals_db_miss_v <- sightings_v[which(sightings_v$slr_image %in% images_db_miss_v$slr_imag), ]
seals_db_miss_v <- merge(seals_db_miss_v, images_db_miss_v, by = "slr_image")
seals_db_miss_v$frame_num <- seals_db_miss_v$id
seals_db_miss_v <- subset(seals_db_miss_v, select = -c(id))
seals_db_miss_v <- seals_db_miss_v[, c(2, 1, 3:11)]
images_db_miss_v <- images_db_miss_v$slr_image

# create image list and existing sighitngs list where DB count = VIAME count
images_v_match_db <- data.frame(slr_image = sort(unique(sum_by_frame[which(sum_by_frame$diff == 0), c("slr_image")])), stringsAsFactors = FALSE)
images_v_match_db$id <- as.integer(row.names(images_v_match_db)) - 1

seals_v_match_db <- sightings_v[which(sightings_v$slr_image %in% images_v_match_db$slr_image), ]
seals_v_match_db <- merge(seals_v_match_db, images_v_match_db, by = "slr_image")
seals_v_match_db$frame_num <- seals_v_match_db$id
seals_v_match_db <- subset(seals_v_match_db, select = -c(id))
seals_v_match_db <- seals_v_match_db[, c(2, 1, 3:11)]
images_v_match_db <- images_v_match_db$slr_image

# Export data for reviewing images where seals were viame count does not equal DB count for review by GMB
images2review <- unique(rbind(data.frame(images = images_db_miss_v, stringsAsFactors = FALSE), data.frame(images = images_v_miss_db, stringsAsFactors = FALSE)))
images2review$frame_numR <- as.integer(row.names(images2review)) - 1
images2review_v <- merge(images2review, sightings_v, by.x = "images", by.y = "slr_image")
images2review_v <- images2review_v[, c("viame_id", "images", "frame_numR", "updated_left", "updated_top", "updated_right", "updated_bottom", "probability", "junk1", "species", "junk2")]
images2review <- images2review$images

#write.csv(images2review, "C:/skh/boss_reconcile_imageList_20200115_SKH.csv", row.names = FALSE)
#write.csv(images2review_v, "C:/skh/boss_reconcile_seals_20200115_SKH.csv", row.names = FALSE)

write.csv(images_v_match_db, "C:/skh/boss_matched_imageList_20200115_SKH.csv", row.names = FALSE)
write.csv(seals_v_match_db, "C:/skh/boss_matched_seals_20200115_SKH.csv", row.names = FALSE)


