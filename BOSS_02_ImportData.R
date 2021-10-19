# Import effort, priority data to surv_pv_cst DB
# S. Hardy, 13JUN2017

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
install_pkg("RPostgreSQL")
install_pkg("lubridate")

# Run code -------------------------------------------------------
# Set initial working directory
wd <- "//akc0SS-N086/NMML_Users/Stacie.Hardy/Work/Projects/AS_BOSS/Data/oracle_exports"
setwd(wd)
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              rstudioapi::askForPassword(paste("Enter your DB password for user account: ", Sys.getenv("pep_admin"), sep = "")))

# Log data
log <- read.csv("FMCLOGS_201707051537.csv", stringsAsFactors = FALSE)
log$ID <- as.numeric(gsub(",", "", log$ID))
log$GPSALT <- as.numeric(gsub(",", "", log$GPSALT))
log$BAROALT <- as.numeric(gsub(",", "", log$BAROALT))
dbWriteTable(con, c("surv_boss", "geo_fmclogs"), log, append = TRUE, row.names = FALSE)
RPostgreSQL::dbSendQuery(con, "ALTER TABLE surv_boss.geo_fmclogs ADD COLUMN geom geometry(POINT, 4326)")
RPostgreSQL::dbSendQuery(con, "UPDATE surv_boss.geo_fmclogs SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)")

# Hotspot detect data
detect <- read.csv("HOTSPOTS_DETECT_201707051538.csv", stringsAsFactors = FALSE)
detect <- detect[, c(1:11, 16, 20:22)]
detect$ï..HOTSPOTID <- as.numeric(gsub(",", "", detect$ï..HOTSPOTID))
detect$FRAME <- as.numeric(gsub(",", "", detect$FRAME))
detect$FLIGHTDATE <- format(as.Date(ifelse(detect$FLIGHTDATE == "", "1111-01-01 00:00:00", detect$FLIGHTDATE), format="%Y-%m-%d"), "%Y-%m-%d")
detect$HOTSPOT_DT <- format(ymd_hms(detect$HOTSPOT_DT, tz = "America/Vancouver"), tz = "UTC")
detect$HOTSPOT_DT <- ifelse(detect$HOTSPOT_DT == "", "1111-01-01 00:00:00", detect$HOTSPOT_DT)
colnames(detect) <- c("hotspot_id", "hotspot_time", "thermal_image", "center_line_cross", "threshold_cross", "threshold_value", "flight_id", 
                      "camera_id", "flight_date", "frame", "user_id", "stats_filename", "disturb", "detect_method", "hotspot_dt")
RPostgreSQL::dbWriteTable(con, c("surv_boss", "tbl_hotspot_detect"), detect, append = TRUE, row.names = FALSE)

# Hotspot detect redo data
redo <- read.csv("HOTSPOTS_DETECT_REDO_201707051538.csv", stringsAsFactors = FALSE)
redo <- redo[, c(1:11, 16, 20:22)]
redo$ï..HOTSPOTID <- as.numeric(gsub(",", "", redo$ï..HOTSPOTID))
redo$FRAME <- as.numeric(gsub(",", "", redo$FRAME))
redo$FLIGHTDATE <- ifelse(redo$FLIGHTDATE == "", "1111-01-01 00:00:00", redo$FLIGHTDATE)
redo$HOTSPOT_DT <- format(ymd_hms(redo$HOTSPOT_DT, tz = "America/Vancouver"), tz = "UTC")
colnames(redo) <- c("hotspot_id", "hotspot_time", "thermal_image", "center_line_cross", "threshold_cross", "threshold_value", "flight_id", 
                      "camera_id", "flight_date", "frame", "user_id", "stats_filename", "disturb", "detect_method", "hotspot_dt")
RPostgreSQL::dbWriteTable(con, c("surv_boss", "tbl_hotspot_detect_redo"), redo, append = TRUE, row.names = FALSE)

# Hotspot match data
match <- read.csv("HOTSPOTS_MATCH_201707051539.csv", stringsAsFactors = FALSE)
match$ï..HOTSPOTID <- as.numeric(gsub(",", "", match$ï..HOTSPOTID))
match$MATCHID <- as.numeric(gsub(",", "", match$MATCHID))
colnames(match) <- c("hotspot_id", "slr_match", "slr_image", "hotspot_found", "hotspot_type", "num_seals", "fog", "match_uncertainty", "slr_data", 
                     "notes", "user_id", "match_dt", "match_id", "disturb", "species_id", "species_conf", "gross_age", 'gross_age_conf', "species_alt", 
                     "gross_age_alt", "species_alt_conf", "gross_age_alt_conf", "species_user")
dbWriteTable(con, c("surv_boss", "tbl_hotspot_match"), match, append = TRUE, row.names = FALSE)

# Species id data
species <- read.csv("SPECIES_ID_201707051539.csv", stringsAsFactors = FALSE)
species$ï..ASSIGNID <- as.numeric(gsub(",", "", species$ï..ASSIGNID))
species$HOTSPOTID <- as.numeric(gsub(",", "", species$HOTSPOTID))
RPostgreSQL::dbWriteTable(con, c("surv_boss", "tbl_species_id"), species, append = TRUE, row.names = FALSE)

# Disconnect for database and delete unnecessary variables ----------------------------
dbDisconnect(con)
rm(con, log, detect, redo, match, species, wd)
