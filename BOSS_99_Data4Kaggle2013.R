# Process data to send to Kaggle
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
install_pkg("RPostgreSQL")

# Run code -------------------------------------------------------
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_user"), 
                              rstudioapi::askForPassword(paste("Enter your DB password for user account: ", Sys.getenv("pep_user"), sep = "")))
files <- RPostgreSQL::dbGetQuery(con, "SELECT flight_id, camera_id, hotspot_id, thermal_image, slr_image, hotspot_dt FROM surv_boss.hotspot WHERE slr_image LIKE \'BOSS13%\' AND (hotspot_type = \'seal\' OR hotspot_type = \'other_animal\' OR hotspot_type = \'unknown\')")
dat <- files
#dat <- read.csv("//nmfs/akc-nmml/NMML_CHESS_Imagery/ImagesForKaggle/BOSS_hotspots_Kaggle.csv", stringsAsFactors = FALSE)

wd <- "//nmfs/akc-nmml/Polar_Imagery_2/Surveys_IceSeals_BOSS_2013"
setwd(wd)

# dat$id <- rownames(dat)
# dat <- merge(dat, files, by.x = "hotspotid", by.y = "hotspot_id", all.x = TRUE)

dat$hotspot_dt <- as.POSIXct(dat$hotspot_dt, format="%m/%d/%Y %H:%M:%S", tz=Sys.timezone())
dat$folder <- substr(dat$flight_id, 4, length(dat$flight_id))
dat$loc <- ifelse(substr(dat$camera_id, 1, 1) == "C", "Center",
                  ifelse(substr(dat$camera_id, 1, 1) == "P", "Port",
                         ifelse(substr(dat$camera_id, 1, 1) == "S", "Starboard", "HELP!")))
dat$color <- paste(wd, "/Images/", dat$folder, "/", dat$loc, "/", dat$slr_image, sep = "")
dat$thermal <- paste(wd, "/Thermal_Frames/", dat$folder, "/", dat$loc, "/", dat$thermal_image, sep = "")

# No random selection needed because there are fewer than 2500 thermal images with seals in this dataset
#animal <- unique(dat[which(dat$hotspot_type == 'seal'), c("thermal_image")])
#rand_animal <- sample(animal, 2500)
#dat$rand_ani <- ifelse(dat$thermal_image %in% rand_animal, "Select", "")
#selected <- dat[which(dat$rand_ani == 'Select' | dat$hotspot_type == 'dirty_ice_anomaly' | dat$hotspot_type == 'unknown'), ]

color <- unique(dat$color)
thermal <- unique(dat$thermal)

file.copy(color, "//nmfs/akc-nmml/NMML_CHESS_Imagery/ImagesForDetectionDevelopment/BOSS/BOSS_color")
file.copy(thermal, "//nmfs/akc-nmml/NMML_CHESS_Imagery/ImagesForDetectionDevelopment/BOSS/BOSS_thermal")

dat <- dat[, c(1:13, 16:17)]
write.csv(dat, "//nmfs/akc-nmml/NMML_CHESS_Imagery/ImagesForDetectionDevelopment/_BOSS_ImagesSelected4ImageDev2013.csv", row.names = FALSE)
#zip(zipfile = "//nmfs/akc-nmml/NMML_CHESS_Imagery/ImagesForDetectionDevelopment/BOSS_sampleImages", files = "//nmfs/akc-nmml/NMML_CHESS_Imagery/ImagesForKaggle/BOSS")
