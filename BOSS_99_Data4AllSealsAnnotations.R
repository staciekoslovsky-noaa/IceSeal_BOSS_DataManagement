# BOSS: Generate image lists and detection file for training images
# S. Koslovsky

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
install_pkg("rjson")
install_pkg("plyr")
install_pkg("stringr")
install_pkg("tidyverse")

# Get data from DB
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              password = Sys.getenv("admin_pw"))


detections <- RPostgreSQL::dbGetQuery(con, "SELECT image_name as a, image_dir || \'\\' || image_name as image_name, hotspot_id, bound_left, bound_top, bound_right, bound_bottom, detection_type as species_id, species_conf 
                                  FROM surv_boss.tbl_detections_processed_rgb
                                  LEFT JOIN surv_boss.geo_hotspots USING (hotspot_id)
                                  LEFT JOIN surv_boss.tbl_images USING (image_name)
                                  WHERE (detection_type LIKE \'%seal\' OR detection_type LIKE \'%pup\')
                                  ORDER BY image_name")
write.csv(detections, "C:/smk/BOSS_ALL_20240111_Detections.csv", row.names = FALSE)

images <- detections %>%
  select(image_name) %>%
  unique()
write.table(images, "C:/smk/BOSS_ALL_20240111_ImageList.txt", row.names = FALSE, col.names = FALSE, quote = FALSE)

RPostgreSQL::dbDisconnect(con)
