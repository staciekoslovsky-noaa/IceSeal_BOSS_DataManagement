# BOSS: Generate image lists and detection file for training images
# S. Hardy

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

images <- RPostgreSQL::dbGetQuery(con, "SELECT image_name FROM surv_boss.tbl_images WHERE ml_imagestatus = \'training\' ORDER BY image_name")
write.table(images, "D:/BOSS_Color_Training/BOSS_TrainingImages_20220712_ImageList.txt", row.names = FALSE, col.names = FALSE, quote = FALSE)
write.table(images, "E:/BOSS_Color_Training/BOSS_TrainingImages_20220712_ImageList.txt", row.names = FALSE, col.names = FALSE, quote = FALSE)

detections <- RPostgreSQL::dbGetQuery(con, "SELECT image_name, hotspot_id, bound_left, bound_top, bound_right, bound_bottom, detection_type as species_id, species_conf 
                                  FROM surv_boss.tbl_detections_processed_rgb
                                  LEFT JOIN surv_boss.geo_hotspots USING (hotspot_id)
                                  WHERE image_name in (SELECT image_name FROM surv_boss.tbl_images WHERE ml_imagestatus = \'training\') 
                                  AND (detection_type LIKE \'%seal\' OR detection_type LIKE \'%pup\')
                                  ORDER BY image_name")
write.csv(detections, "D:/BOSS_Color_Training/BOSS_TrainingImages_20220712_Detections.csv", row.names = FALSE)
write.csv(detections, "E:/BOSS_Color_Training/BOSS_TrainingImages_20220712_Detections.csv", row.names = FALSE)

RPostgreSQL::dbDisconnect(con)
