# BOSS: Process Images to DB
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

# Process images -------------------------------------------------
images2DB <- data.frame(image_name = as.character(""), image_type = as.character(""), image_dir = as.character(""), stringsAsFactors = FALSE)
images2DB <- images2DB[which(images2DB == "test"), ]

# Process RGB images 
# Set working folders
wd_2012 <- "//akc0ss-n086/NMML_POlar_Imagery/Surveys_IceSeals/BOSS/2012/Images"
wd_2013 <- "//akc0ss-n086/NMML_POlar_Imagery_2/Surveys_IceSeals_BOSS_2013/Images"

# Create list of camera folders within which data need to be processed 
dir_2012 <- list.dirs(wd_2012, full.names = FALSE, recursive = FALSE)
dir_2012 <- data.frame(folder = dir_2012[grep("Fl", dir_2012)], stringsAsFactors = FALSE)
dir_2012$path <- paste(wd_2012, dir_2012$folder, sep = "/")

dir_2013 <- list.dirs(wd_2013, full.names = FALSE, recursive = FALSE)
dir_2013 <- data.frame(folder = dir_2013[grep("Fl", dir_2013)], stringsAsFactors = FALSE)
dir_2013$path <- paste(wd_2013, dir_2013$folder, sep = "/")

dir <- rbind(dir_2012, dir_2013)

camera_views <- list.dirs(dir$path[1], full.names = TRUE, recursive = FALSE)
for (i in 2:nrow(dir)){
  temp <- list.dirs(dir$path[i], full.names = TRUE, recursive = FALSE)
  camera_views <- append(camera_views, temp)
}

for (i in 1:length(camera_views)){
  print(i)
  files <- list.files(camera_views[i], full.names = FALSE, recursive = FALSE, pattern = "JPG|jpg")
  files <- data.frame(image_name = files, stringsAsFactors = FALSE)
  files$image_type <- "rgb_image"
  files$image_dir <- camera_views[i]
  
  images2DB <- rbind(images2DB, files)
}

# Process IR images 
# Set working folders
wd_2012 <- "//akc0ss-n086/NMML_POlar_Imagery/Surveys_IceSeals/BOSS/2012/Thermal_Frames"
wd_2013 <- "//akc0ss-n086/NMML_POlar_Imagery_2/Surveys_IceSeals_BOSS_2013/Thermal_Frames"

# Create list of camera folders within which data need to be processed 
dir_2012 <- list.dirs(wd_2012, full.names = FALSE, recursive = FALSE)
dir_2012 <- data.frame(folder = dir_2012[grep("Fl", dir_2012)], stringsAsFactors = FALSE)
dir_2012$path <- paste(wd_2012, dir_2012$folder, sep = "/")

dir_2013 <- list.dirs(wd_2013, full.names = FALSE, recursive = FALSE)
dir_2013 <- data.frame(folder = dir_2013[grep("Fl", dir_2013)], stringsAsFactors = FALSE)
dir_2013$path <- paste(wd_2013, dir_2013$folder, sep = "/")

dir <- rbind(dir_2012, dir_2013)

camera_views <- list.dirs(dir$path[1], full.names = TRUE, recursive = FALSE)
for (i in 2:nrow(dir)){
  temp <- list.dirs(dir$path[i], full.names = TRUE, recursive = FALSE)
  camera_views <- append(camera_views, temp)
}
camera_views <- camera_views[which(basename(camera_views) == "Port" | basename(camera_views) == "Starboard" | basename(camera_views) == "Center")]

for (i in 1:length(camera_views)) {
  print(i)
  files <- list.files(camera_views[i], full.names = FALSE, recursive = FALSE, pattern = "PNG|png|BMP|bmp")
  if(length(files) > 0) {
    files <- data.frame(image_name = files, stringsAsFactors = FALSE)
    files$image_type <- "ir_image"
    files$image_dir <- camera_views[i]
    
    images2DB <- rbind(images2DB, files)
  }
}

# Export data to PostgreSQL -----------------------------------------------------------
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              password = Sys.getenv("admin_pw"))

# Push data to database
RPostgreSQL::dbWriteTable(con, c("surv_boss", "tbl_images"), images2DB, overwrite = TRUE, row.names = FALSE)

RPostgreSQL::dbDisconnect(con)
