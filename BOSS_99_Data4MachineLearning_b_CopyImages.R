# BOSS: Copy Images for ML Training
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


# Get data from DB
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              password = Sys.getenv("admin_pw"))

data <- RPostgreSQL::dbGetQuery(con, "SELECT image_dir || \'\\' || image_name AS image FROM surv_boss.tbl_images WHERE ml_imagestatus = \'training\'")

file.copy(data$image, "D:/BOSS_Color_Training")
file.copy(data$image, "E:/BOSS_Color_Training")

RPostgreSQL::dbDisconnect(con)
 