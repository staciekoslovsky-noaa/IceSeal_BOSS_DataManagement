# BOSS: Process Manually Reviewed Data to DB
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
install_pkg("tidyverse")

# Read data table ------------------------------------------------
manual_review <- read.table("//akc0ss-n086/NMML_Polar/Data/Survey_IceSeal_BOSS/2012/Power Analysis/DetectionRate_data.txt", 
                            col.names = c("image_num", "image_dt", "flight", "image_name", "seal_vis", "hotspot_id", "seal_count", "r73_2016", "notes"),
                            sep = "\t", skip = 1, header = FALSE, stringsAsFactors = FALSE, blank.lines.skip = TRUE, colClasses = "character", 
                            dec = ".", fill = TRUE, strip.white = TRUE) %>%
  mutate(reviewer = 'ELR')

# Export data to PostgreSQL --------------------------------------
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              password = Sys.getenv("admin_pw"))

# Push data to database
RPostgreSQL::dbWriteTable(con, c("surv_boss", "tbl_manual_review"), manual_review, overwrite = TRUE, row.names = FALSE)

RPostgreSQL::dbDisconnect(con)