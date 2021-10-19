# Import BOSS data from Paul
# 9 January 2020, S. Hardy

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
# Process frame lists + associated metadata
### All BOSS frames
load("C:/Users/stacie.hardy/Work/Work/Projects/AS_BOSS/Data/BOSS_effort_calcs_FromPaul_20200106/boss_geo_sp.rda")
boss_allFrames <- data.frame(boss_geo)
rm(boss_geo)
boss_allFrames$flightid <- as.character(boss_allFrames$flightid)
boss_allFrames$side <- as.character(boss_allFrames$side)
boss_allFrames$img_name <- as.character(boss_allFrames$img_name)
boss_allFrames$effort <- as.character(boss_allFrames$effort)
boss_allFrames$id <- as.integer(row.names(boss_allFrames))
boss_allFrames <- boss_allFrames[, c(10, 1:9)]

### Only frames used in analysis
load("C:/Users/stacie.hardy/Work/Work/Projects/AS_BOSS/Data/BOSS_effort_calcs_FromPaul_20200106/boss_geo_sp_tmp5.Rda")
boss_subFrames <- data.frame(boss_geo)
rm(boss_geo)

### Update image data before import
boss_allFrames$use_in_analysis <- ifelse(boss_allFrames$img_name %in% boss_subFrames$img_name == TRUE, "Yes", "No")

# Import data into DB
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              rstudioapi::askForPassword(paste("Enter your DB password for user account: ", Sys.getenv("pep_admin"), sep = "")))

RPostgreSQL::dbWriteTable(con, c("surv_boss", "tbl_images"), boss_allFrames, append = FALSE, overwrite = TRUE, row.names = FALSE)
RPostgreSQL::dbSendQuery(con, "ALTER TABLE surv_boss.tbl_images ADD COLUMN geom geometry(POINT, 4326)")
RPostgreSQL::dbSendQuery(con, "UPDATE surv_boss.tbl_images SET geom = ST_SetSRID(ST_MakePoint(interp_lon, interp_lat), 4326)")