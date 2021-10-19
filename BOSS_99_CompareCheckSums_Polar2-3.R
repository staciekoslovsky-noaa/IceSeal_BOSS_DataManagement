# Compare check sums for files on imagery 2 and 3
# S. Hardy, 20JUL2017

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
install_pkg("digest")

# Run code -------------------------------------------------------
polar2 <- list.files("//nmfs/akc-nmml/Polar_Imagery_2/Surveys_IceSeals_BOSS_2013/Thermal_Video/", full.names = TRUE, recursive = TRUE)
polar2 <- polar2[which(grepl("seq|SEQ", polar2))]
polar2 <- data.frame(files2 = polar2, stringsAsFactors = FALSE)
polar2$seq <- basename(polar2$files2)
polar3 <- list.files("//nmfs/akc-nmml/Polar_Imagery_3/", full.names = TRUE, recursive = TRUE)
polar3 <- polar3[which(grepl("seq|SEQ", polar3))]
polar3 <- data.frame(files3 = polar3, stringsAsFactors = FALSE)
polar3$seq <- basename(polar3$files3)

thermal <- merge(polar2, polar3, by = "seq")

for (i in 1:nrow(thermal)){
  thermal$ident[i] <- ifelse(identical(digest::digest(thermal$files2[i], algo="md5", serialize=F), 
                                       digest::digest(thermal$files3[i], algo="md5", serialize=F)),
                          "TRUE", "FALSE")
}