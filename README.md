# BOSS Ice Seal Survey Data Management

This repository stores the code associated with managing BOSS survey data. Data were originally stored in an Oracle DB and migrated to PostgreSQL in 2017. Much of the early data processing code is for reorganizing the data into the new structure/platform. 

Code numbered 0+ are intended to be run sequentially as the data are available for processing. Code numbered 99 are stored for longetivity, but are intended to only be run once to address a specific issue or run as needed, depending on the intent of the code.

The data management processing code is as follows:
* **BOSS_01_CreateDB.txt** - code for creating the PostgreSQL database structure into which data will be imported; code is to be run in PGAdmin
* **BOSS_02_ImportData.R** - code to import data from the prior Oracle DB into the PostgreSQL DB
* **BOSS_02_ImportDataFromPaul.R** - code to import data from Paul into the DB
* **BOSS_02_ImportImageInventory.R** - code to import image inventory into the DB
* **BOSS_02_ImportManualReview.R** - code to import manual review information into the DB
* **BOSS_02b_AssignManualReview2ImageInventory.txt** - code to assign manual review information into the image inventory (after both have been imported into the DB); code to be run in PGAdmin
* **BOSS_02b_AssignSurveyEffortField.txt** - code for assigning effort to survey data
* **BOSS_02b_CreateTracklineByEffort.txt** - code for creating views of trackline data by effort type
* **BOSS_03_QAQC.txt** - code to QA/QC BOSS sightings data
* **BOSS_04_RecreateFinalData.txt** - code for recreating the final dataset after migrating all the information to PostgreSQL; code to be run in PGAdmin
* **BOSS_05_ProcessDetections2DB.R** - code to import detection data (run to get bounding boxes for seals) into the DB
* **BOSS_06_ReconcileViameAndDB_rd#.R** - code from three rounds of data processing to align bounding boxes with seals in the DB

Other code in the repository includes:
* Code for comparing checksums across data in different locations (to evaluate if data are truly duplicates):
	* BOSS_99_CompareCheckSums_Polar2-3.R
* Code for preparing data for possible Kaggle competition:
	* BOSS_99_Data4Kaggle2012.R
	* BOSS_99_Data4Kaggle2013.R
* Code for machine learning processing to generate bounding boxes for all known seals:
	* BOSS_99_Data4MachineLearning_a_IdentifyTrainingImages.txt
	* BOSS_99_Data4MachineLearning_b_CopyImages.R
	* BOSS_99_Data4MachineLearning_c_ExportImageDetectionLists.R
* Code for getting sea ice concentration values at seal sighting locations:
	* BOSS_99_IceSealsWithSeaIce.txt