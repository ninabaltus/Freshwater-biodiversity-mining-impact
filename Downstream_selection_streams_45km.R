
library(ggplot2)
library(dplyr)
library(openxlsx)
library(foreach)
library(data.table)
library(writexl)
library(readxl)

# dbf file including river reaches and intersected mines, including data on primary commodity and activity 
data_directories <- c(
  "/Users/nina/Documents/Scriptie/Coding/HydroATLAS_data/RiverATLAS_Data_v10_shp/RiverATLAS_v10_shp/RiverATLAS_v10_ar.dbf",
  "/Users/nina/Documents/Scriptie/Coding/HydroATLAS_data/RiverATLAS_Data_v10_shp/RiverATLAS_v10_shp/RiverATLAS_v10_sa_north.dbf",
  "/Users/nina/Documents/Scriptie/Coding/HydroATLAS_data/RiverATLAS_Data_v10_shp/RiverATLAS_v10_shp/RiverATLAS_v10_sa_south.dbf",
  "/Users/nina/Documents/Scriptie/Coding/HydroATLAS_data/RiverATLAS_Data_v10_shp/RiverATLAS_v10_shp/RiverATLAS_v10_af.dbf",
  "/Users/nina/Documents/Scriptie/Coding/HydroATLAS_data/RiverATLAS_Data_v10_shp/RiverATLAS_v10_shp/RiverATLAS_v10_as.dbf",
  "/Users/nina/Documents/Scriptie/Coding/HydroATLAS_data/RiverATLAS_Data_v10_shp/RiverATLAS_v10_shp/RiverATLAS_v10_au.dbf",
  "/Users/nina/Documents/Scriptie/Coding/HydroATLAS_data/RiverATLAS_Data_v10_shp/RiverATLAS_v10_shp/RiverATLAS_v10_eu.dbf",
  "/Users/nina/Documents/Scriptie/Coding/HydroATLAS_data/RiverATLAS_Data_v10_shp/RiverATLAS_v10_shp/RiverATLAS_v10_gr.dbf",
  "/Users/nina/Documents/Scriptie/Coding/HydroATLAS_data/RiverATLAS_Data_v10_shp/RiverATLAS_v10_shp/RiverATLAS_v10_na.dbf",
  "/Users/nina/Documents/Scriptie/Coding/HydroATLAS_data/RiverATLAS_Data_v10_shp/RiverATLAS_v10_shp/RiverATLAS_v10_si.dbf"
)

# data retrieved from https://www.hydrosheds.org/hydroatlas
LakeATLAS_west <- read.dbf("/Users/nina/Documents/Scriptie/Coding/LakeATLAS_v10_pnt_west.dbf")
LakeATLAS_east <- read.dbf("/Users/nina/Documents/Scriptie/Coding/LakeATLAS_v10_pnt_east.dbf")
LakeATLAS_fused <- rbind(LakeATLAS_east, LakeATLAS_west)
rm(LakeATLAS_west, LakeATLAS_east)

# aggregate HYRIV_RCH column
aggregated_lake_data <- LakeATLAS_fused %>%
  group_by(HYRIV_RCH) %>%
  summarise(count = n())

# load excel list for critical materials from OECD
critical_materials <- read_xlsx("/Users/nina/Documents/Scriptie/Coding/Critical_minerals.xlsx")

# for loop that findssegments up to 45km  downstream from the mine start location 

for (data_dir in data_directories) {
  
hb_data <- read.dbf(data_dir) %>%
  select("HYRIV_ID", "NEXT_DOWN", "LENGTH_KM", "riv_tc_csu")

region_abbreviation <- gsub(".*/RiverATLAS_v10_(\\w+)\\.dbf", "\\1", data_dir) # derive the region code from the path

# Construct the path for the rivers dataset
intersect_data_path <- sprintf("/Users/nina/Documents/Scriptie/Coding/intersects/%s_intersect_10km.dbf", tolower(region_abbreviation))
intersect_data <- read.dbf(intersect_data_path) %>%
  select("HYRIV_ID", "PRIMARY_CO", "ACTV_STATU")

# Use grepl() function to check if a string contains "a certain set of characters".
# Use         paste(critical_materials$Critical, collapse = "|")       to use grepl() function on more than one "a certain set of characters" 
intersect_data$criticality  <- ifelse(grepl(paste(critical_materials$Critical, collapse = "|"), intersect_data$PRIMARY_CO), "1","2")

# Create a new column 'status' based on the 'ACTV_STATU' field and only assign TRUE when 'Active'
intersect_data$status <- intersect_data$ACTV_STATU == "Active"

length_threshold <- 45 #set here the length threshold in KM

#mine_streams <- (intersect_data$HYRIV_ID)

#crtical active: critical (1) and TRUE
# critical inactive: critical (1) and FALSE 
# active: TRUE
# inactive: FALSE
# all mines: use every mine as starting location

tracked_list <- foreach(id = intersect_data$HYRIV_ID[intersect_data$criticality == 1 & intersect_data$status == FALSE]) %do% {#change to either FALSE or TRUE, now 2 
  start_row <- which(hb_data$HYRIV_ID == id)
  
  # Check if start_row is empty
  if (length(start_row) == 0) {
    return(NULL)  # Skip to the next iteration if no match is found
  }
  
  # get the HYRIV_ID, NEXT_DOWN, and LENGTH_KM values for the current row
  current_hyriv_id <- hb_data$HYRIV_ID[start_row]
  next_down <- hb_data$NEXT_DOWN[start_row]
  length_km <- hb_data$LENGTH_KM[start_row]
  total_length <- length_km  
  
  res <- data.frame(HYRIV_ID = current_hyriv_id, LENGTH_KM = length_km)
  
  # follow the NEXT_DOWN chain to add all corresponding HYRIV_ID and LENGTH_KM values
  while (next_down != 0 ) {
    next_row <- which(hb_data$HYRIV_ID == next_down)
    
    next_hyriv_id <- hb_data$HYRIV_ID[next_row]
    next_length_km <- hb_data$LENGTH_KM[next_row]
    
    if ((total_length + next_length_km) <= length_threshold) {
      total_length <- total_length + next_length_km
      res <- rbind(res, data.frame(HYRIV_ID = next_hyriv_id, LENGTH_KM = next_length_km))
    } else {
      # The next segment would exceed the threshold
      break
    }
    
    next_down <- hb_data$NEXT_DOWN[next_row]
  }
  return(res)
}
###### post processing #######

# Combine the list of dataframes into a single dataframe
processed_data1 <- do.call(rbind,tracked_list)

# take out duplicates
processed_data_unique <- unique(processed_data1)

# taking out lake ids!
path_filtered_rivers <- sprintf("/Users/nina/Documents/Scriptie/Coding/HydroATLAS_data/Filtered_RiverATLAS/RiverATLAS_%s_withoutlakes.dbf", tolower(region_abbreviation))

RiverATLAS_without_lakes <- read.dbf(path_filtered_rivers) %>%
  select(HYRIV_ID)

filtered_dataset <- merge(RiverATLAS_without_lakes, processed_data_unique, by="HYRIV_ID") # includes all segments that are impacted by mines in AR 

path_final_results <- sprintf("/Users/nina/Documents/Scriptie/Coding/Output/final_impacted_streams_filtered_%s_scenario2_6_5_distance.dbf", tolower(region_abbreviation))

write.dbf(filtered_dataset, path_final_results)

} # end of for loop 

