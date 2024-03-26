
library(dplyr)
library(openxlsx)
library(writexl)
library(readxl)
library(sf)
library(foreign)
library(rnaturalearth)
library(rnaturalearthdata)


# load all datasets for region with mines (depends on former analysis)
data_directories <- c(
  "/Users/nina/Documents/Scriptie/Coding/Output/final_impacted_streams_filtered_ar_scenario1_6_5_distance.dbf",
  "/Users/nina/Documents/Scriptie/Coding/Output/final_impacted_streams_filtered_sa_north_scenario1_6_5_distance.dbf",
  "/Users/nina/Documents/Scriptie/Coding/Output/final_impacted_streams_filtered_sa_south_scenario1_6_5_distance.dbf",
  "/Users/nina/Documents/Scriptie/Coding/Output/final_impacted_streams_filtered_af_scenario1_6_5_distance.dbf",
  "/Users/nina/Documents/Scriptie/Coding/Output/final_impacted_streams_filtered_as_scenario1_6_5_distance.dbf",
  "/Users/nina/Documents/Scriptie/Coding/Output/final_impacted_streams_filtered_au_scenario1_6_5_distance.dbf",
  "/Users/nina/Documents/Scriptie/Coding/Output/final_impacted_streams_filtered_eu_scenario1_6_5_distance.dbf",
  "/Users/nina/Documents/Scriptie/Coding/Output/final_impacted_streams_filtered_gr_scenario1_6_5_distance.dbf",
  "/Users/nina/Documents/Scriptie/Coding/Output/final_impacted_streams_filtered_na_scenario1_6_5_distance.dbf",
  "/Users/nina/Documents/Scriptie/Coding/Output/final_impacted_streams_filtered_si_scenario1_6_5_distance.dbf"
)

# load overlaying polygons (countries)  #for large i need to install a github package 
countries <- rnaturalearth::ne_countries(scale = 'medium', type = 'countries',
                                         returnclass = 'sf') %>%
  select(admin, adm0_a3)

#Prepare PA areas
polygons_PA_0 <- st_read("/Users/nina/Documents/Scriptie/Coding/KBA_PA_polygons/WDPA_Nov2023_Public_shp/WDPA_Nov2023_Public_shp_0/WDPA_Nov2023_Public_shp-polygons.shp")
polygons_PA_1 <- st_read("/Users/nina/Documents/Scriptie/Coding/KBA_PA_polygons/WDPA_Nov2023_Public_shp/WDPA_Nov2023_Public_shp_1/WDPA_Nov2023_Public_shp-polygons.shp")
polygons_PA_2 <- st_read("/Users/nina/Documents/Scriptie/Coding/KBA_PA_polygons/WDPA_Nov2023_Public_shp/WDPA_Nov2023_Public_shp_2/WDPA_Nov2023_Public_shp-polygons.shp")

polygons_PA <- bind_rows(polygons_PA_0, polygons_PA_1, polygons_PA_2)
rm(polygons_PA_0, polygons_PA_1, polygons_PA_2)

#KBA
polygons_KBA <- st_read("/Users/nina/Documents/Scriptie/Coding/KBA_PA_polygons/KBAsGlobal_2023_September_02_Criteria_TriggerSpecies/KBAsGlobal_2023_September_02_POL.shp")

#plot(st_geometry(countries))

#######################  FOR LOOP that divides data for the countries and adds up distance and volume  ############################################
# This 'for loop' loads all the datasets per region. This is a dataset is without lake segments
# and includes the volume and length. This for loop divides these impacted HYRIV_IDs into countries
# and calculates the sum of volume and distance impacted per country, but also per KBA and PA in these countries. 

for (data_dir in data_directories) {

data_impacted_streams <- read.dbf(data_dir)
# river data with geographical information

region_abbreviation <- gsub(".*filtered_(\\w+)_scenario.*", "\\1", data_dir)
# derive the region code from the path

# Construct the path for the rivers dataset
rivers_path <- sprintf("/Users/nina/Documents/Scriptie/Coding/HydroATLAS_data/RiverATLAS_Data_v10_shp/RiverATLAS_v10_shp/RiverATLAS_v10_%s.shp", (region_abbreviation))
rivers <- st_read(rivers_path) %>%
select(HYRIV_ID,LENGTH_KM, riv_tc_csu) 

# filter out lakes in Rivers dataset

path_filtered_rivers <- sprintf("/Users/nina/Documents/Scriptie/Coding/HydroATLAS_data/Filtered_RiverATLAS/RiverATLAS_%s_withoutlakes.dbf", (region_abbreviation))
RiverATLAS_without_lakes <- read.dbf(path_filtered_rivers)
rivers <- filter(rivers, HYRIV_ID %in% RiverATLAS_without_lakes$HYRIV_ID)

# convert to centroids to avoid a feature being assigned to 2 overlaying polygons
rivers_p <- st_centroid(rivers)

# check all features are maintained
nrow(rivers) == nrow(rivers_p)

# check crs, if not the same reproject using st_transform
st_crs(countries) == st_crs(rivers_p)

# get the sparse list of intersections
sf_use_s2(FALSE) # this needs to be set in order to avoid edge contamination errors
lst <- st_intersects(rivers_p,countries)

# check length list is the same as rivers
length(lst) == nrow(rivers)

# check that no river has more than one polygon ID assigned
sapply(lst,function(x) length(x) > 1) %>% sum

# check that all rivers have one polygon ID assigned,
# not all might depending on the coverage of the layer (e.g., coastal areas)
sapply(lst,length) %>% sum
# how many
sapply(lst,function(x) length(x) == 0) %>% sum
# percentage of length not covered
percent_missing <-(rivers$LENGTH_KM[sapply(lst,function(x) length(x) == 0)] %>% sum)/sum(rivers$LENGTH_KM)*100
print(percent_missing)
# 0.55% (can try to increase the resolution of countries to large to see whether this improves)

# create column with polygon ID
rivers$admin <- countries$admin[do.call('c',lapply(lst,function(x) ifelse(length(x) == 1, x, NA)))]
rivers$adm0_a3 <- countries$adm0_a3[do.call('c',lapply(lst,function(x) ifelse(length(x) == 1, x, NA)))]

rm(lst)

# fuse mines with volume data 
selected_streams <- data_impacted_streams %>% select (HYRIV_ID)
rivers_impacted_fused <- rivers %>%
  mutate(impacted = HYRIV_ID %in% selected_streams$HYRIV_ID) # Add 'impacted' column with TRUE for matching rows

# group data by country
grouped_rivers <- rivers_impacted_fused %>% group_by(admin)

################### intersect KBA + PA  ####################

# check crs, if not the same reproject using st_transform
st_crs(polygons_KBA) == st_crs(rivers)

#convert grouped_rivers to points again
grouped_rivers_p <- st_centroid(grouped_rivers)

sf_use_s2(FALSE) #avoid edge contamination errors
lst_KBA <- st_intersects(grouped_rivers_p, polygons_KBA)

# check that no river has more than one KBA polygon ID assigned
sapply(lst_KBA,function(x) length(x) > 1) %>% sum # 180 double polygon IDs!

# assign the KBA ID code to all rows that have 1 or more intersect 
# take word 'double' out of column for other regions
grouped_rivers$KBA_ID_doubles <- sapply(lst_KBA, function(x) {
  if (length(x) == 1) {
    polygons_KBA$SitRecID[x]
  } else if (length(x) > 1) {
    # If intersecting with multiple polygons, concatenate all SitRecID values
    paste(polygons_KBA$SitRecID[x], collapse = ",")
  } else {
    NA
  }
})

# check crs, if not the same reproject using st_transform
st_crs(polygons_PA) == st_crs(rivers)

# get the sparse list of intersections
sf_use_s2(FALSE) # this needs to be set in order to avoid edge contamination errors
lst_PA <- st_intersects(grouped_rivers_p, polygons_PA)

# assign the KBA ID code to all rows that have 1 or more intersect 
# take 'double' out of column for other regions
grouped_rivers$PA_ID_doubles <- sapply(lst_PA, function(x) {
  if (length(x) == 1) {
    polygons_KBA$SitRecID[x]
  } else if (length(x) > 1) {
    # If intersecting with multiple polygons, concatenate all SitRecID values
    paste(polygons_PA$SitRecID[x], collapse = ",")
  } else {
    NA
  }
})


# sum the distance and volume for the rivers impacted by mines located in a KBA/PA, in the entire country + sum total distance/volume
sum_impacted <- grouped_rivers %>%
  summarise(
    impacted_dist_KBA = sum(LENGTH_KM[impacted == TRUE & !is.na(KBA_ID_doubles)], na.rm = TRUE),
    impacted_vol_KBA = sum(riv_tc_csu[impacted == TRUE & !is.na(KBA_ID_doubles)], na.rm = TRUE),
    total_dist_KBA = sum(LENGTH_KM[!is.na(KBA_ID_doubles)], na.rm = TRUE),
    total_vol_KBA = sum(riv_tc_csu[!is.na(KBA_ID_doubles)], na.rm = TRUE),
    sum_country_impacted_km = sum(LENGTH_KM[impacted == TRUE], na.rm = TRUE),
    sum_country_impacted_vol = sum(riv_tc_csu[impacted == TRUE], na.rm = TRUE),
    total_dist_country = sum(LENGTH_KM, na.rm = TRUE),
    total_vol_country = sum(riv_tc_csu, na.rm = TRUE),
    impacted_dist_PA = sum(LENGTH_KM[impacted == TRUE & !is.na(PA_ID_doubles)], na.rm = TRUE),
    impacted_vol_PA = sum(riv_tc_csu[impacted == TRUE & !is.na(PA_ID_doubles)], na.rm = TRUE), 
    total_dist_PA = sum(LENGTH_KM[!is.na(PA_ID_doubles)], na.rm = TRUE),
    total_vol_PA = sum(riv_tc_csu[!is.na(PA_ID_doubles)], na.rm = TRUE)
  )

sum_impacted_df <- st_drop_geometry(sum_impacted) #easier to inspect

#write output
output_filename <- sprintf("/Users/nina/Documents/Scriptie/Coding/Output/%s_sum_country_scenario1_6_5km.xlsx", tolower(region_abbreviation))
write_xlsx(sum_impacted_df, output_filename)

#write_xlsx(sum_impacted_df, "/Users/nina/Documents/Scriptie/Coding/AR/AR_sum.xlsx")
rm(data_mines, rivers, rivers_fused, grouped_rivers, sum_impacted, sum_impacted_df, data_impacted_streams)
} # end of for loop 


