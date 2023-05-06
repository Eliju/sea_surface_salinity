{
  library(ncdf4)
  library(ncdf4.helpers)
  library(PCICt)
  library(readr)
  library(dplyr)
  library(tidyr)
  library(ggplot2)
  library(tools)
  library(arrow)
}

folder_path <- "data/dap.ceda.ac.uk/neodc/esacci/sea_surface_salinity/data/v03.21/7days/"
files <- list.files(path = folder_path, pattern="\\.nc", all.files=F,
           full.names=F, recursive = T)
pq_path <- "data/sea_surface_salinity"
first <- T
prev_year_path <- ""
for (f in files) {
  file_path <- f
  # year_path <- substr(file_path, 1, 4)
  # if (prev_year_path == "") {
  #   prev_year_path <- year_path
  # }

  climate_filepath <- paste0(folder_path, file_path)
  climate_output <- nc_open(climate_filepath)
  
  time <- ncvar_get(climate_output, varid = "time")
  sss <- ncvar_get(climate_output, varid = "sss")
  
  time_units <- climate_output$dim$time$units
  calendar <- climate_output$dim$time$calendar
  if ((time_units == "days since 1970-01-01 00:00:00 UTC") & (calendar == "standard")) {
    dt <- as.POSIXct(time*24*60*60, origin = "1970-01-01", tz="UTC")
    year_value <- format(as.Date(dt), "%Y")
    month_value <- format(as.Date(dt), "%m")

        sss_df <- as_tibble(sss, rownames = "lon")
    sss_one_day <- sss_df |> 
      pivot_longer(
        cols = starts_with("V"), 
        names_to = "lat", 
        values_to = "sss"
      ) |> 
      mutate(
        lat = parse_number(lat),
        time = dt,
        year = year_value,
        month = month_value
      )
    if (first) {
      sss_final = sss_one_day
      first <- F
      prev_month = month_value
      prev_year = year_value
    } else if ((prev_year == year_value) & (prev_month == month_value)) { 
      sss_final <- bind_rows(sss_final, sss_one_day) 
    } else {
      sss_final |> 
        group_by(year, month) |> 
        write_dataset(path = pq_path, format = "parquet")
      sss_final <- sss_one_day
      prev_year <- year_value
      prev_month <- month_value
    }
  } else {
    print("Different calendar settings")
  }
}
sss_final |> 
  group_by(year, month) |> 
  write_dataset(path = pq_path, format = "parquet")


