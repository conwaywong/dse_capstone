# MAS DSE Capstone Project

## Traffic

	* http://pems.dot.ca.gov/ - Data Source
		* http://www.dot.ca.gov/hq/tpp/offices/omsp/system_planning/documents/System_Planning_Training_Module_2_TCR_Guide_Working_Draft_2013_02_22.pdf
	* http://www.dot.ca.gov/index.shtml
	* http://www.dot.ca.gov/hq/tsip/gis/datalibrary/ - Caltrans GIS Data
	* http://traffic-counts.dot.ca.gov/ - Traffic counts
	* http://www.dot.ca.gov/cwwp

## Weather
### CA precipitation for 15-minute increments
#### Documentation

 ftp://ftp.ncdc.noaa.gov/pub/data/15min_precip-3260/dsi3260.pdf

 #### CA data

 [1971-2011](ftp://ftp.ncdc.noaa.gov/pub/data/15min_precip-3260/04/)  
 [2012](ftp://ftp.ncdc.noaa.gov/pub/data/15min_precip-3260/by_month2012)  
 [2013](ftp://ftp.ncdc.noaa.gov/pub/data/15min_precip-3260/by_month2014)  
 [2014](ftp://ftp.ncdc.noaa.gov/pub/data/15min_precip-3260/by_month2014)
 	* not sure why 2014 directory is listed but empty

 #### Notes

 Download data:  
 * `wget --passive-ftp -r ftp://ftp.ncdc.noaa.gov/pub/data/15min_precip-3260/04/*`
