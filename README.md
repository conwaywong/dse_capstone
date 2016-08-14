DSE Cohort 1 - Traffic Capstone Project
====

Traffic modeling and prediction is a field that has been researched and studied for many years. With the 
introduction of large data sets taken from sensor stations throughout California, and a myriad of other data sources 
available contributing to traffic metrics collection, opportunities for analysis into traffic modeling and the factors 
causing traffic are ever expanding. 
 
In this Capstone overview, we describe how traffic volume can be modeled utilizing Principal Component Analysis 
(PCA), additionally how easily and effectively it can be visualized to assist in the detection of traffic patterns and 
volume for further analysis. In addition, our work experiments with identifying different traffic behaviors exhibited 
throughout the week utilizing the KMeans++ clustering algorithm. A cursory examination into determining the 
factors that contribute to traffic volume is explored using Elastic Net Regression. The purpose of this work is to 
inform business decision makers and the general public of traffic patterns that exist in California, with the hope 
being that additional insight can guide solutions to address high traffic volume. 

Reports
---------
| Location      | Description   
| ------------- | -------------  
| final/doc/FinalReport.pdf | Final Report
| final/doc/FinalPresentation.pdf | Final Presentation      

Buckets
---------
| Location      | Description   
| ------------- | -------------  
| s3://dse-team2-2014/dse_traffic | Directory containing original downloaded traffic files
| s3://dse-team2-2014/pivot_output_#{year} | Directory containing Pivot Output Files from parsing downloaded traffic files      
| s3://dse-team2-2014/regression | Directory containing files used for Elastic Net Regression      

Source Code
------------
| Location      | Description   
| ------------- | -------------  
| ml/scala/traffic | Maven Project for Machine Learning, Pivoting Scala Code
| traffic/vis/stations | Traffic GIS Map Visualization - HTML/Javascript Source Code      
| final/eigenvector_analysis.py | Eigenvector Analysis Visualiation - Bokeh Python Source Code
| traffic/src/scraper.py | Web Scraper Python Source Code
| traffic/jsg_test | Directory Containing CloverETL graphs, transformers for Postgres DB ETL
| traffic/traffic_etl_spark.py | PySpark Job to extract traffic data from S3 into Postrgres DB

S
