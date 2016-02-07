org.ucsd.dse.capstone.traffic.DefaultSparkTemplate
	An instance of SparkTemplate that relies on the command line spark-submit. See PCAMain, PivotMain for example usage.

org.ucsd.dse.capstone.traffic.Executor
	Trait defining signature for executors that will perform work in Spark using the specified SparkContext and SQLContext

org.ucsd.dse.capstone.traffic.IOUtils
	IO Utilities that deserializes RDD[Row] in compressed text format to RDD[Vector] for use in PCA

org.ucsd.dse.capstone.traffic.MLibUtils
	Utility functions wrapping Spark MLib APIs and executes PCA

org.ucsd.dse.capstone.traffic.PCAExecutor.*
	Defines the classes and logic that executes PCA against a specified compressed RDD[Row]. The result of the PCA is stored as CSV files in the location specified by S3Parameter and OutputParameter.

org.ucsd.dse.capstone.traffic.PCAMain
	Driver that executes PCA against a compressed RDD[Row]

org.ucsd.dse.capstone.traffic.PivotExecutor
	Class that pivots an RDD[String] consisting of rows of individual traffic readings to columns of traffic readings. The resulting RDD[Row] consists of: 
		288 total flow 5m readings as columns
		288 occupancy 5m readings as columns
		288 speed 5m readings as columns
	Each row represents the readings for a traffic station in a given day.

org.ucsd.dse.capstone.traffic.PivotMain
	Driver that executes the pivot of traffic data from RDD[String] to RDD[Row] and serializes it to compressed text.

org.ucsd.dse.capstone.traffic.ReadPivotMain
	Driver that deserializes RDD[Row] from compressed text files into a Spark SQL DataFrame.

org.ucsd.dse.capstone.traffic.SparkTemplate
	Defines the helper class, main drivers will utilize to execute work using the SparkContext. Instances of SparkTemplate are responsible to manage the lifecycle for the SparkContext.

org.ucsd.dse.capstone.traffic.StandaloneSparkTemplate
	An instance of SparkTemplate that submits the Spark Application programatically (without using spark-submit command line)