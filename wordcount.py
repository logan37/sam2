import shutil
import os
import logging
from pyspark import SparkContext

# Set JAVA_HOME environment variable
# os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk1.8.0_441'

# Set HADOOP_HOME environment variable
# os.environ['HADOOP_HOME'] = r'D:\Workspace\Repos\winutils\hadoop-3.3.5'

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize a SparkContext
sc = SparkContext("local", "WordCount")

output_dir = r"D:\Workspace\Data\sam2\README_count"

try:
    # Remove the output directory if it exists
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
        logger.info(f"Removed existing output directory: {output_dir}")

    # Read the input text file into an RDD
    text_file = sc.textFile(r"D:\Workspace\sam2\README.md")
    logger.info("Read input text file into RDD")

    # Perform the word count
    counts = (text_file
              .flatMap(lambda line: line.split(" "))  # Split each line into words
              .map(lambda word: (word, 1))            # Map each word to a (word, 1) pair
              .reduceByKey(lambda a, b: a + b))       # Reduce by key (word), summing the counts
    logger.info("Performed word count")

    # Save the word count results to an output file
    counts.saveAsTextFile(output_dir)
    logger.info(f"Saved word count results to: {output_dir}")


    # Read the word count results back into an RDD
    result_rdd = sc.textFile(output_dir)
    logger.info("Read word count results back into RDD")

    # Collect and print the results
    results = result_rdd.collect()
    for result in results:
        print(result)


except Exception as e:
    logger.error(f"An error occurred: {e}", exc_info=True)

finally:
    # Stop the SparkContext
    sc.stop()
    logger.info("Stopped SparkContext")