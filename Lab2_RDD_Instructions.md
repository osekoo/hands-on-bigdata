# **Lab Session: Algorithms and Programming with Spark RDDs using PySpark**  

## Introduction
This lab session introduces you to foundational concepts of distributed data processing using Spark's Resilient Distributed Datasets (RDDs). You'll leverage Python and the PySpark library within the Colab environment to build, execute, and analyze various Spark programs. The session covers:

- **Setting up PySpark in Colab**: Learn to configure and initialize the SparkContext.
- **Practical Exercises**:
  1. **Word Count Problem**: Implement a Spark algorithm to analyze word frequencies in a text file.
  2. **Data Aggregation**: Compute the average quantities from a sample dataset while minimizing shuffle operations.
  3. **Join Operations**: Explore algorithms to perform equi-joins and right-outer joins on RDDs without the direct `join()` transformation.
  4. **SQL Query Encoding in Spark**: Encode and test SQL-like queries using Python MapReduce transformations.

Each exercise is designed to deepen your understanding of Spark's capabilities, RDD transformations, and actions. You'll also develop skills in optimizing performance and implementing complex operations.

**Tools Required**:
- **Python**: For Spark programming.
- **Colab Notebook**: To run your Spark scripts.

By the end of this session, you'll have hands-on experience in using Spark for solving real-world problems effectively.

## **Exercise 1: Word Count Problem**
**Objective**: Design and implement a Spark algorithm to compute word frequencies in a text file.

1. **Setup**:  
   - Ensure your Colab environment is ready with Spark installed. Run the following commands:
     ```python
     !pip install pyspark
     from pyspark import SparkConf, SparkContext
     sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
     ```
   - Download or use an existing text file (e.g., `shake.txt`).
     ```bash
     !wget https://www.dropbox.com/s/7ae58iydjloajvt/shake.txt
     ```

2. **Create the RDD**:
   - Load the text file into an RDD:
     ```python
     document = sc.textFile("shake.txt")
     ```

3. **Transform and Process**:
   - Tokenize the lines into words
   - Map each word to a key-value pair
   - Reduce by key to count occurrence

4. **View Results**:
   - Display the word counts


## **Exercise 2: Data Aggregation**
**Objective**: Compute the average quantity of each pet from a dataset and analyze shuffle operations.

1. **Setup**:  
   - Create an RDD for the dataset. For example

2. **Aggregate Data**:
   - Calculate the total quantity and count for each pet
   - Compute the average

3. **Optimize Shuffle**:
   - How to reduce shuffle operations?.

4. **View Results**:
   - Print the averages.

## **Exercise 3: Join Operations**
**Objective**: Perform equi-joins and right-outer joins without the `join()` transformation.

1. **Setup**:  
   - Use two RDDs representing key-value datasets:
     ```python
     rdd1 = sc.parallelize([("A", 1), ("B", 2), ("C", 3)])
     rdd2 = sc.parallelize([("A", 4), ("B", 5), ("D", 6)])
     ```

2. **Equi-Join Implementation**:
   - Perform a cartesian product and filter.

3. **Right-Outer Join**:
   - Extend the equi-join logic to include keys exclusive to `rdd2`.

4. **Discuss Results**:
   - Compare performance with the standard `join()` transformation.


## **Exercise 4: Encoding SQL Queries in Spark**
**Objective**: Encode SQL-like queries using Python MapReduce and test them.

1. **Setup**:  
   - Download the files:
     ```bash
     !wget https://www.dropbox.com/s/tmt6u80mkrwfjkv/Customer.txt
     !wget https://www.dropbox.com/s/8n5cbmufqhzs4r3/Order.txt
     ```
   - Load data into RDDs:
     ```python
     customer_rdd = sc.textFile("Customer.txt").map(lambda line: line.split(","))
     order_rdd = sc.textFile("Order.txt").map(lambda line: line.split(","))
     ```

2. **Query 1: Customers with Orders in July**:
   - Filter customers by month.

3. **Query 2: Distinct Names**:
   - Use `distinct()`.

4. **Query 3: Aggregated Orders**:
   - Perform grouping and aggregation.

5. **Query 4: Join Customers and Orders**:
   - Use a key-based join.
