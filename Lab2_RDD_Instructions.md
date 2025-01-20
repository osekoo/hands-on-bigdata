# **Lab Session: Algorithms and Programming with Spark RDDs in Colab**  

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
   - Tokenize the lines into words:
     ```python
     words = document.flatMap(lambda line: line.split())
     ```
   - Map each word to a key-value pair:
     ```python
     word_pairs = words.map(lambda word: (word, 1))
     ```
   - Reduce by key to count occurrences:
     ```python
     word_counts = word_pairs.reduceByKey(lambda x, y: x + y)
     ```

4. **View Results**:
   - Display the word counts:
     ```python
     print(word_counts.collect())
     ```


## **Exercise 2: Data Aggregation**
**Objective**: Compute the average quantity of each pet from a dataset and analyze shuffle operations.

1. **Setup**:  
   - Create an RDD for the dataset. For example:
     ```python
     data = [("dog", 3), ("cat", 4), ("dog", 5), ("cat", 6)]
     pets = sc.parallelize(data)
     ```

2. **Aggregate Data**:
   - Calculate the total quantity and count for each pet:
     ```python
     totals = pets.mapValues(lambda qty: (qty, 1)).reduceByKey(
         lambda x, y: (x[0] + y[0], x[1] + y[1])
     )
     ```
   - Compute the average:
     ```python
     averages = totals.mapValues(lambda x: x[0] / x[1])
     ```

3. **Optimize Shuffle**:
   - Discuss with the class how to reduce shuffle operations, e.g., using `combineByKey`.

4. **View Results**:
   - Print the averages:
     ```python
     print(averages.collect())
     ```

---

## **Exercise 3: Join Operations**
**Objective**: Perform equi-joins and right-outer joins without the `join()` transformation.

1. **Setup**:  
   - Use two RDDs representing key-value datasets:
     ```python
     rdd1 = sc.parallelize([("A", 1), ("B", 2), ("C", 3)])
     rdd2 = sc.parallelize([("A", 4), ("B", 5), ("D", 6)])
     ```

2. **Equi-Join Implementation**:
   - Perform a cartesian product and filter:
     ```python
     equi_join = rdd1.cartesian(rdd2).filter(lambda x: x[0][0] == x[1][0]).map(
         lambda x: (x[0][0], (x[0][1], x[1][1]))
     )
     ```

3. **Right-Outer Join**:
   - Extend the equi-join logic to include keys exclusive to `rdd2`.

4. **Discuss Results**:
   - Compare performance with the standard `join()` transformation.

---

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
   - Filter customers by month:
     ```python
     from datetime import datetime
     july_customers = customer_rdd.filter(
         lambda x: datetime.strptime(x[1], "%Y-%m-%d").month == 7
     ).map(lambda x: x[2])
     print(july_customers.collect())
     ```

3. **Query 2: Distinct Names**:
   - Use `distinct()`:
     ```python
     distinct_names = july_customers.distinct()
     print(distinct_names.collect())
     ```

4. **Query 3: Aggregated Orders**:
   - Perform grouping and aggregation:
     ```python
     grouped_orders = order_rdd.map(lambda x: (x[0], float(x[1]))).groupByKey()
     aggregated = grouped_orders.mapValues(
         lambda x: (sum(x), len(set(x)))
     )
     print(aggregated.collect())
     ```

5. **Query 4: Join Customers and Orders**:
   - Use a key-based join:
     ```python
     join_result = customer_rdd.map(lambda x: (x[0], x[2])).join(order_rdd.map(lambda x: (x[0], x[1])))
     print(join_result.collect())
     ```
