# **Lab 2: MapReduce with PySpark**


## **Introduction**

In this lab, you will extend your understanding of big data processing by exploring MapReduce techniques in a distributed computing environment using PySpark. Building on Lab 1 concepts, this lab focuses on practical implementation of operations like `map`, `reduceByKey`, and `sortBy`. By the end of this lab, you will:
- Learn how PySpark processes data in parallel across distributed nodes.
- Understand the significance of partitioning in distributed systems.
- Implement and analyze distributed operations for real-world data processing tasks.


### **Learning Objectives**

1. Implement MapReduce-style word counting using PySpark transformations.
2. Perform grouping and aggregation on distributed data.
3. Apply distributed sorting on large datasets.
4. Analyze the performance impacts of partitioning in distributed systems.


### **Prerequisites**

Ensure the following are available before starting the lab:
1. **Python (3.7 or later)** and **PySpark installed**. To install PySpark, use:
   ```bash
   pip install pyspark
   ```
2. **Dataset files**:
   - `text_data.txt`: A text file containing multiple lines of text.
   - `group_data.txt`: A file with `(k, v)` pairs, where `k` is a key and `v` is a value.
   - `search_data.txt`: A file with random integers.

If these files are unavailable, refer to the dataset generation instructions provided in the exercises.


Download the Lab's Notebook at <a href="https://github.com/osekoo/hands-on-bigdata/blob/develop/Lab2_Student.ipynb">Lab2_Student.ipynb</a>

---

## **Exercise 1: Word Count**

### **Objective**

Count the occurrences of each word in a text file using PySpark. This exercise simulates a typical MapReduce task where the data is split, mapped, and reduced in parallel.

### **Instructions**

1. **Load the Dataset**:
   - Use the file `text_data.txt` containing lines of text. Each line may have multiple words separated by spaces.

2. **Transform the Data**:
   - Split each line into individual words.
   - Map each word to a key-value pair `(word, 1)`.

3. **Reduce and Aggregate**:
   - Use `reduceByKey` to sum up the occurrences of each word.

4. **Collect the Results**:
   - Collect the results into a list of `(word, count)` tuples.
   - Print the top 10 most frequent words for verification.


### **Key Takeaway**

You will learn how PySpark's transformations (`flatMap`, `map`, `reduceByKey`) enable efficient distributed word counting. This operation demonstrates parallelized MapReduce in action.

---

## **Exercise 2: Grouping and Aggregation**

### **Objective**

Group a dataset of `(k, v)` pairs by key and compute the sum of values for each key. This operation simulates the "reduce" phase of MapReduce.


### **Instructions**

1. **Load the Dataset**:
   - Use the file `group_data.txt` containing lines in the format `k,v`, where:
     - `k`: A random integer between 1 and 7 (key).
     - `v`: A random integer between 1 and 1,000 (value).

2. **Parse the Data**:
   - Split each line into a tuple `(k, v)`, converting `k` and `v` to integers.

3. **Aggregate by Key**:
   - Use `reduceByKey` to compute the sum of values for each key.

4. **Collect the Results**:
   - Collect the results into a list of `(key, total_value)` tuples.
   - Print all the results to verify correctness.


### **Key Takeaway**

This exercise demonstrates how PySpark's `reduceByKey` efficiently performs grouping and aggregation in a distributed system. You will also observe how parallel computation scales for large datasets.

---

## **Exercise 3: Distributed Sorting**

### **Objective**

Sort a dataset of integers in ascending order using PySpark's distributed transformations. This operation demonstrates the "shuffle-and-sort" phase of MapReduce.


### **Instructions**

1. **Generate the Dataset**:
   - If `search_data.txt` is unavailable, generate a file containing 100,000 random integers between 1 and 1,000,000. Save one integer per line.

2. **Load the Dataset**:
   - Use the file `search_data.txt`.

3. **Transform the Data**:
   - Parse each line into an integer.

4. **Sort the Data**:
   - Use `sortBy` to sort the integers in ascending order.

5. **Collect the Results**:
   - Collect the sorted integers and verify that the order is correct by printing the first 10 and last 10 numbers.


### **Key Takeaway**

You will observe how distributed sorting operates efficiently on large datasets using PySpark. This exercise highlights the scalability of sorting operations in distributed systems.

---

## **Exercise 4: Partitioning and Performance Analysis**

### **Objective**

Explore how partitioning affects the performance of PySpark operations. You will measure the time taken for transformations before and after repartitioning the data.

### **Instructions**

1. **Load the Dataset**:
   - Use the file `text_data.txt`.

2. **Repartition the Data**:
   - Change the number of partitions using the `repartition` function. Experiment with different partition sizes.

3. **Apply a Transformation**:
   - Perform a simple transformation (e.g., split lines into words).

4. **Measure Performance**:
   - Record the execution time of the transformation before and after repartitioning.
   - Compare the results to understand the impact of partitioning on performance.


### **Key Takeaway**

Partitioning can significantly affect the performance of distributed computations. This exercise will help you understand when and how to use repartitioning effectively in big data workflows.

---

## **Expected Deliverables**

1. **Code Implementations**:
   - Completed code for all exercises.
   - Ensure correctness and clarity of logic.

2. **Performance Logs**:
   - Record execution times for all operations.
   - Analyze performance changes with and without repartitioning in Exercise 4.

3. **Report**:
   - Discuss the observed performance differences.
   - Reflect on the importance of partitioning in distributed systems.

---

## **Conclusion**

This lab introduces you to practical MapReduce techniques using PySpark. By completing these exercises, you will gain hands-on experience with distributed data transformations, understand the role of partitioning, and appreciate the scalability of Spark for processing large datasets.
