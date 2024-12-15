# **Lab 1: Introduction to Big Data Processing**

## **Introduction**

In this lab, you will take the first steps toward big data processing, where the principles of scalability, efficient algorithms, and distributed computing play crucial roles. The exercises are designed to provide a foundational understanding of how to handle large datasets stored on disk.

These tasks mimic real-world challenges encountered in systems like MapReduce and frameworks like Apache Spark.

The key focus will be on:
1. Processing data that exceeds memory limits by sequentially scanning files stored on disk.
2. Applying partitioning to optimize operations and minimize unnecessary computation.
3. Understanding the relationship between algorithm design and scalability.
4. Exploring techniques that are foundational for distributed systems, such as:
   - Partitioning for parallelism.
   - Sorting and merging large datasets.
   - Aggregating data efficiently.

These exercises will help you appreciate the power of tools like MapReduce, Spark, and Hadoop while also providing a clear understanding of their underlying principles.

Download the Lab's Notebook at <a href="https://github.com/osekoo/hands-on-bigdata/blob/develop/Lab1_Student.ipynb">Lab1_Student.ipynb</a>

---

## **Exercise 1: Searching via Partitioning**

### **Objective**

Efficiently search for integers in a large dataset using both naive and optimized approaches, laying the groundwork for distributed data searching.

### **Instructions**

1. **Dataset Generation**:
   - Sequentially generate and save into a file named `search_data.txt` a collection of 900,000 random integers between 1 and 1,000,000.
   - Store one integer per line.

2. **Linear Search**:
   - Implement a naive function that sequentially scans the file for 5 user-provided integers.
   - Output `YES` if the integer exists in the dataset or `NO` otherwise.

3. **Partitioned Search**:
   - Partition the dataset into `m = 1,000` smaller files based on a hash function (e.g., `H(n) = n % m `).
   - Implement a partitioned search that reads only the relevant partition file for each user-provided integer.

4. **Compare Performance**:
   - Compare the performance of the linear search and partitioned search methods.

### **Key Takeaway**

Understand how partitioning reduces the search space and mimics the approach used in distributed file systems.

---

## **Exercise 2: Grouping and Aggregation**

### **Objective**

Perform grouping and aggregation on large datasets stored on disk, simulating operations like those in MapReduce.

### **Instructions**

1. **Dataset Generation**:
   - Sequentially generate and save into a file named `group_data.txt` a collection of 30,000,000 `(k, v)` pairs, where:
     - `k` is a random integer between 1 and 7.
     - `v` is a random integer between 1 and 1,000.
   - With each line formatted as `k, v`.

2. **Naive Grouping**:
   - Implement a function that reads the file sequentially and computes the sum of `v` values for each `k` without advanced data structures.
   - Output the aggregated result as a list of tuples (k, $\sum_{(k,v)} v$).

3. **Partitioned Grouping**:
   - Partition the dataset into `m = 10` smaller files using a hash function (e.g., `H(k) = k \% m`).
   - Implement a partitioned grouping function that processes each partition file sequentially.
   - Combine results from all partitions into a single output.

4. **Performance Analysis**:
   - Compare the naive and partitioned approaches, discussing how partitioning simulates a distributed "reduce" operation in MapReduce.

### **Key Takeaway**

Learn how partitioning allows parallel processing and reduces memory usage, mimicking the "reduce" step in distributed frameworks.

---

## **Exercise 2bis: Sorting and Grouping by Key**

### **Objective**

Perform grouping and aggregation on a sorted dataset by key, simulating how sorting helps optimize grouping operations in big data systems.


### **Instructions**

1. **Dataset Generation**:
   - Generate a dataset of  a collection of 30,000,000 `(k, v)` pairs, where:
     - `k`: A controlled integer between 1 and 7.
     - `v`: A random integer between 1 and 1,000.
   - Save the dataset in a **sorted order by `k`** to a file named `group_sorted_data.txt`.

2. **Iterator-based File Access**:
   - Use Python iterators to simulate streaming access to the dataset file, hiding direct file handling from the processing logic.
   - Wrap the dataset file in an iterator to process it line by line, simulating streaming access.
   - This hides the complexity of file handling and ensures memory efficiency, especially for large datasets.

3. **Grouping via Iteration**:
   - Implement a function to iterate through the sorted dataset, grouping values by `k `as they appear. This avoids random access and simulates how sorting simplifies grouping in distributed systems.
   - Since the file is sorted by `k`, you can group values without random access:
     1. Read the file line by line.
     2. Accumulate values for the current key `k`.
     3. When `k `changes, save the results for the previous key and start accumulating for the new key.

4. **Comparison**:
   - Compare the performance of the iterator-based grouping on the sorted file against the naive grouping on an unsorted dataset.

### **Key Takeaways**
1. Sorting the dataset by `k `beforehand simplifies grouping, reducing memory usage and computation overhead.
2. Iterators enable efficient file processing without loading the entire dataset into memory.
3. Skewed distributions for `k `simulate real-world scenarios where data isn't uniformly distributed, offering better insights into optimization techniques.

---

## **Exercise 3: n-Way Merge-Sort**

### **Objective**

Merge multiple sorted datasets into a single sorted output using techniques foundational to shuffle-and-sort operations in MapReduce.

### **Instructions**

1. **Dataset Generation**:
   - Generate `n = 3 `sorted lists of random integers:
     - List 1: 10 integers between 1 and 100.
     - List 2: 10 integers between 50 and 150.
     - List 3: 10 integers between 100 and 200.
   - Save each sorted list to a separate file (e.g., `list_0.txt`, `list_1.txt`, `list_2.txt`).

2. **n-Way Merge-Sort**:
   - Implement a function that reads each sorted file sequentially and merges the lists into a single sorted output file, `sorted_output.txt`.
   - Use a linear scan of the files to pick the smallest element across all current file pointers.

3. **Performance Analysis**:
   - Estimate the time complexity of the algorithm.
   - Discuss how the algorithm forms the backbone of distributed sorting in tools like Hadoop and Spark.

### **Key Takeaway**

Understand how merge-sort optimizes disk-based sorting and scales for distributed systems.

---

## **Exercise 4: Word Counting**

### **Objective**

Count word occurrences in a large text dataset, simulating a MapReduce-style word count operation.

### **Instructions**

1. **Dataset Preparation**:
   - Provide a large text file, `text_data.txt`, where each line contains multiple words.

2. **Sequential Word Count**:
   - Implement a naive function that reads the file line by line and counts the occurrences of each word.
   - Store the results as a list of tuples $(\text{word}, \text{count})$.

3. **Partitioned Word Count**:
   - Partition the text file into smaller files based on a hash function (e.g., $H(\text{word}) = \sum \text{ord}(c) \text{ for } c \text{ in } \text{word} \% m$).
   - Implement a function to process each partition sequentially and combine the results.


### **Key Takeaway**

Understand the "map" and "reduce" steps in distributed word counting.

---

## **Expected Deliverables**

1. **Python Implementations**:
   - Provide Python scripts for all exercises.

2. **Performance Logs**:
   - Include execution logs showing performance measurements (time taken) for naive and optimized approaches.

3. **Brief Report**:
   - Discuss the observed performance differences between naive and optimized algorithms.
   - Highlight the importance of partitioning and sequential file scanning in big data processing.
   - Relate these concepts to frameworks like MapReduce and Spark.

---

## **Conclusion**

This lab introduces foundational techniques for handling large datasets efficiently, focusing on scalability and partitioning. By completing these exercises, you have learned how to:
- Reduce computational overhead through partitioning.
- Simulate MapReduce operations like grouping, sorting, and counting.
- Understand the principles behind distributed frameworks like Hadoop and Spark.

These concepts are essential stepping stones to mastering big data frameworks, and the skills gained here will prepare you for practical applications in distributed data processing.
