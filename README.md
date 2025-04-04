# Distributed Graph Coloring using PySpark

This project implements a distributed graph coloring algorithm using **Apache Spark** with **PySpark**. Graph coloring is a classic problem in graph theory where each vertex of a graph is assigned a color such that no two adjacent vertices share the same color.

By leveraging the power of distributed computing with PySpark, this implementation can handle large-scale graphs that wouldn't fit in memory on a single machine.

---

## 📌 Features

- Distributed graph coloring using PySpark RDDs
- Supports input graphs in edge list format
- Greedy coloring heuristic for performance and simplicity
- Works on undirected graphs
- Handles large graphs

---

## 🛠️ Technologies Used

- Python 3.12.2
- Apache Spark 3.5.4
- PySpark (Spark's Python API)

---

## 📁 Project Structure

### graph_node.py
- Defines the node used in the graph colouring algorithm
- Defines a function to generate a random undirected graph

### graph_coloring.py
- Defines functions to serialize and deserialize graphs in json format
- Defines the function for graph_coloring

### main.py
- Allows CLI input to be in form of .json file, or a new random graph with given specifications
- Measurs execution time of the graph coloring algorithm

