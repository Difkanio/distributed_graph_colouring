from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
)
from pprint import pprint
import json

def get_max_degree(rdd):
    '''returns the maximum degree of the nodes in the graph'''
    return rdd.map(lambda x: len(x[2])).max()

def calculate_average_degree(rdd):
    '''returns the average degree of the nodes in the graph'''
    return rdd.map(lambda x: len(x[2])).mean()
    
def load_json_into_rdd(file_path):
    spark = SparkSession.builder \
    .appName("json to rdd parsing") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("color", IntegerType(), True),
            StructField("neighbors", StringType(), True),
        ]
    )
    df = spark.read.json(file_path, multiLine=True, schema=schema)
    rdd = df.rdd

    parsed_rdd = rdd.map(
        lambda x: (
            x[0], #id
            x[1], #color
            [int(i) for i in x[2].split(",") if i], #neighbors
        )
    )
    return parsed_rdd

def rdd_to_json(output_path, rdd):
    '''saves graph from rdd into json file'''
    # Convert the RDD into a JSON-serializable format
    # convert negihbors list into string
    json_rdd = rdd.map(
        lambda x: {
            "id": x[0],
            "color": x[1],
            "neighbors": ",".join([str(neighbor) for neighbor in x[2]]),
        }
    )

    with open(output_path, "w") as f:
        json.dump(json_rdd.collect(), f, indent=4)

def validate_coloring(rdd):
    '''checks if the coloring is valid'''
    rdd_neighbour_colors = get_neighbour_colors_rdd(rdd)
    if rdd_neighbour_colors.filter(lambda x: (x[1] in x[3]) or x[1] == -1).count() > 0:
        return False
    return True

def get_neighbour_colors_rdd(rdd):
    """
    Update each node with its neighbors' current colors.
    Keeps the node's current color intact.
    """
    node_colors = rdd.map(lambda x: (x[0], x[1])).collectAsMap()

    updated_rdd = rdd.map(
        lambda x: (
            x[0],  # node ID
            x[1],  # current color
            x[2],  # neighbors
            #use node_colors to get the colors of the neighbors in pairs (neighbor_id, neighbor_color)
            [(neighbor, node_colors[neighbor]) for neighbor in x[2]],
        )
    )

    return updated_rdd 

def choose_color(current_color, neighbor_colors, max_colors):
    """
    Choose a color for the node that is not in the neighbor's colors.
    """
    if current_color != -1:
        return current_color
    
    for color in range(max_colors):
        if color not in neighbor_colors:
            return color

    return -1

def color_graph(graph_rdd, max_colors):
    """
    Perform distributed graph coloring.
    """
    colored_graph_rdd = None
    temp_rdd = get_neighbour_colors_rdd(graph_rdd)

    while temp_rdd.filter(lambda x: x[1] == -1).count() > 0:
        convergence_counter = temp_rdd.filter(lambda x: x[1] == -1).count()
        temp_rdd = get_neighbour_colors_rdd(temp_rdd)
        temp_rdd = temp_rdd.map(
            lambda x: (
                x[0],  # node ID
                choose_color(x[1], [i[1] for i in x[3]], max_colors),  # color
                x[2],  # neighbors
            )
        )

        temp_rdd = get_neighbour_colors_rdd(temp_rdd)
        temp_rdd = temp_rdd.map(
            lambda x: (
                x[0],  # node ID
                # If the color is not in the neighbor's colors, keep it or if this nodes id is smaller; otherwise, set it to -1
                -1 if any(x[1] == i[1] and x[0] > i[0] for i in x[3]) else x[1],  # color
                x[2],  # neighbors
            )
        )
        # If the graph is not colorable, break the loop
        if temp_rdd.filter(lambda x: x[1] == -1).count() == convergence_counter:
            break

    colored_graph_rdd = temp_rdd
        
    return colored_graph_rdd


def distributed_graph_coloring(graph_rdd):
    """
    Find the minimum number of colors required to color the graph.
    """
    max_degree = get_max_degree(graph_rdd)
    
    min_colors_graph_rdd = None
    for colors in range(max_degree + 1, 1, -1):  # Test from max_degree + 1 to 1 colors
        print(f"Trying {colors} colors...")
        colored_graph = color_graph(graph_rdd, colors)
        if colored_graph.filter(lambda x: x[1] == -1).count() == 0:
            min_colors_graph_rdd = colored_graph
        else:
            break
    
    return min_colors_graph_rdd

def main():
    rdd = load_json_into_rdd("graph.json")
    rdd_with_neighbour_colors = get_neighbour_colors_rdd(rdd)
    print(rdd_with_neighbour_colors.collect())

if __name__ == "__main__":
    main()