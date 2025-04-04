from pprint import pprint
from graph_coloring import *
from graph_node import *
import time
import argparse
import os

def generate_graph_from_args(num_nodes, max_degree):
    graph = generate_random_uag(num_nodes, max_degree)
    serialize_graph_into_json(graph, "args_graph.json")


#python3 main.py --input-file graph.json --output-file colored_graph.json
def main():
    parser = argparse.ArgumentParser(description="Graph Coloring CLI Interface")
    parser.add_argument(
        "--input-file",
        type=str,
        help="Path to the input graph file (JSON format)."
    )
    parser.add_argument(
        "--size",
        type=int,
        help="Size of the graph (number of nodes). Required if no input file is provided."
    )
    parser.add_argument(
        "--max-degree",
        type=int,
        help="Maximum degree of the graph nodes. Required if no input file is provided."
    )
    parser.add_argument(
        "--output-file",
        type=str,
        required=True,
        help="Path to save the serialized graph (JSON format)."
    )

    args = parser.parse_args()

    #check for valid arguments
    if not args.input_file and (not args.size or not args.max_degree):
        parser.error("Either --input-file or both --size and --max-degree must be specified.")

    if args.input_file and (args.size or args.max_degree):
        parser.error("--input-file and --size/--max-degree cannot be specified together.")

    rdd = None
    if args.input_file:
        rdd = load_json_into_rdd(args.input_file)
    else:
        generate_graph_from_args(args.size, args.max_degree)
        rdd = load_json_into_rdd("args_graph.json")
        #delete the temporary file
        os.remove("args_graph.json")
    
    #measure time to color the graph in seconds
    start_time = time.time()
    colored_graph = distributed_graph_coloring(rdd)
    end_time = time.time()
    print("Time to color the graph: " + str(end_time - start_time) + " seconds")
    rdd_to_json(args.output_file, colored_graph)

    if(validate_coloring(colored_graph)):
        print("Graph is properly colored")
    else:
        print("Graph is not properly colored")
    

if __name__ == "__main__":
    main()
    