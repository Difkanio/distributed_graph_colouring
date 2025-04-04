import random
import json

class Node:
    def __init__(self, id, color = -1):
        self.id = id
        self.neighbors = []
        self.color = color

    def add_neighbor(self, neighbor):
        self.neighbors.append(neighbor)

    def set_color(self, color):
        self.color = color

    def __repr__(self):
        return self.id
    
    def __str__(self):
        return str(self.id) + " " + str(self.color) + " " + str([neighbor.id for neighbor in self.neighbors])


def generate_random_uag(num_nodes, max_degree):
    if num_nodes <= 0:
        raise ValueError("num_nodes must be greater than 0")
    if max_degree < 0:
        raise ValueError("max_degree must be non-negative")
    
    nodes = [Node(i) for i in range(num_nodes)]
    result = []
    stack = []
    
    # Initialize the graph with the first node
    current_node = nodes.pop(0)
    result.append(current_node)
    stack.append(current_node)

    while stack and nodes:
        current_node = stack.pop()
        
        # Determine the number of neighbors for the current node
        num_neighbours = min(random.randint(0, min(len(nodes), max_degree)), random.randint(0, min(len(nodes), max_degree)))

        while len(current_node.neighbors) < num_neighbours and nodes:
            random_index = random.randint(0, len(nodes) - 1)
            random_node = nodes.pop(random_index)

            if random_node not in current_node.neighbors:
                current_node.add_neighbor(random_node)
                result.append(random_node)
                stack.append(random_node)

    # Add return edges
    for node in result:
        for neighbor_node in node.neighbors:
            if node not in neighbor_node.neighbors:
                neighbor_node.add_neighbor(node)

    return result


def serialize_graph_into_json(nodes, output_path):
    graph = []
    for node in nodes:
        node_data = {}
        node_data["id"] = node.id
        node_data["color"] = node.color
        node_data["neighbors"] = ",".join([str(neighbor.id) for neighbor in node.neighbors])
        graph.append(node_data)

    with open(output_path, "w") as f:
        json.dump(graph, f, indent=4)

def generate_random_graph(num_nodes, max_degree):
    if num_nodes <= 0:
        raise ValueError("num_nodes must be greater than 0")
    if max_degree < 0:
        raise ValueError("max_degree must be non-negative")
    
    nodes = [Node(i) for i in range(num_nodes)]

    for i in range(num_nodes):
        num_neighbours = random.randint(0, max_degree - len(nodes[i].neighbors))
        for _ in range(num_neighbours):
            random_index = random.randint(0, num_nodes - 1 - len(nodes[i].neighbors))
            if random_index != i and nodes[random_index] not in nodes[i].neighbors and len(nodes[random_index].neighbors) < max_degree:
                nodes[i].add_neighbor(nodes[random_index])
                nodes[random_index].add_neighbor(nodes[i])
    
    return nodes

def main():
    #nodes = generate_random_uag(1000, 5)
    nodes = generate_random_graph(30000, 10)
    serialize_graph_into_json(nodes, "graph.json")

if __name__ == "__main__":
    main()