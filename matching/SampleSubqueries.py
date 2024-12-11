
import os
import random

dataset_prefix = "/home/yanglinglin/neural_dataset/"

datasets_name = ["eu2005"]
# datasets_name = ["dblp", "eu2005", "hprd", "human", "patents", "wordnet", "yeast", "youtube"]

def sample_for_dataset(dataset):
    save_prefix = dataset_prefix+dataset+"/sampled_subqueries/"
    os.makedirs(save_prefix, exist_ok=True)
    ccgs_path = dataset_prefix+dataset+"/cardinality_cost_graph/"
    queries_path = dataset_prefix+dataset+"/query_graph/"
    sampled_result_path = dataset_prefix+dataset+"/sampled_subqueries_files.txt"

    with open(sampled_result_path, 'w') as f:
        for filename in os.listdir(ccgs_path):
            if ("_dense_4_" in filename):
                continue
            file_path = os.path.join(ccgs_path, filename)
            with open(file_path, 'r') as file:
                first_line_list = file.readline().strip().split(" ")
                ccg_states_count = int(first_line_list[1])
                if ccg_states_count >= 5:
                    lines = file.readlines()  # begin at the second line
                    sampled_numbers = []
                    results = []
                    while True:
                        if (len(sampled_numbers) >= 5):
                            sampled_numbers.sort()
                            break
                        number = random.randint(1, ccg_states_count-1)
                        number_line = lines[number]
                        if (number not in sampled_numbers):
                            sampled_subqueries_list = number_line.strip().split()
                            if (int(sampled_subqueries_list[4]) >= 3):
                                sampled_numbers.append([number, int(sampled_subqueries_list[2])])
                
                    for index, number in enumerate(sampled_numbers):
                        save_subquery(queries_path, filename, index, sampled_subqueries_list)
                        base_name = filename.replace(".ccg", "")
                        f.write(f"{base_name}_{index}.graph {number[0]} {number[1]}\n")

def save_subquery(queries_prefix, ccg_name, index, sampled_subqueries_list):
    filename = (queries_prefix+ccg_name).replace(".ccg", ".graph")

    vertices, edges = read_graph(filename)
    sub_vertices, sub_edges = extract_subgraph(vertices, edges, set(list(map(int, sampled_subqueries_list[5:]))))
    output_filename = filename.replace(".graph", "").replace("query_graph", "sampled_subqueries")+"_"+str(index)+".graph"
    write_subgraph(output_filename, sub_vertices, sub_edges)

def read_graph(filename):
    vertices = {}
    edges = []
    with open(filename, 'r') as file:
        for line in file:
            parts = line.strip().split()
            if parts[0] == 'v': 
                # v id label degree
                vertex_id = int(parts[1])
                vertices[vertex_id] = [int(parts[1]), int(parts[2]), int(parts[3])] 
            elif parts[0] == 'e':
                # e v1 v2 label
                edge = (int(parts[1]), int(parts[2]), int(parts[3]))
                edges.append(edge)
    return vertices, edges

def extract_subgraph(vertices, edges, subset):
    new_id_map = {old_id: new_id for new_id, old_id in enumerate(sorted(subset))}
    
    sub_vertices = {new_id_map[v]: [new_id_map[v], vertices[v][1], 0] for v in subset}

    sub_edges = []
    for e in edges:
        if e[0] in subset and e[1] in subset:
            new_edge = (new_id_map[e[0]], new_id_map[e[1]], e[2])
            sub_edges.append(new_edge)
            sub_vertices[new_id_map[e[0]]][2] += 1
            sub_vertices[new_id_map[e[1]]][2] += 1

    return sub_vertices, sub_edges

def write_subgraph(filename, sub_vertices, sub_edges):
    with open(filename, 'w') as file:
        file.write(f"t {len(sub_vertices)} {len(sub_edges)}\n")

        for v in sorted(sub_vertices.keys()):
            file.write(f"v {sub_vertices[v][0]} {sub_vertices[v][1]} {sub_vertices[v][2]}\n")
        for e in sub_edges:
            file.write(f"e {e[0]} {e[1]} {e[2]}\n")


if __name__=="__main__":
    random.seed(42)
    for dataset in datasets_name:
        sample_for_dataset(dataset)
