
import os
from re import X
import subprocess
import concurrent.futures
from unittest import result
from concurrent.futures import ProcessPoolExecutor


datasets = ['yeast']
# datasets = ['hprd', 'yeast', 'dblp', 'eu2005', 'patents', 'youtube','wordnet', 'human']

# methods = ['VF2PP', 'RI', 'QSI', 'GQL',  'DPiso', 'CFL', 'TSO', 'INPUT']
# methods = ['VF2PP', 'RI', 'QSI', 'GQL',  'DPiso', 'CFL', 'TSO', 'CECI']
methods = ['INPUT']


def run_command(command):
    try:
        result = subprocess.run(command, shell=True, text=True, capture_output=True)
        return result.stdout, result.stderr
    except Exception as e:
        return None, str(e)


def extract_info(output):
    query_vertice_count = 0
    matching_order = []
    pivots = []
    for index, line in enumerate(output):
        if index == 0:
            query_vertice_count = int(line.strip().split()[1])
        elif index == 1:
            matching_order = list(map(int, line.strip().split(" ")))
        elif index == 2:
            pivots = list(map(int, line.strip().split(" ")))
    return query_vertice_count, matching_order, pivots

        
    return enumerate_time, filter_time, total_time, generate_time, embeddings

def run_for_dataset_and_order_method(dataset, method):
    print("Begin for {} {}".format(dataset, method))
    data_graph_path = f"../neural_dataset/{dataset}/data_graph/{dataset}.graph"
    query_graph_dir = f"../neural_dataset/{dataset}/query_graph"
    result_path = f"../CCG_count/result/opt/{dataset}/{method}_order.txt"

    result_dir = os.path.dirname(result_path)
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
        
    if method == 'INPUT':
        plan_file = f'../CCG_count/result/{dataset}_plans.txt'
        with open(plan_file, 'r') as f:
            have_plan_queries_name = set()
            for line in f:
                have_plan_queries_name.add(line.strip().split(",")[0]+'.graph')

    with open(result_path, 'w') as f:
        for query_graph_path in os.listdir(query_graph_dir):
            if method == 'INPUT':
                if query_graph_path not in have_plan_queries_name: continue
            if method != 'CECI':
                command = f"./build/matching/PrintOrder.out -d {data_graph_path} -q {query_graph_dir}/{query_graph_path} -order {method} -num 100000"
            else:
                command = f"./build/matching/PrintOrder.out -d {data_graph_path} -q {query_graph_dir}/{query_graph_path} -filter CECI -order {method} -engine CECI -num 100000"

            print(command)
            try:
                result = subprocess.run(command, shell=True, capture_output=True, timeout=350, text=True)
                stdout = result.stdout

                if stdout is not None:
                    query_vertices_count, matching_order, pivots = extract_info(stdout.splitlines()[-3:])
                    f.write(f"{query_graph_path} {query_vertices_count} {' '.join(str(x) for x in matching_order)} {' '.join(str(x) for x in pivots)}\n")
                else:
                    f.write(f"{query_graph_path} EXECUTIONERROR\n")
            except subprocess.TimeoutExpired:
                f.write(f"{query_graph_path} TIMEOUT\n")
                print("The command timed out after the specified duration.")
            except Exception as e:
                f.write(f"{query_graph_path} ERROR\n")
                print(f"An error occurred: {e}")
            

if __name__ == '__main__':

    # 使用 ProcessPoolExecutor 来并行化任务
    with ProcessPoolExecutor(max_workers=6) as executor:
        # 提交任务到线程池，返回结果
        futures = [executor.submit(run_for_dataset_and_order_method, dataset, method)
                   for dataset in datasets
                   for method in methods]
        
        # 等待并获取结果
        for future in futures:
            result = future.result()  # 获取每个任务的返回值
            print(f"Completed task: {result}")

