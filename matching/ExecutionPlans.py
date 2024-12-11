
import os
import subprocess
import concurrent.futures
from unittest import result
import numpy as np
from concurrent.futures import ProcessPoolExecutor


k = 3  # the execution times for each queries
# datasets = ['hprd']
# result_size_list = [100000]
result_size_list = [100000, 1000000, 5000000, 10000000]
# result_size = 100000
# datasets = ['yeast', 'dblp', 'eu2005', 'patents', 'youtube']
datasets = ['hprd', 'yeast', 'dblp', 'eu2005', 'patents', 'youtube']
# datasets = ['hprd', 'yeast', 'dblp', 'eu2005', 'patents', 'youtube','wordnet', 'human']

# methods = ['VF2PP', 'RI', 'INPUT', 'QSI', 'GQL',  'DPiso', 'CFL', 'TSO', 'CECI']
methods = ['VF2PP', 'RI', 'QSI', 'GQL',  'DPiso', 'CFL', 'TSO', 'CECI']
# methods = ['INPUT']
# methods = ['RI']
# methods = [ 'QSI', 'GQL',  'CFL']



def run_command(command):
    try:
        result = subprocess.run(command, shell=True, text=True, capture_output=True)
        return result.stdout, result.stderr
    except Exception as e:
        return None, str(e)


def extract_info(output):
    enumerate_time, filter_time, total_time, generate_time, embeddings, call_count = None, None, None, None, None, None
    for index, line in enumerate(output):
        if line.startswith("Enumerate time"):
            enumerate_time = float(line.strip().split()[-1])
        elif line.startswith("Filter vertices time"):
            filter_time = float(line.strip().split()[-1])
        elif line.startswith("Total time"):
            total_time = float(line.strip().split()[-1])
        elif line.startswith("Generate query plan time"):
            generate_time = float(line.strip().split()[-1])
        elif line.startswith("#Embeddings"):
            embeddings = int(line.strip().split()[-1])
        elif line.startswith("Call Count"):
            call_count = int(line.strip().split()[-1])
        
    return enumerate_time, filter_time, total_time, generate_time, embeddings, call_count

def run_for_dataset_and_order_method_and_limit(dataset, method, limit_num):
    print("Begin for {} {}".format(dataset, method))
    data_graph_path = f"../neural_dataset/{dataset}/data_graph/{dataset}.graph"
    query_graph_dir = f"../neural_dataset/{dataset}/query_graph"
    result_path = f"../CCG_count/result/opt/{dataset}/{method}_{limit_num}.txt"
    dataset_test_query_graph_file = f"../neural_dataset/{dataset}/all_test_query_graph.txt"

    test_query_set = set()
    with open(dataset_test_query_graph_file, 'r') as f:
        for line in f:
            test_query_set.add(line.strip()+'.graph')

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
            if query_graph_path not in test_query_set:
                # print(f"Skip {query_graph_path}")
                continue
            # if dataset != 'hprd' and ('dense_32_' in query_graph_path or 'sparse_32_' in query_graph_path): continue
            if method == 'INPUT':
                if query_graph_path not in have_plan_queries_name:
                    continue
            enumerate_time_list = []
            filter_time_list = []
            total_time_list = []
            generate_time_list = []
            embeddings_list = []
            call_count_list = []
            for kk in range(k):
                if method != 'CECI':
                    command = f"./build/matching/SubgraphMatching.out -d {data_graph_path} -q {query_graph_dir}/{query_graph_path} -order {method} -num {limit_num}"
                else:
                    command = f"./build/matching/SubgraphMatching.out -d {data_graph_path} -q {query_graph_dir}/{query_graph_path} -filter CECI -order {method} -engine CECI -num {limit_num}"

                print(command)
                try:
                    result = subprocess.run(command, shell=True, capture_output=True, timeout=350, text=True)
                    stdout = result.stdout

                    if stdout is not None:
                        enumerate_time, filter_time, total_time, generate_time, embeddings, call_count = extract_info(stdout.splitlines()[-15:])
                        enumerate_time_list.append(enumerate_time)
                        filter_time_list.append(filter_time)
                        total_time_list.append(total_time)
                        generate_time_list.append(generate_time)
                        embeddings_list.append(embeddings)
                        call_count_list.append(call_count)
                except subprocess.TimeoutExpired:
                    # f.write(f"{query_graph_path} TIMEOUT\n")
                    print("The command timed out after the specified duration.")
                    break
                except Exception as e:
                    # f.write(f"{query_graph_path} ERROR\n")
                    print(f"An error occurred: {e}")
            if len(enumerate_time_list) == k:
                f.write(f"{query_graph_path} {np.array(enumerate_time_list).mean()} {np.array(filter_time_list).mean()} {np.array(total_time_list).mean()} {np.array(generate_time_list).mean()} {np.array(embeddings_list).mean()} {np.array(call_count_list).mean()}\n")
            else:
                f.write(f"{query_graph_path} TIMEOUT\n")


if __name__ == '__main__':

    # 使用 ProcessPoolExecutor 来并行化任务
    with ProcessPoolExecutor(max_workers=5) as executor:
        # 提交任务到线程池，返回结果
        futures = [executor.submit(run_for_dataset_and_order_method_and_limit, dataset, method, limit_num)
                   for dataset in datasets
                   for method in methods
                   for limit_num in result_size_list]
        
        # 等待并获取结果
        for future in futures:
            result = future.result()  # 获取每个任务的返回值
            print(f"Completed task: {result}")
