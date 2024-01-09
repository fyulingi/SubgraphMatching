//
// Created by yanglinglin on 24-1-9.
//

#include <chrono>
#include <future>
#include <thread>
#include <fstream>
#include <filesystem>

#include "matchingcommand.h"
#include "graph/graph.h"
#include "Cardinality/WanderJoin.h"

#define NANOSECTOSEC(elapsed_time) ((elapsed_time)/(double)1000000000)
#define BYTESTOMB(memory_cost) ((memory_cost)/(double)(1024 * 1024))

int main(int argc, char** argv) {
  MatchingCommand command(argc, argv);
  std::string input_query_graph_dir = command.getQueryDir();
  std::string input_data_graph_file = command.getDataGraphFilePath();
  std::string est_method = command.getCardEstMethod();
  std::string sample_ratio = command.getSampleRatio();

  /**
   * Output the command line information.
   */
  std::cout << "Command Line:" << std::endl;
  std::cout << "\tData Graph: " << input_data_graph_file << std::endl;
  std::cout << "\tQuery Graph Dir: " << input_query_graph_dir << std::endl;
  std::cout << "\tEst Method: " << est_method << std::endl;
  std::cout << "\tSample Ratio: " << sample_ratio << std::endl;

  std::cout << "--------------------------------------------------------------------" << std::endl;

  /**
   * Load input graphs.
   */
  std::cout << "Load data graphs..." << std::endl;
//  auto start = std::chrono::high_resolution_clock::now();
  Graph* data_graph = new Graph(true);
  data_graph->loadGraphFromFile(input_data_graph_file);
  data_graph->BuildLabelOffset();
//  auto end = std::chrono::high_resolution_clock::now();
//  double load_graphs_time_in_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

  std::cout << "-----" << std::endl;
  std::cout << "Data Graph Meta Information" << std::endl;
  data_graph->printGraphMetaData();

  std::cout << "--------------------------------------------------------------------" << std::endl;

  /**
   * Start cardinality estimation.
   */

  for (const auto &query_entry : std::filesystem::directory_iterator(input_query_graph_dir)) {
    if (query_entry.is_regular_file()) { // 判断是否为文件
      std::cout << "query_entry_name: " << query_entry.path() << std::endl;
      Graph* query_graph = new Graph(true);
      query_graph->loadGraphFromFile(query_entry.path());
      query_graph->buildCoreTable();

      WanderJoin* wj_est_ptr = nullptr;
      if (est_method == "wjlinear") wj_est_ptr = new WanderJoin(data_graph, query_graph, WanderJoin::SampleType::Linear, std::stod(sample_ratio));
      else wj_est_ptr = new WanderJoin(data_graph, query_graph, WanderJoin::SampleType::Log, std::stod(sample_ratio));

      auto est_start = std::chrono::high_resolution_clock::now();
      auto est_card = wj_est_ptr->GetCard();
      auto est_end = std::chrono::high_resolution_clock::now();
      double est_time_in_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(est_end - est_start).count();
      std::cout << query_entry.path() << " " << est_card << " " << NANOSECTOSEC(est_time_in_ns) << std::endl;

      delete query_graph;
      delete wj_est_ptr;
    }
  }


#ifdef DISTRIBUTION
  std::ofstream outfile (input_distribution_file_path , std::ofstream::binary);
    outfile.write((char*)EvaluateQuery::distribution_count_, sizeof(size_t) * data_graph->getVerticesCount());
    delete[] EvaluateQuery::distribution_count_;
#endif

  std::cout << "--------------------------------------------------------------------" << std::endl;
  std::cout << "Release memories..." << std::endl;
  /**
   * Release the allocated memories.
   */
  delete data_graph;
  return 0;
}