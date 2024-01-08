//
// Created by yanglinglin on 24-1-8.
//


#include <chrono>
#include <future>
#include <thread>
#include <fstream>

#include "matchingcommand.h"
#include "graph/graph.h"
#include "Cardinality/WanderJoin.h"

#define NANOSECTOSEC(elapsed_time) ((elapsed_time)/(double)1000000000)
#define BYTESTOMB(memory_cost) ((memory_cost)/(double)(1024 * 1024))

int main(int argc, char** argv) {
  MatchingCommand command(argc, argv);
  std::string input_query_graph_file = command.getQueryGraphFilePath();
  std::string input_data_graph_file = command.getDataGraphFilePath();
  std::string est_method = command.getCardEstMethod();
  std::string sample_ratio = command.getSampleRatio();

  /**
   * Output the command line information.
   */
  std::cout << "Command Line:" << std::endl;
  std::cout << "\tData Graph: " << input_data_graph_file << std::endl;
  std::cout << "\tQuery Graph: " << input_query_graph_file << std::endl;
  std::cout << "\tEst Method: " << est_method << std::endl;
  std::cout << "\tSample Ratio: " << sample_ratio << std::endl;

  std::cout << "--------------------------------------------------------------------" << std::endl;

  /**
   * Load input graphs.
   */
  std::cout << "Load graphs..." << std::endl;
  auto start = std::chrono::high_resolution_clock::now();
  Graph* query_graph = new Graph(true);
  query_graph->loadGraphFromFile(input_query_graph_file);
  query_graph->buildCoreTable();
  Graph* data_graph = new Graph(true);
  data_graph->loadGraphFromFile(input_data_graph_file);
  data_graph->BuildLabelOffset();
  auto end = std::chrono::high_resolution_clock::now();
  double load_graphs_time_in_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

  std::cout << "-----" << std::endl;
  std::cout << "Query Graph Meta Information" << std::endl;
  query_graph->printGraphMetaData();
  std::cout << "-----" << std::endl;
  std::cout << "Data Graph Meta Information" << std::endl;
  data_graph->printGraphMetaData();

  std::cout << "--------------------------------------------------------------------" << std::endl;

  /**
   * Start cardinality estimation.
   */
   WanderJoin* wj_est_ptr = nullptr;
   if (est_method == "wjlinear") wj_est_ptr = new WanderJoin(data_graph, query_graph, WanderJoin::SampleType::Linear, std::stod(sample_ratio));
   else wj_est_ptr = new WanderJoin(data_graph, query_graph, WanderJoin::SampleType::Log, std::stod(sample_ratio));

  std::cout << "Start estimate..." << std::endl;
  std::cout << "-----" << std::endl;

  start = std::chrono::high_resolution_clock::now();
  std::cout << "Estimate cardinality: " << wj_est_ptr->GetCard() << std::endl;
  end = std::chrono::high_resolution_clock::now();
  double est_time_in_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();


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
  delete query_graph;
  delete data_graph;
  delete wj_est_ptr;

  /**
   * End.
   */
  std::cout << "--------------------------------------------------------------------" << std::endl;

  printf("Load graphs time (seconds): %.4lf\n", NANOSECTOSEC(load_graphs_time_in_ns));
  printf("Enumerate time (seconds): %.4lf\n", NANOSECTOSEC(est_time_in_ns));
  std::cout << "End." << std::endl;
  return 0;
}