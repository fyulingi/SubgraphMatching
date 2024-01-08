//
// Created by yanglinglin on 24-1-7.
//

#include "WanderJoin.h"

#include <map>
#include <queue>
#include <random>
#include <algorithm>

using namespace std;

const ui LEAST_NUM = 5;

WanderJoin::WanderJoin(Graph *data_graph, Graph *query_graph, SampleType sample_type, double sample_ratio):
    data_graph_(data_graph), query_graph_(query_graph), succ_count_(0), sample_type_(sample_type), sample_ratio_(sample_ratio) {
  embedding_ = new ui[query_graph->getVerticesCount()];
  visited_node_ = new bool[data_graph->getVerticesCount()];
  std::fill(visited_node_, visited_node_+data_graph->getVerticesCount(), false);
  node_to_index_.resize(query_graph->getVerticesCount());
  gen_ =  std::mt19937(rd_());

  sample_count_.resize(query_graph->getVerticesCount());
  total_count_.resize(query_graph->getVerticesCount());
}

WanderJoin::~WanderJoin() {
  delete[] embedding_;
  delete[] visited_node_;
}

inline bool IsomorphismTest(bool *visited_node, const ui* embedding, ui index) {
  return !visited_node[embedding[index]];
}

void WanderJoin::GetJoinOrder() {
  //  give each query node a score and sort it
  //  the score = label_count/degree
  //  the fewer the score is, the order is earlier
  map<ui, double> node_score_map;
  ui first_node_id = std::numeric_limits<ui>::max();
  double first_node_score = std::numeric_limits<double>::max();
  for (ui u = 0; u < query_graph_->getVerticesCount(); ++u) {
    double u_score = (double)data_graph_->getLabelsFrequency(query_graph_->getVertexLabel(u))/query_graph_->getVertexDegree(u);
    if (first_node_score > u_score) {
      first_node_score = u_score;
      first_node_id = u;
    }
    node_score_map[u] = u_score;
  }

  vector<bool> in_plan_node(query_graph_->getVerticesCount(), false);
  vector<bool> visited_node(query_graph_->getVerticesCount(), false);
  priority_queue<pair<double, ui>> p_queue;
  p_queue.emplace(first_node_score, first_node_id);

  while (!p_queue.empty()) {
    auto [s, u] = p_queue.top();
    p_queue.pop();
    vector<ui> parent_node;
    ui count = 0;
    const ui* neighbor = query_graph_->getVertexNeighbors(u, count);
    for (ui index = 0; index < count; ++index) {
      if (in_plan_node[neighbor[index]]) {
        parent_node.push_back(neighbor[index]);
      } else if (!visited_node[neighbor[index]]){
        p_queue.emplace(node_score_map[neighbor[index]], neighbor[index]);
        visited_node[neighbor[index]] = true;
      }
    }
    parent_node_.push_back(std::move(parent_node));
    node_to_index_[u] = join_order_.size();
    join_order_.push_back(u);
    in_plan_node[u] = true;
  }
}

void WanderJoin::Expand(ui index) {
  if (index == query_graph_->getVerticesCount()) {
    ++succ_count_;
    return;
  }
  ui count = 0;
  const ui *u_can_const_ptr = data_graph_->getNeighborsByLabel(embedding_[node_to_index_[parent_node_[index][0]]],
                                                               query_graph_->getVertexLabel(join_order_[index]),
                                                               count);
  ui* u_can_ptr = const_cast<ui*>(u_can_const_ptr);
  ui* wait_for_delete = nullptr;
  if (parent_node_[index].size() > 1) {
    ui *candidate = new ui[count];
    wait_for_delete = candidate;
    ui candidate_count = 0;
    for (ui u_p_index = 1; u_p_index < parent_node_[index].size(); ++u_p_index) {

      ui this_count = 0;
      const ui *this_can_ptr = data_graph_->getNeighborsByLabel(embedding_[node_to_index_[parent_node_[index][u_p_index]]],
                                                             query_graph_->getVertexLabel(join_order_[index]),
                                                             this_count);
      ComputeSetIntersection::ComputeCandidates(u_can_ptr, count, this_can_ptr, this_count, candidate, candidate_count);
      swap(u_can_ptr, candidate);
      swap(count, candidate_count);
    }
  }
  if (!count) { if (parent_node_[index].size() > 1) delete[] wait_for_delete; return;}
  total_count_[index] += count;
  std::uniform_int_distribution<> distrib(0, count-1);
  ui sample_total = 0;
  if (sample_type_ == SampleType::Linear) sample_total = max((ui)1, ui(count*sample_ratio_));
  else sample_total = max((ui)1, ui(log(count)*sample_ratio_));
  sample_total = max(sample_total, LEAST_NUM);
  sample_count_[index] += sample_total;

  for (ui can_count = 0; can_count < sample_total; ++can_count) {
    embedding_[index] = u_can_ptr[distrib(gen_)];
    if (IsomorphismTest(visited_node_, embedding_, index)) {
      visited_node_[embedding_[index]] = true;
      Expand(index+1);
      visited_node_[embedding_[index]] = false;
    }
  }
  delete[] wait_for_delete;
}

ull WanderJoin::GetCard() {
  GetJoinOrder();

  //  first node
  ui first_node_count = 0;
  const ui* first_node_ptr = data_graph_->getVerticesByLabel(query_graph_->getVertexLabel(join_order_[0]), first_node_count);
  if (!first_node_count) return 0;
  total_count_[0] = first_node_count;

  std::uniform_int_distribution<> distrib(0, first_node_count-1);
  ui sample_total = 0;
  if (sample_type_ == SampleType::Linear) sample_total = max((ui)1, ui(first_node_count*sample_ratio_));
  else sample_total = max((ui)1, ui(log(first_node_count)*sample_ratio_));
  sample_total = max(sample_total, LEAST_NUM);
  sample_count_[0] = sample_total;
  for (ui sample_count = 0; sample_count < sample_total; ++sample_count) {
    ui first_embedding = first_node_ptr[distrib(gen_)];
    embedding_[0] = first_embedding;
    visited_node_[first_embedding] = true;
    Expand(1);
    visited_node_[first_embedding] = false;
  }
  cout << "succ_count = " << succ_count_ << endl;

  cout << "total_count_: " << endl;
  for (auto x : total_count_) cout << x << " ";
  cout << endl << "sample_count_: " << endl;
  for (auto x : sample_count_) cout << x << " ";
  cout << endl;
  double est_card = succ_count_;
  for (ui index = 0; index < total_count_.size(); ++index) {
    est_card = max(est_card*total_count_[index]/max(sample_count_[index], (ui)1), 1.0);
  }
  return max((ull)1, (ull)(est_card));
}

