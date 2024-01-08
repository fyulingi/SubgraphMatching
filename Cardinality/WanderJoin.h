//
// Created by yanglinglin on 24-1-7.
//

#ifndef SUBGRAPHMATCHING_WANDERJOIN_H
#define SUBGRAPHMATCHING_WANDERJOIN_H

#include <random>
#include<vector>

#include "graph/graph.h"
#include "utility/computesetintersection.h"

class WanderJoin {
 public:
    Graph* data_graph_;
    Graph* query_graph_;

    std::vector<ui> join_order_;
    std::vector<ui> node_to_index_;
    std::vector<std::vector<ui>> parent_node_;

    std::vector<ui> sample_count_;
    std::vector<ui> total_count_;

    ui* embedding_;
    bool* visited_node_;

    ull succ_count_;

    std::random_device rd_;
    std::mt19937 gen_;
    enum class SampleType{Linear, Log} sample_type_;
    double sample_ratio_;

    WanderJoin() = default;
    WanderJoin(Graph* data_graph, Graph* query_graph, SampleType sample_type, double sample_ratio);
    ~WanderJoin();

    void GetJoinOrder();
    void Expand(ui index);
    ull GetCard();
};


#endif //SUBGRAPHMATCHING_WANDERJOIN_H
