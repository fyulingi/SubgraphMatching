set(SUBGRAPH_MATCHING_FILE_LIST
        matchingcommand.cpp
        matchingcommand.h
        FilterVertices.cpp
        FilterVertices.h
        BuildTable.cpp
        BuildTable.h
        GenerateQueryPlan.cpp
        GenerateQueryPlan.h
        EvaluateQuery.cpp
        EvaluateQuery.h
        GenerateFilteringPlan.cpp
        GenerateFilteringPlan.h
        StudyPerformance.cpp)

set(MATCH_FROM_PLANS_FILE_LIST
        matchingcommand.cpp
        matchingcommand.h
        FilterVertices.cpp
        FilterVertices.h
        BuildTable.cpp
        BuildTable.h
        GenerateQueryPlan.cpp
        GenerateQueryPlan.h
        EvaluateQuery.cpp
        EvaluateQuery.h
        GenerateFilteringPlan.cpp
        GenerateFilteringPlan.h
        MatchFromPlans.cpp)

set(EST_CARD_FILE_LIST
        matchingcommand.cpp
        matchingcommand.h
        EstCard.cpp
)

set(EST_CARD_DIR_FILE_LIST
        matchingcommand.cpp
        matchingcommand.h
        EstCardOneDir.cpp
)

set(GRAPH_CONVERTER_FILE_LIST
        GraphConverter.cpp)

add_executable(SubgraphMatching.out ${SUBGRAPH_MATCHING_FILE_LIST})
target_link_libraries(SubgraphMatching.out graph utility)

add_executable(MatchFromPlans.out ${MATCH_FROM_PLANS_FILE_LIST})
target_link_libraries(MatchFromPlans.out graph utility)

add_executable(EstCard.out ${EST_CARD_FILE_LIST})
target_link_libraries(EstCard.out graph utility card)

add_executable(EstCardOneDir.out ${EST_CARD_DIR_FILE_LIST})
target_link_libraries(EstCardOneDir.out graph utility card)


add_executable(GraphConverter.out ${GRAPH_CONVERTER_FILE_LIST})
target_link_libraries(GraphConverter.out graph utility)

add_executable(EdgeListConverter.out EdgeListToCSR.cpp)
add_executable(ReassignLabel ReassignLabel.cpp)
add_executable(SelectEdge SelectEdge.cpp)