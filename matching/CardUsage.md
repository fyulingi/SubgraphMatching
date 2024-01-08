
## Usage Information

```shell
./cmake-build-debug/matching/EstCard.out -d test/sample_dataset/test_case_1.graph -q test/sample_dataset/query1_positive.graph -ratio 0.03 -method wjlinear -filter GQL -order GQL -engine LFTJ -num MAX
./cmake-build-debug/matching/SubgraphMatching.out -d test/sample_dataset/test_case_1.graph -q test/sample_dataset/query1_positive.graph -filter GQL -order GQL -engine LFTJ -num MAX

./cmake-build-debug/matching/EstCard.out -d test/data_graph/HPRD.graph -q test/query_graph/query_dense_16_1.graph -method wjlinear -ratio 10 
./cmake-build-debug/matching/SubgraphMatching.out -d test/data_graph/HPRD.graph -q test/query_graph/query_dense_16_1.graph -filter GQL -order GQL -engine LFTJ -num MAX
```

## Parameter
