
## Usage Information


### EstCard

```shell
./cmake-build-debug/matching/EstCard.out -d test/data_graph/HPRD.graph -q test/query_graph/query_dense_16_1.graph -method wjlinear -ratio 0.05
./cmake-build-debug/matching/EstCardOneDir.out -d test/data_graph/HPRD.graph -qd ../dataset/hprd/query_graph -method wjlog -ratio 0.05
```

### Match

```shell
./cmake-build-debug/matching/SubgraphMatching.out -d test/data_graph/HPRD.graph -q test/query_graph/query_dense_16_1.graph -filter GQL -order GQL -engine LFTJ -num MAX
./cmake-build-debug/matching/MatchFromPlans.out -d ./test/data_graph/HPRD.graph -qd ../dataset/hprd/query_graph -order INPUT -num 500000
```

## Parameter

* `-d` : data graph file path
* `-q` : query graph file path
* `-qd` : query graph directory path
* `-order` : order method
* `-filter` : filter method
* `-engine` : matching engine
* `-num` : number of matching results
* `method` : cardinality estimation method  (`wjlinear` or `wjlog`)
* `ratio` : ratio of sampling (only used for `wjlinear` method)


