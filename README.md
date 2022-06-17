# dragonboat-example
dragonboat framework (https://github.com/lni/dragonboat) project use example

项目是对[dragonboat](https://github.com/lni/dragonboat) 深入使用的样例代码

## 项目架构

[dragonboat](https://github.com/lni/dragonboat) 框架支持多Raft，在dragonboat项目里的定义就是一个ReplicaID拥有多个ShardID。



protoc -I ./pb/api --go_out=plugins=grpc:./pb/api ./pb/api/*.proto
protoc -I ./pb/moveto --go_out=plugins=grpc:./pb/moveto ./pb/moveto/*.proto