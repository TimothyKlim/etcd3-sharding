```
kubectl scale statefulsets -n testing etcd-sharding-worker --replicas 1
kubectl scale statefulsets -n testing etcd-sharding-node --replicas 2
```
