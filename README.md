```
kubectl scale statefulsets -n development etcd-sharding-worker --replicas 1
kubectl scale statefulsets -n development etcd-sharding-node --replicas 2
```
