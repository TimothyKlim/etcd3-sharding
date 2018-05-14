test:
	sbt docker:stage
	docker-compose down
	docker-compose build # --no-cache
	docker-compose up -d

	echo Wait to scale up
	sleep 15
	docker-compose up --scale app_node=2 -d

	echo Wait to scale up
	sleep 15
	docker-compose up --scale app_node=3 -d

	echo Wait to scale up
	sleep 15
	docker-compose up --scale app_node=5 -d

clean_k8s:
	kubectl delete -f k8s.yml --force || true
	kubectl scale statefulsets -n development etcd-sharding-worker --replicas 0
	kubectl scale statefulsets -n development etcd-sharding-node --replicas 0
	kubectl delete po -n development etcd-sharding-node-1 etcd-sharding-node-2 etcd-sharding-node-3 etcd-sharding-node-4 etcd-sharding-worker-0 etcd-sharding-node-0  etcd-sharding --force --grace-period=0 || true

test_k8s:
	clean_k8s
	kubectl create -f k8s.yml || true
	kubectl apply -f k8s.yml
	sleep 15
	kubectl scale statefulsets -n development etcd-sharding-worker --replicas 1
