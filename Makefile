test:
	sbt docker:stage
	docker-compose down
	docker-compose build --no-cache
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
