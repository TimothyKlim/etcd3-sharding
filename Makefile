test:
	sbt docker:stage
	docker-compose down
	docker-compose up -d
	docker-compose up --scale app_node=3
