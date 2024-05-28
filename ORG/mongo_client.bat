@echo
docker run -it --name mongo_cli --network=org_spark-mongo-network --rm mongo mongosh --host org-mongo-1 mongo:latest
pause