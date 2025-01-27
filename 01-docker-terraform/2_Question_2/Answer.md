Step 1:
  - create docker-compose.yaml with given code

Step 2: 
  - docker-compose up -d to run it in detached mode and to keep working in the terminal

Step 3:
  - Go to your favorite browser and run localhost:8080.
  - Login with pgadmin@pgadmin.com and password as pgadmin
  - Try to add server with all the options of host and port pair and user as postgres and password as postgres 
    . postgres:5433 - It will fail.
    . localhost:5432 - It will fail.
    . db:5433 - It will fail.
    . postgres:5432 - It will add it.
    . db: 5432 - It will add it.

Reason: For the services in the same network, it will look for the port within the network and not the port on the host. The name can be container name (postgres) or the service name (db) that we have given.