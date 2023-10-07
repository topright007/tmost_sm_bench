psql -h localhost -p 15432 -U postgres -c 'drop database if exists xxx;' && \
 psql -h localhost -p 15432 -U postgres -c 'create database xxx;' && \
 psql -h localhost -p 15432 -U postgres -d xxx -a -f ./migration.sql
