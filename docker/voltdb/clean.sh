docker exec -it node1 sqlcmd --query='delete from user'
docker exec -it node1 sqlcmd --query='delete from tweet'
docker exec -it node1 sqlcmd --query='delete from engagement'