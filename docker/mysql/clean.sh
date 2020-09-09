docker stop mysql
docker rm mysql
docker run -d -p 3306:3306 --name=mysql usde-mysql

