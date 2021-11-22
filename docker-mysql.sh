docker run -d \
--name mysql-movies \
-e MYSQL_ROOT_PASSWORD=MoviesAtTAU \
-p 3306:3306 \
mysql:8.0.27