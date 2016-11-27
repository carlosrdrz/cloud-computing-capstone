# cloud-computing-capstone

For this project, I've used the dataset called "airlines_ontime".
I've ignored cancelled and diverted flights, since those have not arrived at their destiny.

## 1. Clean the raw code
```
$ ./clean.rb /mnt/data/aviation/airline_ontime /mnt/data/cleaned
$ aws s3 sync /mnt/data/cleaned s3://datatestrdrz/capstone
```

## 2. Task1 Group1
```
$ hadoop jar capstone.jar com.github.carlosrdrz.capstone.task1.TopAirports s3://datatestrdrz/capstone/ s3://datatestrdrz/results/task1/top_airports
```
