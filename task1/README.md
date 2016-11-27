# cloud-computing-capstone

For this project, I've used the dataset called "airlines_ontime".
I've ignored cancelled and diverted flights, since those have not arrived at their destiny.

## 1. Clean the raw code
```
$ ./clean.rb /mnt/data/aviation/airline_ontime /mnt/data/cleaned
$ aws s3 sync /mnt/data/cleaned s3://mybucket/cleaned_data
```
