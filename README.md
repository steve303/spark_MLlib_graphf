# CS 498 - MP5 Templates

This repository contains templates to help you get started with MP5.

# Execution instructions
- Each file can be executed by running
```spark-submit --packages graphframes:graphframes:0.7.0-spark2.4-s_2.11 part_xxx.py```
- You can alternatively run the following to get rid of spark logs
```spark-submit --packages graphframes:graphframes:0.7.0-spark2.4-s_2.11 part_xxx.py 2> /dev/null```
- Make sure that you have the given dataset in the directory you are running
the given code from. The structure this repository is arranged in is recommended.
- While the extra argument for graphframes is not required for part b
and part c, it is not necessary to remove it these parts