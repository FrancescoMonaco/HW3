'''
HW3 - Group 47
  Alessandro Lucchiari
  Lorenzo Riccò
  Francesco Pio Monaco
'''
from pyspark import SparkContext, SparkConf
import sys, time, os
import random as rand
from operator import add
from statistics import median, mean
from collections import defaultdict