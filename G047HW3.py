from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
from statistics import median, mean
from operator import add
import threading
import sys
import numpy as np
import random as rand

# After how many items should we stop?
THRESHOLD = 10000000

def hash(vertex,n,C):
        global hash_vals
        ret = ((hash_vals[n][0]*vertex+hash_vals[n][1])%pi)%C
        if C == 1:
            if ret == 0:
                ret = -1
        return ret

# Operations to perform after receiving an RDD 'batch' at time 'time'
def process_batch(time, batch, left, right, cols):
    global streamLength, histogram, mat, hash_vals
    # Variables definition

    # We are working on the batch at time `time`.
    batch_size = batch.count()
    if streamLength[0]>=THRESHOLD:
        return
    streamLength[0] += batch_size
    # Extract the distinct items from the batch in range (left,right)
    batch_items = batch.map(lambda s: (int(s), 1)).reduceByKey(add).collectAsMap()

    # Update the streaming state
    for key in batch_items:
        if left <= int(key) <= right:
                if key not in histogram:
                    histogram[key] = batch_items[key]
                else:
                    histogram[key] += batch_items[key]
                for place_i in range(batch_items[key]):
                    for i,row in enumerate(hash_vals):
                        mat[i][hash(key, i, cols)] += hash(key, i, 1)*1

    # If we wanted, here we could run some additional code on the global histogram
    if batch_size > 0:
        print("Batch size at time [{0}] is: {1}".format(time, batch_size))

    if streamLength[0] >= THRESHOLD:
        stopping_condition.set()
        


if __name__ == '__main__':
    #assert len(sys.argv) == 2, "USAGE: port"

    # IMPORTANT: when running locally, it is *fundamental* that the
    # `master` setting is "local[*]" or "local[n]" with n > 1, otherwise
    # there will be no processor running the streaming computation and your
    # code will crash with an out of memory (because the input keeps accumulating).
    conf = SparkConf().setMaster("local[*]").setAppName("DistinctExample")
    # If you get an OutOfMemory error in the heap consider to increase the
    # executor and drivers heap space with the following lines:
    # conf = conf.set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
    
    
    # Here, with the duration you can control how large to make your batches.
    # Beware that the data generator we are using is very fast, so the suggestion
    # is to use batches of less than a second, otherwise you might exhaust the memory.
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)  # Batch duration of 1 second
    ssc.sparkContext.setLogLevel("ERROR")
    
    # TECHNICAL DETAIL:
    # The streaming spark context and our code and the tasks that are spawned all
    # work concurrently. To ensure a clean shut down we use this semaphore.
    # The main thread will first acquire the only permit available and then try
    # to acquire another one right after spinning up the streaming computation.
    # The second tentative at acquiring the semaphore will make the main thread
    # wait on the call. Then, in the `foreachRDD` call, when the stopping condition
    # is met we release the semaphore, basically giving "green light" to the main
    # thread to shut down the computation.
    # We cannot call `ssc.stop()` directly in `foreachRDD` because it might lead
    # to deadlocks.
    stopping_condition = threading.Event()
    
    
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # INPUT READING
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # Check types
    assert sys.argv[1].isdigit(), "D must be an int"
    assert sys.argv[2].isdigit(), "W must be an int"
    assert sys.argv[3].isdigit(), "left must be an int"
    assert sys.argv[4].isdigit(), "right must be an int"
    assert sys.argv[5].isdigit(), "K must be an int"
    assert sys.argv[6].isdigit(), "portExp must be an int"

    D = int(sys.argv[1])
    W = int(sys.argv[2])
    left = int(sys.argv[3])
    right = int(sys.argv[4])
    K = int(sys.argv[5])
    portExp = int(sys.argv[6])
    print("Receiving data from port =", portExp)
    
    
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    streamLength = [0] # Stream length (an array to be passed by reference)
    histogram = {} # Hash Table for the distinct elements
    mat = np.zeros((D,W))
    hash_vals = []
    pi = 8191
    for i in range(D):
        a = rand.randint(1, pi-1)
        b = rand.randint(0, pi-1)
        hash_vals.append((a,b))
    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    # For each batch, to the following.
    # BEWARE: the `foreachRDD` method has "at least once semantics", meaning
    # that the same data might be processed multiple times in case of failure.
    stream.foreachRDD(lambda time, batch: process_batch(time, batch, left, right, W))
    
    # MANAGING STREAMING SPARK CONTEXT
    print("Starting streaming engine")
    ssc.start()
    print("Waiting for shutdown condition")
    stopping_condition.wait()
    print("Stopping the streaming engine")
    # NOTE: You will see some data being processed even after the
    # shutdown command has been issued: This is because we are asking
    # to stop "gracefully", meaning that any outstanding work
    # will be done.
    ssc.stop(False, True)
    print("Streaming engine stopped")

    # COMPUTE FINAL STATISTICS
    sum_er = 0
    vals_sure = []
    second_moment = 0
        # Second Moment
    for el in sorted(histogram):
        sum_er += histogram[el]
        second_moment += (histogram[el]**2)
        vals_sure.append(abs(histogram[el]))
    second_moment = second_moment/(sum_er**2)

        # Approx Sec Mom
    vals_med = []
    for el in sorted(histogram):
        res = []
        for i,row in enumerate(hash_vals):
            res.append(mat[i][hash(el, i, W)]*hash(el, i, 1))
        vals_med.append(median(res))
    vals_med = np.array(vals_med) 
    #second_moment_approx = sum(vals_med**2)/(sum_er**2)
    sec_moments = []
    for i,row in enumerate(hash_vals):
        sec_moments.append( sum(np.array(mat[i])**2) )
    second_moment_approx = median(sec_moments)/(sum_er**2)
        # Get index sorting on the frequencies,
        # we use the slice to obtain the descending order
    sorted_vals = np.argsort(vals_sure)[::-1] 

        # Avg err
    mean_are = []
    for sec in range(K):
        mean_are.append( (abs( vals_sure[sorted_vals[sec]] - vals_med[sorted_vals[sec]] ))
                            /vals_sure[sorted_vals[sec]] )
    mean_are = mean(mean_are)

    largest_item = max(histogram.keys())

    # Print section
    print("D =", D, "W =", W, "[left,right] = [{},{}]".format(left,right),\
          "K =", K, "Port =", portExp)
    print("Total number of items =", streamLength[0])
    print("Total number of items in [{},{}] =".format(left,right), sum_er)
    print("Number of distinct items in [{},{}] =".format(left,right), len(histogram))
    if K < 21:
        for idx_ranger in range(K):
            print("Item {} Freq = {} Est. Freq = {:.0f}".format( (sorted_vals[idx_ranger]+1),\
                                                vals_sure[sorted_vals[idx_ranger]],\
                                                vals_med[sorted_vals[idx_ranger]] ))

    print("Avg err for top {} =".format(K), mean_are)
    print("F2 {} F2 Estimate {}".format(second_moment,second_moment_approx))