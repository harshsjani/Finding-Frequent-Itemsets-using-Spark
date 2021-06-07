from os import stat
from pyspark import SparkContext
import csv
import json
import sys
import time


class PreProcess:
    def __init__(self) -> None:
        self.state_filter = sys.argv[1]
        self.review_file = sys.argv[2]
        self.business_file = sys.argv[3]
        self.output_file = sys.argv[4]
        self.sc = SparkContext.getOrCreate()
        self.sc.setLogLevel("OFF")

    @staticmethod
    def get_review_data(row):
        data = json.loads(row)
        return (data["business_id"], data["user_id"])
    
    @staticmethod
    def get_business_data(row):
        data = json.loads(row)
        return (data["business_id"], data["state"])

    def main(self):
        reviewRDD = self.sc.textFile(self.review_file).map(lambda row: PreProcess.get_review_data(row))
        bizRDD = self.sc.textFile(self.business_file).map(lambda row: PreProcess.get_business_data(row))#.filter(lambda row: row[1] == self.state_filter)#.map(lambda row: row[0])

        bizRDD = bizRDD.filter(lambda row: row[1] == self.state_filter)
        rdd = reviewRDD.join(bizRDD).map(lambda row: (row[1][0], row[0]))
        # rdd1 = reviewRDD.collect()
        # rdd2 = bizRDD.collect()
        with open(self.output_file, "w", newline='') as f:
            csvwriter = csv.writer(f)
            csvwriter.writerow(["user_id", "business_id"])
            csvwriter.writerows(rdd.collect())


if __name__ == "__main__":
    start_time = time.time()
    runner = PreProcess()
    runner.main()
    end_time = time.time()
    print("Duration: {}".format(end_time - start_time))
