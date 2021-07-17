from pyspark import SparkContext
from collections import defaultdict
from itertools import combinations
from functools import reduce
import time
import sys
import math

class Task1:
    BUCKET_COUNT = 32768

    def __init__(self):
        self.case = int(sys.argv[1]) - 1
        self.support = int(sys.argv[2])
        self.input_file = sys.argv[3]
        self.output_file = sys.argv[4]

        self.sc = SparkContext.getOrCreate()
        self.sc.setLogLevel("OFF")
        self.sc.setSystemProperty('spark.driver.memory', '4g')
        self.sc.setSystemProperty('spark.executor.memory', '4g')
    
    def format_data(self, data):
        dtext = ""
        res = defaultdict(list)
        ml = 0

        for x in data:
            l = len(x)
            if l > ml:
                ml = l
            res[l].append(str(x).replace(",)", ")"))
        for k in range(1, ml + 1):
            dtext += ",".join(res[k])
            dtext += "\n\n"
        return dtext

    def write_data(self, cand, freq):
        text = ''
        text += "Candidates:\n"
        text += self.format_data(cand)
        text += "Frequent Itemsets:\n"
        text += self.format_data(freq).rstrip("\n\n")

        with open(self.output_file, "w+") as f:
            f.write(text)

    @staticmethod
    def rkhash(x):
        A = 921382323
        B = 10 ** 9 + 7
        h = ord(x[0])

        for i in range(1, len(x)):
            h = (h * A + ord(x[i])) % B
        return h

    @staticmethod
    def basket_fn(x, case):
        elems = x.split(",")
        return elems[case], elems[~case]

    @staticmethod
    def get_L1(baskets, s):
        counts = defaultdict(int)
        buckets1 = [0] * Task1.BUCKET_COUNT
        buckets2 = [0] * Task1.BUCKET_COUNT

        for basket in baskets:
            for item in basket:
                counts[item] += 1
            for x, y in combinations(sorted(basket), 2):
                h1 = hash(x + y) % Task1.BUCKET_COUNT
                h2 = Task1.rkhash(x + y) % Task1.BUCKET_COUNT
                buckets1[h1] += 1
                buckets2[h2] += 1
        
        return [{k: v for k, v in counts.items() if v >= s}, (buckets1, buckets2)]
    
    @staticmethod
    def get_C2fromL1(baskets, L1, buckets1, buckets2, s):
        C2 = defaultdict(int)

        for basket in baskets:
            for x, y in combinations(sorted(basket), 2):
                h1 = hash(x + y) % Task1.BUCKET_COUNT
                h2 = Task1.rkhash(x + y) % Task1.BUCKET_COUNT
                if buckets1[h1] >= s and buckets2[h2] >= s:
                    if x in L1 and y in L1:
                        C2[(x, y)] += 1
        return C2

    @staticmethod
    def get_L2fromC2(C2, s):
        return [candidate for candidate, val in C2.items() if val >= s]

    @staticmethod
    def GetLFromC(C, s):
        return sorted([k for k, v in C.items() if v >= s])
    
    @staticmethod
    def GetCFromL(baskets, L, k):
        ckp1 = defaultdict(int)
        
        genset = set()
        for x in L:
            genset.update(set(x))

        for basket in baskets:
            gen = basket.intersection(genset)
            for x in combinations(sorted(gen), k):
                ckp1[tuple(x)] += 1

        return ckp1

    @staticmethod
    def filter_baskets(baskets, L1):
        freq_set = set(L1.keys())
        return list(map(lambda basket: basket.intersection(freq_set), baskets))

    @staticmethod
    def multihash(baskets, s, totalitems):
        s = math.ceil(s * len(baskets) / totalitems)
        print("s: {}".format(s))
        L1, (bv1, bv2) = Task1.get_L1(baskets, s)

        baskets = Task1.filter_baskets(baskets, L1)
        C2 = Task1.get_C2fromL1(baskets, L1, bv1, bv2, s)
        L2 = Task1.get_L2fromC2(C2, s)
        
        k = 3
        
        lk = L2
        frequent_items = []
        frequent_items.extend([(k,) for k in L1])
        frequent_items.extend([tuple(sorted(x)) for x in L2])

        del bv1
        del bv2
        del L1
        del C2
        
        
        while lk:
            print("Processing k: {}".format(k))
            ck = Task1.GetCFromL(baskets, lk, k)
            lk = Task1.GetLFromC(ck, s)
            
            if not lk:
                break

            frequent_items.extend(lk)
            k += 1

        return frequent_items

    @staticmethod
    def SONp2(baskets, candidates):
        ctr = defaultdict(int)

        for basket in baskets:
            bk = set(basket)
            for candidate in candidates:
                cd = set(candidate)
                if len(cd) > len(bk):
                    break
                if cd.issubset(bk):
                    ctr[candidate] += 1
        return [(k, v) for k, v in ctr.items()]

    def run(self):
        case = self.case
        support = self.support
        
        rdd = self.sc.textFile(self.input_file)
        first = rdd.first()
        rdd = rdd.filter(lambda x: x != first).map(lambda x: Task1.basket_fn(x, case)).groupByKey().map(lambda x: set(x[1]))
        total_items = rdd.count()
        
        candidates = rdd.mapPartitions(lambda chunk: Task1.multihash(list(chunk), support, total_items)).distinct().sortBy(lambda x: (len(x), x)).collect()
        frequents = rdd.mapPartitions(lambda chunk: Task1.SONp2(list(chunk), candidates)).reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] >= support).map(lambda x: x[0]).distinct().sortBy(lambda x: (len(x), x)).collect()
        self.write_data(candidates, frequents)


if __name__ == "__main__":
    runner = Task1()

    st = time.time()
    runner.run()
    et = time.time()

    print("Duration: {}".format(et - st))
