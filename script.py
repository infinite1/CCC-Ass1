import argparse
import json
import os
import time
import heapq
from mpi4py import MPI
from collections import Counter 

# record start time
start_time = time.time()

def valid_file(filename):
    """Check if the input file is a json file"""
    base, ext = os.path.splitext(filename)
    if ext.lower() != '.json':
        raise argparse.ArgumentTypeError('file must have a json extension')
    return filename

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

# parse command line arguments
parser = argparse.ArgumentParser("python3 script.py")
parser.add_argument('filename', type=valid_file,
                    help='twitter json file to process')
args = parser.parse_args()
filename = args.filename

# record frequency of hashtags
hashtags_table = {}

# update hashtags frequency table
with open(filename) as data:
    line_number = 0
    for line in data:
        if rank != line_number % size:
            line_number += 1
            continue
        line_number += 1
        line = line.strip()	# remove trailing space
        try:
            content = json.loads(line[0:len(line)-1])	# remove trailing comma

            # count hashtags in tweet text
            hashtags = content["doc"]["entities"]["hashtags"]
            if len(hashtags) > 0:
                for hashtag in hashtags:
                    text = "#" + hashtag["text"].lower()
                    if text not in hashtags_table:
                        hashtags_table[text] = 0
                    hashtags_table[text] += 1

                    # # count hashtags in retweets
                    # retweet_count = content["doc"]["retweet_count"]
                    # hashtags_table[text] += retweet_count

            # # count quoted hashtags in retweet status
            # if "retweeted_status" in content["doc"]:
            # 	if "quoted_status" in content["doc"]["retweeted_status"]:
            # 		quote_hashtags = content["doc"]["retweeted_status"]["quoted_status"]["entities"]["hashtags"]
            # 		if len(quote_hashtags) > 0:
		          #       for hashtag in quote_hashtags:
		          #           text = "#" + hashtag["text"]
		          #           if text not in hashtags_table:
		          #               hashtags_table[text] = 0
		          #           hashtags_table[text] += 1

		          #           # count hashtags in retweets
		          #           retweet_count = content["doc"]["retweeted_status"]["quoted_status"]["entities"]["retweet_count"]
		          #           hashtags_table[text] += retweet_count


            # # count quoted hashtags in tweet text
            # if "quoted_status" in content["doc"]:
	           #  quote_hashtags = content["doc"]["quoted_status"]["entities"]["hashtags"]
	           #  if len(quote_hashtags) > 0:
	           #      for hashtag in quote_hashtags:
	           #          text = "#" + hashtag["text"]
	           #          if text not in hashtags_table:
	           #              hashtags_table[text] = 0
	           #          hashtags_table[text] += 1

	           #          # count hashtags in retweets
	           #          retweet_count = content["doc"]["retweeted_status"]["quoted_status"]["entities"]["retweet_count"]
	           #          hashtags_table[text] += retweet_count
	                    
            # print(content["doc"]["text"], content["doc"]["lang"])

        except:
            # skip lines that are not formatted correctly in json
            # print("invalid json format")
            continue

# if there is only one core used
if size == 1:
	# retrieve top ten most hashtags
	topTenHashtags = heapq.nlargest(10, hashtags_table.items(), key=lambda i: i[1])
	print(topTenHashtags)

	# calculate exucation time
	duration = time.time() - start_time
	print("\nThe program uses {0:.2f} seconds".format(duration))

	MPI.Finalize()

	
# if there are more than one core used, then gather data
elif size > 1:
	hashtags_table_array = comm.gather(hashtags_table)

# if at root node, then collect parallelized data
if rank == 0 and size > 1:
	# merge parallelized hashtags into one dictionary
	gather_data = {}
	for dictionary in hashtags_table_array:
		gather_data = dict(Counter(gather_data) + Counter(dictionary))

	# retrieve top ten most hashtags
	topTenHashtags = heapq.nlargest(10, gather_data.items(), key=lambda i: i[1])
	print(topTenHashtags)

	# calculate exucation time
	duration = time.time() - start_time
	print("\nThe program uses {0:.2f} seconds".format(duration))

	MPI.Finalize()
