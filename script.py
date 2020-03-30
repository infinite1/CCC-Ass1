import argparse
import json
import os
import time
import heapq
import pycountry
from mpi4py import MPI
from collections import Counter

# mpiexec -np 4 python3 script.py

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

# record frequency of hashtags and languages
hashtags_table = {}
langaugae_table = {}
all_lines = 0

# update hashtags frequency table
with open(filename) as data:
    line_number = 0
    for line in data:
        if rank != line_number % size:
            line_number += 1
            continue
        line_number += 1
        line = line.strip()  # remove trailing space
        try:
            content = json.loads(line[0:len(line) - 1])  # remove trailing comma

            hashtags = content["doc"]["entities"]["hashtags"]
            if len(hashtags) > 0:
                for hashtag in hashtags:
                    text = "#" + hashtag["text"]
                    if text not in hashtags_table:
                        hashtags_table[text] = 0
                    hashtags_table[text] += 1

            langaugaes = content["doc"]["metadata"]["iso_language_code"]
            langaugaes_name = pycountry.languages.get(alpha_2=langaugaes).name
            if langaugaes_name not in langaugae_table:
                langaugae_table[langaugaes_name] = 0
            langaugae_table[langaugaes_name] += 1


        except:
            # skip lines that are not formatted correctly in json
            # print("invalid json format")
            continue

# if there is only one core used
if size == 1:
    # retrieve top ten most hashtags
    topTenHashtags = heapq.nlargest(10, hashtags_table.items(), key=lambda i: i[1])
    print("Top ten hashtags: ",topTenHashtags)
    # retrieve top ten most languages
    topFiveLan = heapq.nlargest(5, langaugae_table.items(), key=lambda i: i[1])
    print("Top five languages: ", topFiveLan)

    # calculate exucation time
    duration = time.time() - start_time
    print("\nThe program uses {0:.2f} seconds".format(duration))

    MPI.Finalize()


# if there are more than one core used, then gather data
elif size > 1:
    hashtags_table_array = comm.gather(hashtags_table)
    languages_table_array = comm.gather(langaugae_table)

# if at root node, then collect parallelized data
if rank == 0 and size > 1:
    # merge parallelized hashtags into one dictionary
    gather_tags = {}
    gather_language = {}

    for tag_dictionary in hashtags_table_array:
        gather_tags = dict(Counter(gather_tags) + Counter(tag_dictionary))
    for language_dictionary in languages_table_array:
        gather_language = dict(Counter(gather_language) + Counter(language_dictionary))



    # retrieve top ten most hashtags
    topTenHashtags = heapq.nlargest(10, gather_tags.items(), key=lambda i: i[1])
    print(topTenHashtags)

    # retrieve top ten most languages
    topFiveLan = heapq.nlargest(5, gather_language.items(), key=lambda i: i[1])
    print("Top five languages: ", topFiveLan)


    # calculate exucation time
    duration = time.time() - start_time
    print("\nThe program uses {0:.2f} seconds".format(duration))

    MPI.Finalize()
