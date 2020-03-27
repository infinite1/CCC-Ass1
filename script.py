import argparse
import json
import os
import time
import heapq
import pycountry

def valid_file(filename):
    """Check if the input file is a json file"""
    base, ext = os.path.splitext(filename)
    if ext.lower() != '.json':
        raise argparse.ArgumentTypeError('file must have a json extension')
    return filename

# record start time
start_time = time.time()

# parse command line arguments
parser = argparse.ArgumentParser("python3 script.py")
parser.add_argument('filename', type=valid_file,
                    help='twitter json file to process')
args = parser.parse_args()
filename = args.filename

# record frequency of hashtags
hashtags_table = {}
langaugae_table = {}

with open(filename) as data:
    for line in data:
        line = line.strip()
        try:
            content = json.loads(line[0:len(line)-1])

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

topTenHashtags = heapq.nlargest(10, hashtags_table.items(), key=lambda i: i[1])
print("Top ten hashtags: ",topTenHashtags)

topFiveLan = heapq.nlargest(5,langaugae_table.items(), key=lambda i: i[1])
print("Top five languages: ",topFiveLan)

# calculate exucation time
duration = time.time() - start_time
print("\nThe program uses {0:.2f} seconds".format(duration))
