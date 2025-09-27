# Databricks notebook source
!pip install jellyfish

# COMMAND ----------

!pip install us

# COMMAND ----------

#%sh
#pip install spacy
#python -m spacy download en_core_web_md

# COMMAND ----------

# Restart the Python process
#dbutils.library.restartPython()

# COMMAND ----------

from jellyfish import damerau_levenshtein_distance as dld
from jellyfish import soundex as sdx
import os
os.environ['DC_STATEHOOD'] = '1'
import us
import pyspark.sql.functions as f
from collections import Counter
import re
import numpy as np
#from itertools import combinations
#from scipy.spatial.distance import cosine
#import spacy
#nlp = spacy.load("en_core_web_md")

# COMMAND ----------

#def cosine_sim(a, b):
#    return 1 - cosine(a, b)

def get_word_similarity_metrics(word_a, word_b, ignore_lst=False):
    """Check if two words are similar based on edit distance and soundex."""
    for ignore_keyword in ignore_lst:
        word_a = word_a.replace(ignore_keyword, '')
        word_b = word_b.replace(ignore_keyword, '')
    return {
        'distance': dld(word_a, word_b),
        'same_soundex': sdx(word_a) == sdx(word_b)
    }

def should_map_words(metrics, score_a, score_b, min_dist):
    """Determine if words should be mapped based on similarity and scores."""
    return (metrics['distance'] < min_dist and 
            score_a > score_b and 
            metrics['same_soundex'])

def create_typo_mapping(word_cnt, min_dist=3, ignore_lst=[]):
    """Create mapping for similar words likely to be typos.
    
    Args:
        word_cnt (dict): Dictionary of words and their frequencies
        
    Returns:
        dict: Mapping of words to their corrected forms
    """
    # Initial pass to identify direct mappings
    direct_mappings = {}
    reverse_mappings = {}
    
    words = [k for k, v in word_cnt.most_common()]
    
    for i, word_a in enumerate(words):
        for word_b in words[(i+1):]:
            
            score_a = word_cnt[word_a]
            score_b = word_cnt[word_b]
            metrics = get_word_similarity_metrics(word_a, word_b, 
                                                  ignore_lst=ignore_lst)
            
            if should_map_words(metrics, score_a, score_b, min_dist):
                # Only update if new mapping has higher frequency
                if (word_b not in direct_mappings or 
                    score_a > direct_mappings[word_b]["score_a"]):
                    mapping_doc = {
                        "a": word_a,
                        "b": word_b,
                        "score_a": score_a,
                        "score_b": score_b
                    }
                    direct_mappings[word_b] = mapping_doc
                    reverse_mappings[word_a] = mapping_doc

    # Second pass to handle transitive mappings
    for key_reverse, doc_reverse in reverse_mappings.items():
        if key_reverse in direct_mappings:
            word_a = doc_reverse["a"]
            word_b = doc_reverse["b"]
            # Update mapping to point to highest frequency word
            direct_mappings[word_b]["a"] = direct_mappings[word_a]["a"]
            direct_mappings[word_b]["score_a"] = direct_mappings[word_a]["score_a"]

    # Create final replacement map
    replace_map = {k: v["a"] for k, v in direct_mappings.items()}
    
    return replace_map

def cleanup_statename(word_cnt):
    replace_map = {}
    for word_a in word_cnt.keys():
        cleaned = re.sub(r'[^a-zA-Z\s]', '', word_a)
        matching = us.states.lookup(cleaned)
        if matching:
            if word_a != matching.name:
                replace_map[word_a] = matching.name
    return replace_map


def get_replace_mapping(tablepath, colname, method, min_dist=3):
    word_cnt = Counter()
    for value in spark.read.table(tablepath).select(colname).toPandas()[colname].values:
        if isinstance(value, (list, np.ndarray)):
            for v in value:
                if v is not None:
                    word_cnt[v] += 1
        elif isinstance(value, str):
            for v in value.split(';'):
                if v != '':
                    word_cnt[v.strip()] += 1
    replace_map = {}
    if method == 'typofix':
        replace_map = create_typo_mapping(word_cnt, min_dist=min_dist)
    elif method == 'statename':
        replace_map = cleanup_statename(word_cnt)
    
    return replace_map

# COMMAND ----------


