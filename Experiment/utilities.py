import collections
import os
import errno
import numpy as np
import string
import random
import re


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


# Local implementation of the sigmoid function
def sigmoid(x):
    if isinstance(x, list):
        x = np.asarray(x)
    return 1. / (1 + np.exp(-x))


def hash_string(string):
    return '/hash_' + str(abs(hash(string)))


def make_path(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def dict_to_string(dictionary):
    return ''.join('{} : {}, '.format(key, val) for key, val in dictionary.items())[:-2]


def get_column_names_from_sql_query(sql_query):
    # query cannot contain '*', if it does then raise value error
    if '*' in sql_query:
        raise ValueError("The query must note contain * to be able to extract column names")
    sql_query = sql_query.replace('\n', '').replace('\t', '').replace('\r', '')
    lines = re.findall(r'SELECT(.+?)FROM', sql_query)[0].lower().split(',')
    column_names = []
    for line in lines:
        if ' as ' in line.lower():
            column_names.append(line.split('as')[-1].strip())
        elif len(re.findall(r'[\(\)\':]+', line)) == 0:
            column_names.append(line.strip())
    return column_names

def should_only_have(value, keywords):
    return (isinstance(value, collections.Iterable) and all(keyword in value for keyword in keywords)
            and len(value) == len(keywords))