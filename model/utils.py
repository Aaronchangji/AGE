# -*- coding: utf-8 -*-

import os
import time
import logging
import datetime
import numpy as np
from gensim.models import Word2Vec

WORD2VECMODEL = Word2Vec.load(os.getenv('AGE_ROOT') + "/model/saves/inter_data_type.model")
# WORD2VECMODEL = Word2Vec.load(os.getenv('AGE_ROOT') + "/model/saves/inter_data_type_op_level.model")

def time_start() -> float:
    return time.time()


def time_end(start) -> float:
    end = time.time()
    delay = end - start
    return delay


def get_timestamp_ms():
    return time.time()


def get_timestamp_sec():
    return int(time.time())


def time_to_str(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S.%f")


class Logger:
    def __init__(self, name, log_file="", append_log=False):
        self.log_file = log_file
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        sh = logging.StreamHandler()
        self.logger.addHandler(sh)
        if len(log_file) > 0:
            self.log2file = True
        else:
            self.log2file = False

        if not append_log:
            if os.path.exists(self.log_file):
                open(self.log_file, "w").close()

    def _write_file(self, msg):
        if self.log2file:
            with open(self.log_file, "a+") as f:
                f.write(msg + "\n")

    def get_timestr(self):
        timestamp = get_timestamp_ms()
        date_str = time_to_str(timestamp)
        return date_str

    def warn(self, msg):
        msg = "%s [WARN] %s" % (self.get_timestr(), msg)
        self.logger.warning(msg)
        self._write_file(msg)

    def info(self, msg):
        msg = "%s [INFO] %s" % (self.get_timestr(), msg)
        # self.logger.info(msg)
        self._write_file(msg)

    def error(self, msg):
        msg = "%s [ERROR] %s" % (self.get_timestr(), msg)
        self.logger.error(msg)
        self._write_file(msg)

# from https://github.com/songrotek/DDPG/blob/master/ou_noise.py
class OUProcess(object):
    def __init__(
        self,
        n_actions,
        theta=0.15,
        mu=0,
        sigma=0.1,
        sigma_decay_rate=0.95,
    ):
        self.n_actions = n_actions
        self.theta = theta
        self.mu = mu
        self.sigma = sigma
        self.next_sigma = sigma
        self.sigma_decay_rate = sigma_decay_rate
        self.current_value = np.ones(self.n_actions) * self.mu

    def reset(self, sigma=0):
        if sigma != 0:
            self.sigma = sigma
            return

        self.current_value = np.ones(self.n_actions) * self.mu
        if self.next_sigma != 0:
            self.sigma = self.next_sigma

    def decay(self):
        self.next_sigma = self.sigma * self.sigma_decay_rate

    def noise(self):
        x = self.current_value
        dx = self.theta * (self.mu - x) + self.sigma * np.random.randn(len(x))
        self.current_value = x + dx
        return self.current_value

def sigmoid(x):
    return 1 / (1 + np.exp(-x))

def desigmoid(x):
    return -np.log(1 / x - 1)

def inter_data_type_to_bitlist(type):
    # This type is a 16-bit integer, transfer it to a list of 0s and 1s with the same bitmap
    assert type < 2 ** 16, f"Invalid inter data type: {type}"
    bitlist = []
    for i in range(16):
        bitlist.append((type >> i) & 1)
    # print(f"Inter data type: {type}\t\tBitlist: {bitlist}")
    return bitlist

OPS = ["Expand", "Filter", "Property", "LocalAgg", "Project", "GlobalAgg", "FlowControl", "Scan"]

def inter_data_type_to_oplist(type):
    bitlist = inter_data_type_to_bitlist(type)
    oplist = []
    for i in range(16):
        idx = i % 8
        if bitlist[i]:
            oplist.append(OPS[idx])
    return oplist

def SIF_Embedding(X,p,a=1e-3):
    # X is the concatenated word embeddings (n,m)
    # p is the probability of each word     (n,1)

    U,_,_ = np.linalg.svd(np.matmul(X.transpose(),X))
    U = U[:,0]

    mV = np.matmul( ( a / (a + p) ) , X)
    mV /= X.shape[0]
    mV -= U * np.matmul(U.transpose(), mV)

    return mV
