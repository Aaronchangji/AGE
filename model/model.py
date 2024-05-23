import os
import pickle
import numpy as np
import random
import utils


class Model(object):
    def __init__(self):
        pass
        # self._build_repmem(params["model_dir"], params["model_name"], params["mem_size"])

    def choose_action(self, curr_state):
        return random.random()

    @staticmethod
    def _get_params(params) -> dict:
        return params

    def add_sample(self, state, action, reward, next_state, done):
        pass

    def update(self):
        pass

    def load_model(self, model_dir, model_name):
        pass

    def save_model(self, model_dir, model_name):
        pass
