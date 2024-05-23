import os
import sys
import time
import random
import argparse
import utils
import numpy as np
import pickle
from model.model_sl import SLModel
from env import Env, AGE_ROOT

FOLDER_PREFIX = AGE_ROOT + "/model/"

def init_arguments():
    parser = argparse.ArgumentParser()
    # parser.add_argument("thpt_client", type=str, help="worker id of throughput client")
    parser.add_argument("--mode", type=str, default="train", help="train or test")
    parser.add_argument("--data_path", type=str, default="", help="File path of training data")
    parser.add_argument("--log_folder", type=str, default="logs/", help="Log folder")
    parser.add_argument("--save_folder", type=str, default="saves/", help="Save folder")
    parser.add_argument("--model_name", type=str, default="sl_ddpg", help="Existing model fname")
    parser.add_argument("--repmem_size", type=int, default=100000, help="Replay Memory size")
    parser.add_argument("--batch_size", type=int, default=64, help="Training batch size")
    parser.add_argument("--epoches", type=int, default=1000, help="Training epoches")
    parser.add_argument("--state_dim", type=int, default=56, help="State dimension")
    parser.add_argument("--action_dim", type=int, default=2, help="Action dimension")
    parser.add_argument("--load_model", action="store_true", help="Whether load model from disk")
    parser.add_argument("--learning_rate", type=float, default=0.001, help="Learning rate")
    parser.add_argument(
        "--changing_workload", action="store_true", help="Workload is changing with time"
    )  # default is False
    params = parser.parse_args()
    params.log_folder = FOLDER_PREFIX + params.log_folder
    params.save_folder = FOLDER_PREFIX + params.save_folder
    if params.mode == "train":
        assert len(params.data_path) != 0, "Data path is not set"
        if not os.path.exists(params.data_path):
            assert False, "Data file does not exist at {}".format(params.data_path)

    return params


def init_sl_model(params) -> SLModel:
    sl_params = {}
    sl_params["state_dim"] = params.state_dim
    sl_params["action_dim"] = params.action_dim
    sl_params["mem_size"] = params.repmem_size
    sl_params["model_dir"] = params.save_folder
    sl_params["model_name"] = params.model_name
    sl_params["batch_size"] = params.batch_size
    sl_params["load_model"] = params.load_model
    sl_params["alr"] = params.learning_rate
    sl_params["clr"] = 0.0001
    sl_params["tau"] = 0.001
    sl_params["gamma"] = 0.99

    return SLModel(sl_params)


def init_folder_and_logger(params) -> utils.Logger:
    if not os.path.exists(params.log_folder):
        os.mkdir(params.log_folder)
    if not os.path.exists(params.save_folder):
        os.mkdir(params.save_folder)

    log_file = params.log_folder + "/sl_train.log"
    if os.path.exists(log_file):
        os.remove(log_file)

    logger = utils.Logger(name="age", log_file=log_file)
    return logger


if __name__ == "__main__":
    params = init_arguments()
    logger = init_folder_and_logger(params)
    model = init_sl_model(params)

    random.seed(0)

    if params.mode == "train":
        logger.info("Start training")

        # load data
        with open(params.data_path, "rb") as f:
            data = pickle.load(f)
        logger.info("Load {} data from {}".format(len(data), params.data_path))

        num_samples = len(data)
        num_train_samples = int(num_samples * 0.8)
        num_test_samples = num_samples - num_train_samples
        num_train_steps = int(np.ceil(num_train_samples / params.batch_size))
        num_test_steps = int(np.ceil(num_test_samples / params.batch_size))

        for episode in range(params.epoches):
            if episode % 10 == 0:
                print("Episode {}".format(episode))
            episode_time = utils.time_start()
            random.shuffle(data)
            train_data = data[:num_train_samples]
            test_data = data[num_train_samples:]

            loss = 0
            train_time = 0
            for step in range(num_train_steps):
                start_idx = step * params.batch_size
                end_idx = (step + 1) * params.batch_size
                if end_idx > len(train_data):
                    end_idx = len(train_data)
                batch_data = train_data[start_idx:end_idx]
                batch_states = [x[0] for x in batch_data]
                batch_actions = [x[1] for x in batch_data]

                isprint = False

                cur_train_time = utils.time_start()
                loss += model.train_actor((batch_states, batch_actions), True, isprint)
                train_time += utils.time_end(cur_train_time)

                if step % 10 == 0:
                    logger.info("[Episode {}][Step {}] Loss {}".format(episode, step, loss / (step + 1)))

            test_loss = 0
            test_time = 0
            for step in range(num_test_steps):
                start_idx = step * params.batch_size
                end_idx = (step + 1) * params.batch_size
                if end_idx > len(test_data):
                    end_idx = len(test_data)
                batch_data = test_data[start_idx:end_idx]
                batch_states = [x[0] for x in batch_data]
                batch_actions = [x[1] for x in batch_data]

                isprint = False

                cur_test_time = utils.time_start()
                test_loss += model.train_actor((batch_states, batch_actions), False, isprint)
                test_time += utils.time_end(cur_test_time)

            logger.info("[Episode {}] Test Loss {}".format(episode, test_loss / num_test_steps))
            model.save_model(params.save_folder, params.model_name)
            episode_time = utils.time_end(episode_time)
            logger.info(
                "[Episode {}] Episode Time {}s, Train Time {}s, Test Time {}s".format(
                    episode, episode_time, train_time, test_time
                )
            )

    else:
        logger.info("Start testing")
