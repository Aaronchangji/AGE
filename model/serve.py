import os
import sys
import time
import random
import argparse
import utils
import numpy as np
from model import Model
from model.model_sl import SLModel
from model_search import Search
from model_sample import SampleModel
from state import RawState
from env import Env, AGE_ROOT

FOLDER_PREFIX = AGE_ROOT + "/model/"


def init_arguments():
    parser = argparse.ArgumentParser()
    # global
    parser.add_argument("thpt_client", type=str, help="worker id of throughput client")
    parser.add_argument("--method", type=str, required=True, default="sl", help="method to pick actions")
    parser.add_argument("--log_folder", type=str, default="logs/", help="Log folder")
    parser.add_argument("--save_folder", type=str, default="saves/", help="Save folder")
    parser.add_argument("--buffer_prefix", type=str, default="age", help="Buffer prefix")
    parser.add_argument("--buffer_size", type=int, default=1048576, help="Buffer size")
    parser.add_argument("--repmem_size", type=int, default=100000, help="Replay Memory size")
    parser.add_argument("--epoches", type=int, default=100000, help="Training epoches")
    parser.add_argument("--epoches_per_workload", type=int, default=5, help="Epoches per workload")
    parser.add_argument("--max_steps", type=int, default=12, help="Max steps per episode")
    parser.add_argument("--max_ms_steps", type=int, default=4, help="Max steps for ms")
    parser.add_argument("--changing_workload", action="store_true", help="Workload is changing with time")
    parser.add_argument("--append_log", action="store_true", help="Append log to file")
    parser.add_argument("--generate_trace", action="store_true", help="Generate trace for the workload")
    parser.add_argument("--force_restart_server", action="store_true", help="Force restart server for each step")

    # Env related
    parser.add_argument("--no_opsize", action="store_true", help="state without opsize")
    parser.add_argument("--use_linear", action="store_true", help="linear scale action")
    parser.add_argument("--with_sigmoid", action="store_true", help="Use sigmoid to normalize output")
    parser.add_argument("--use_normalized_inter_data", action="store_true", help="")
    parser.add_argument("--extract_all_state", action="store_true", help="Extract all state infos")
    parser.add_argument("--use_wl_embeddings", action="store_true", help="Use embeddings of workload in state, rather than TopK")
    parser.add_argument("--enhance_cluster_info", action="store_true", help="Enhance cluster infos")
    parser.add_argument("--down_data_scale", action="store_true", help="Down data scale")
    parser.add_argument("--embedding_augment", type=int, default=100, help="Augment of embedding values")
    parser.add_argument("--no_inter_value", action="store_true", help="Do not add inter data value into state")
    parser.add_argument("--no_workload", action="store_true", help="Do not add workload info into state")
    parser.add_argument("--add_weighted_ir", action="store_true", help="Add weighted ir into embedding")
    parser.add_argument("--use_sif", action="store_true", help="Use SIF Embedding")

    # for SL
    parser.add_argument("--model_name", type=str, default="sl", help="Existing model fname")
    parser.add_argument("--batch_size", type=int, default=128, help="Training batch size")
    parser.add_argument("--state_dim", type=int, default=87, help="State dimension")
    parser.add_argument("--action_dim", type=int, default=2, help="Action dimension")
    parser.add_argument("--load_model", action="store_true", help="Whether load model from disk")

    # for search
    parser.add_argument("--max_depth", type=int, default=2, help="Search depth")
    parser.add_argument("--stop_condition", type=float, default=1.0, help="When the perf delta is smaller than stop_condition%, stop the tuning")
    parser.add_argument("--allow_fine_tune", action="store_true", help="Allow fine tune the action in search model")

    # for sample
    parser.add_argument("--repeat_time", type=int, default=1, help="Repeat time of each action")

    params = parser.parse_args()
    params.log_folder = FOLDER_PREFIX + params.log_folder
    params.save_folder = FOLDER_PREFIX + params.save_folder
    params.method = params.method.lower()

    if params.method == "sample":
        params.epoches_per_workload = 1
        params.model_name = "sample"
        params.extract_all_state = True

    if params.method == "search":
        params.model_name = "search"

    return params


def init_folder_and_logger(params) -> utils.Logger:
    if not os.path.exists(params.log_folder):
        os.mkdir(params.log_folder)
    if not os.path.exists(params.save_folder):
        os.mkdir(params.save_folder)

    log_file = params.log_folder + f"/train_{params.method}.log"
    logger = utils.Logger(name="AGE", log_file=log_file, append_log=params.append_log)
    return logger


def init_model(params) -> Model:
    if params.method == "sl":
        return SLModel(SLModel._get_params(params))
    elif params.method == "search":
        return Search(Search._get_params(params), logger)
    elif params.method == "sample":
        return SampleModel(SampleModel._get_params(params), logger)
    else:
        logger.error("Unexpected model type")
        exit(0)


def new_env(params, logger: utils.Logger) -> Env:
    env_params = {}
    env_params["buf_prefix"] = params.buffer_prefix
    env_params["buf_size"] = params.buffer_size
    env_params["thpt_client"] = params.thpt_client
    # env_params["sample"] = params.sample
    # env_params["steps"] = params.max_steps
    env_params["generate_trace"] = params.generate_trace
    env_params["with_sigmoid"] = params.with_sigmoid
    env_params["no_opsize"] = params.no_opsize
    env_params["linear_action"] = params.use_linear
    env_params["use_normalized_inter_data"] = params.use_normalized_inter_data
    env_params["extract_all_state"] = params.extract_all_state
    env_params["use_wl_embeddings"] = params.use_wl_embeddings
    env_params["enhance_cluster_info"] = params.enhance_cluster_info
    env_params["down_data_scale"] = params.down_data_scale
    env_params["embedding_augment"] = params.embedding_augment
    env_params["use_inter_value"] = not params.no_inter_value
    env_params["use_workload"] = not params.no_workload
    env_params["add_weighted_ir"] = params.add_weighted_ir
    env_params["use_sif"] = params.use_sif
    return Env(env_params, logger)


def init_env(env: Env):
    print("@@@@@@@@@@@@@@@@@@@@@@@")
    print("@                     @")
    print("@ Start a new episode @")
    print("@                     @")
    print("@@@@@@@@@@@@@@@@@@@@@@@")
    return env.init_env()


def apply_action(state, action, restart=True):
    env_step_time = utils.time_start()
    next_state, inter_data_info, inner_metrics, next_server_restart = env.step(action, restart=restart)
    env_step_time = utils.time_end(env_step_time)
    logger.info(
        "[Episode: {}][Step: {}] Metrics qps: {:.2f}, lat50: {:.2f}, lat90: {:.2f}, lat95: {:.2f}, lat99: {:.2f}.".format(
            episode,
            iter_counter,
            inner_metrics.qps,
            inner_metrics.lat_p50,
            inner_metrics.lat_p90,
            inner_metrics.lat_p95,
            inner_metrics.lat_p99,
        )
    )
    logger.info(
        "[Episode: {}][Step: {}] Next state: {}".format(
            episode, iter_counter, np.array2string(RawState.extract_without_opinfo(next_state), precision=2).replace("\n", "")
        )
    )
    logger.info(
        "[Episode: {}][Step: {}] Full state: {}".format(episode, iter_counter, next_state)
    )
    logger.info(
        "[Episode: {}][Step: {}] Inter data infos: {}".format(episode, iter_counter, inter_data_info)
    )
    return next_state, inner_metrics, env_step_time, next_server_restart

def save_model(model: SLModel):
    # checkpoint condition here
    if step_counter % 5 == 0:
        model.save_model(params.save_folder, params.model_name)

def episode_exit(icounter: int) -> bool:
    # episode exit condition here
    if icounter == params.max_steps:
        logger.info(f"[Episode: {episode}] Episode exit because of max steps")
        return True
    return False

def collect_step_time(
    time: float,
    action_time: float,
    env_time: float
):
    # Pythonic static variables
    if not hasattr(collect_step_time, "times"):
        collect_step_time.times = []
        collect_step_time.env_times = []
        collect_step_time.train_times = []
        collect_step_time.action_times = []

    collect_step_time.times.append(time)
    collect_step_time.env_times.append(env_time)
    collect_step_time.action_times.append(action_time)

    logger.info(
        "[Episode: {}][Step: {}] step: {:.2f}s, env: {:.2f}s, action: {:.2f}s".format(
            episode,
            iter_counter,
            time,
            env_time,
            action_time,
        )
    )
    logger.info(
        "[Episode: {}][Step: {}][Average] step: {:.2f}s, env: {:.2f}s, train: {:.2f}s, action: {:.2f}s".format(
            episode,
            iter_counter,
            np.mean(collect_step_time.times),
            np.mean(collect_step_time.env_times),
            np.mean(collect_step_time.train_times),
            np.mean(collect_step_time.action_times),
        )
    )

if __name__ == "__main__":
    params = init_arguments()
    logger = init_folder_and_logger(params)
    logger.info(f"Params: {params}")
    model = init_model(params)
    env = new_env(params, logger)

    if isinstance(model, SampleModel):
        params.max_steps = model.get_max_steps()
        logger.info(f"[SampleModel] Max steps: {params.max_steps}")

    step_counter = 0
    for episode in range(params.epoches):
        if params.changing_workload and episode % params.epoches_per_workload == 0:
            if not env.change_workload(int(episode / params.epoches_per_workload)):
                break  # worklaod not loaded

        current_state, inner_metrics, next_server_restart = init_env(env)

        iter_counter = 0
        pass_next_choose_action = False
        while True:
            step_time = utils.time_start()
            print(f"[Episode: {episode}][Step: {iter_counter}]")

            action_time = utils.time_start()
            if pass_next_choose_action:
                pass_next_choose_action = False
            else:
                if isinstance(model, SLModel):
                    action = model.choose_action(current_state)
                elif isinstance(model, Search):
                    action, iter_counter = model.choose_action(iter_counter, current_state, inner_metrics)
                elif isinstance(model, SampleModel):
                    action, iter_counter = model.choose_action(iter_counter, current_state, inner_metrics)
            action_time = utils.time_end(action_time)

            current_state, inner_metrics, env_time, next_server_restart = apply_action(current_state, action, next_server_restart)

            if params.force_restart_server:
                next_server_restart = True

            step_time = utils.time_end(step_time)
            collect_step_time(step_time, action_time, env_time)

            step_counter += 1
            iter_counter += 1

            # implement postprocess
            if isinstance(model, SLModel):
                save_model(model)
                if step_counter % 5 == 0:
                    model.noise.decay()
            elif isinstance(model, Search):
                # if it's the last step, update the best and check whether need to go deep
                if iter_counter == params.max_steps:
                    action, new_step = model.choose_action(iter_counter, current_state, inner_metrics)
                    if new_step != iter_counter:
                        pass_next_choose_action = True
                        logger.info(f"[SearchModel] Last step go deeper to step: {new_step}")
                    iter_counter = new_step
            elif isinstance(model, SampleModel):
                pass

            if episode_exit(iter_counter):
                break

    if isinstance(model, Search) or isinstance(model, SampleModel):
        model.print_best()

    # end the systems
    env.end_env()
