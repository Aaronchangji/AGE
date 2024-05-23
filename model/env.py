import os
import re
import subprocess
import time
import numpy as np
from simple_buffer import SimpleBuffer
from state import RawState, InnerMetrics
import utils
import struct

AGE_ROOT = os.environ["AGE_ROOT"]
# for serve
MIN_ACTION_POW = 9
MAX_ACTION_POW = 21
MIN_MSG_POW = 9
MAX_MSG_POW = 17

# for search & sample
# MIN_ACTION_POW = 11
# MAX_ACTION_POW = 20
# MIN_MSG_POW = 10
# MAX_MSG_POW = 15

ACTION_ALPHA = 0.95
ACTION_BETA = utils.desigmoid(ACTION_ALPHA)

class Env:
    def __init__(self, params: dict, logger: utils.Logger):
        self.buf = SimpleBuffer(params["buf_prefix"], params["buf_size"])
        self.init_metrics = None
        self.last_metrics = None
        self.logger = logger

        self.thpt_client = params["thpt_client"]

        self.qps_weight = 0.1
        self.lat_p50_weight = 0.2
        self.lat_p99_weight = 0.8

        self.score = 0.0
        self.steps = 0
        self.terminate = False

        self.generate_trace = params["generate_trace"]
        self.no_restart_count = 0
        self.read_worker_list()

        self.with_sigmoid = params["with_sigmoid"]
        self.linear_action = params["linear_action"]
        # self.linear_action = False

        self.no_opsize = params["no_opsize"]
        self.use_normalized_inter_data = params["use_normalized_inter_data"]
        self.extract_all_state = params["extract_all_state"]
        self.use_wl_embeddings = params["use_wl_embeddings"]
        self.enhance_cluster_info = params["enhance_cluster_info"]
        self.down_data_scale = params["down_data_scale"]
        if self.use_wl_embeddings:
            self.down_data_scale = True
        self.embedding_augment = params["embedding_augment"]
        self.use_inter_value = params["use_inter_value"]
        self.use_workload = params["use_workload"]
        self.add_weighted_ir = params["add_weighted_ir"]
        self.use_sif = params["use_sif"]

        self.logger.info(f"[ENV] alpha: {ACTION_ALPHA:.3f}, beta: {ACTION_BETA:.3f}")

    def set_last_metrics(self, m):
        self.last_metrics = m

    def set_init_metrics(self, m):
        self.last_metrics = m
        self.init_metrics = m

    def notify_system(self):
        self.buf.notify()

    def get_state(self):
        state_data = self.buf.read()
        rs = RawState()
        rs.set_from_bytes(state_data)
        inter_data_info = []
        rs.get_all_inter_data_infos(inter_data_info)
        opsize = not self.no_opsize
        cur_state = []
        if self.extract_all_state:
            cur_state = rs.extract_all_state()
        else:
            cur_state = rs.extract_state(opsize=opsize, use_normalized_inter_data=self.use_normalized_inter_data,
                                         use_embeddings=self.use_wl_embeddings, enhance_cluster_info=self.enhance_cluster_info, down_data_scale=self.down_data_scale,
                                         embaug=self.embedding_augment, use_inter_value=self.use_inter_value, use_workload=self.use_workload,
                                         add_weighted_ir=self.add_weighted_ir, use_sif=self.use_sif)
        return cur_state, rs.inner_metrics, inter_data_info, rs.retry

    @staticmethod
    def _bs2opt(x, max_pow, min_pow):
        logx = np.log2(x)
        normalizedx = (logx - min_pow) / (max_pow - min_pow)
        if logx > max_pow:
            return utils.sigmoid(ACTION_BETA * normalizedx)
        else:
            return ACTION_ALPHA * normalizedx

    @staticmethod
    def _opt2bs(x, max_pow, min_pow):
        if x > ACTION_ALPHA:
            normalizedx = utils.desigmoid(x) / ACTION_BETA
        else:
            normalizedx = x / ACTION_ALPHA
        logx = normalizedx * (max_pow - min_pow) + min_pow
        return round(2 ** logx)

    @staticmethod
    def get_action(raw_action, max, min, use_linear=False, with_sigmoid=False):
        action = raw_action
        if with_sigmoid:
            # use reverse sigmoid to obtain the action
            action = Env._opt2bs(action, max, min)
            return action

        if use_linear:
            action = action * (2 ** max - 2 ** min) + 2 ** min
            action = round(action)
        else:
            action = action * (max - min) + min
            action = round(2**action)

        return action

    def send_action(self, raw_action, separate_action=True):
        bs = self.get_action(raw_action[0], MAX_ACTION_POW, MIN_ACTION_POW, use_linear=self.linear_action)
        ms = bs
        if separate_action:
            assert len(raw_action) == 2, "The length of the raw action should be 2"
            ms = self.get_action(raw_action[1], MAX_MSG_POW, MIN_MSG_POW)
        action = [bs, ms]
        self.logger.info(f"[ENV] ===========================================================================")
        self.logger.info(f"[ENV] Got raw action {raw_action}, write the action {action} back to the system")

        action_bytes = struct.pack("QQ", action[0], action[1])
        self.buf.write(action_bytes, 16)

    def send_fake_action(self):
        fake_action = [0, 0]
        action_bytes = struct.pack("QQ", fake_action[0], fake_action[1])
        self.buf.write(action_bytes, 16)

    def step(self, action=None, random_seed=False, restart=True):
        # first restart the system
        if restart:
            self.logger.info("[ENV] Restart the system")
            self.no_restart_count = 0
            self.restart_server()
        else:
            if self.check_memory_usgae():
                self.no_restart_count += 1
                self.logger.info(f"[ENV] {self.no_restart_count} no restart")
            else:
                self.logger.info("[ENV] Memory usage too high, restart the system")
                self.no_restart_count = 0
                self.restart_server()

        print(f"Action: {action}")
        is_init = action is None

        # send last action to system
        if is_init:
            self.send_fake_action()
        else:
            self.send_action(action)

        # wait for system to apply
        self.buf.wait_for_system()

        # start the thpt test
        self.activate_thpt_test(random_seed)

        # wait for the state
        next_state_raw, current_inner_metrics, inter_data_info, retry = self.get_state()

        retry_cnt = 0
        if retry != 0:
            # retry this action
            while (retry != 0):
                retry_cnt += 1
                if retry_cnt >= 3:
                    # skip this action
                    break

                self.logger.info(f"[ENV] Retry {retry_cnt}: Restart the system")
                self.no_restart_count = 0
                self.restart_server()
                if is_init:
                    self.send_fake_action()
                else:
                    self.send_action(action)
                self.buf.wait_for_system()
                self.activate_thpt_test(random_seed)
                next_state_raw, current_inner_metrics, inter_data_info, retry = self.get_state()

        next_state = next_state_raw[:-1]
        next_server_restart = True
        self.logger.info("[ENV] State dim: {}".format(len(next_state)))
        if next_state_raw[-1] == 0:
            next_server_restart = False
        self.logger.info("[ENV] Unfinished queries: {}".format(next_state_raw[-1]))
        self.logger.info("[ENV] Current metrics: {}".format(str(current_inner_metrics)))
        if not is_init:
            return next_state, inter_data_info, current_inner_metrics, next_server_restart
        else:
            return next_state, inter_data_info, current_inner_metrics, next_server_restart

    @staticmethod
    def check_server_status():
        restart_cmd = "bash " + AGE_ROOT + "scripts/server_status.sh " + AGE_ROOT
        result = subprocess.run(
            restart_cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        flag = True
        if result.returncode == 0:
            output = result.stdout
            tokens = output.strip().split("\n")
            for l in tokens:
                t = l.strip().split(" ")
                if t[-2] == "not":
                    flag = False
                    break
        else:
            flag = False

        return flag

    def activate_thpt_test(self, random_seed=True):
        activate_thpt_cmd = (
            AGE_ROOT + "/scripts/activate-thpt.sh " + self.thpt_client + " " + str(random_seed) + " " + AGE_ROOT
        )
        print(f"Activate throughput client with command: {activate_thpt_cmd}")
        with open(os.devnull, "wb") as devnull:
            subprocess.check_call(["bash", "-c", activate_thpt_cmd], stdout=devnull, stderr=subprocess.STDOUT)

    def deactivate_thpt_test(self):
        kill_cmd = AGE_ROOT + "/scripts/deactivate-thpt.sh " + self.thpt_client
        with open(os.devnull, "wb") as devnull:
            subprocess.check_call(["bash", "-c", kill_cmd], stdout=devnull, stderr=subprocess.STDOUT)

    def restart_server(self):
        # stop the server first
        stop_cmd = "bash " + AGE_ROOT + "/scripts/stop-servers.sh " + AGE_ROOT
        print(f"Stop the system with command: {stop_cmd}")
        os.system(stop_cmd)

        # reset all semaphores
        self.buf.reset_all_sem()
        time.sleep(1)  # wait for memory to be released if something bad happens

        # start the server
        start_cmd = "bash " + AGE_ROOT + "/scripts/start-servers-w-model.sh " + AGE_ROOT
        print(f"Start the system with command: {start_cmd}")
        os.system(start_cmd)

        # wait for the start of the server
        # Master from the server will notify the model
        print(f"Waiting for system")
        self.buf.wait_for_system()
        print(f"System is Up")

    def init_env(self):
        current_state = None
        inner_metrics = None
        self.score = 0.0
        self.terminate = False

        # Stop the thpt test client if there is
        print("Kill throughput client first")
        self.deactivate_thpt_test()

        current_state, _, inner_metrics, next_server_restart = self.step(action=None, random_seed=self.generate_trace)
        self.logger.info("[ENV] Init metrics: {}".format(str(inner_metrics)))
        self.logger.info("[ENV] Init state: {}".format(np.array2string(current_state[0:12], precision=2).replace("\n", "")))
        self.set_init_metrics(inner_metrics)

        return current_state, inner_metrics, next_server_restart

    def end_env(self):
        # Stop the thpt test client if there is
        print("Kill throughput client first")
        self.deactivate_thpt_test()
        # stop the server first
        stop_cmd = "bash " + AGE_ROOT + "/scripts/stop-servers.sh " + AGE_ROOT
        print(f"Stop the system with command: {stop_cmd}")
        os.system(stop_cmd)
        # reset all semaphores
        self.buf.reset_all_sem()

    def change_workload(self, idx) -> bool:
        workload_fn = AGE_ROOT + f"/model/workloads/workload_{idx}.conf"
        if not os.path.exists(workload_fn):
            self.logger.error(f"[ENV] Workload {workload_fn} does not exist")
            return False

        self.logger.info(f"[ENV] Load workload {workload_fn}")
        load_cmd = "cp " + workload_fn + " " + AGE_ROOT + "/throughput_test.conf"
        os.system(load_cmd)
        return True

    def read_worker_list(self):
        fn = AGE_ROOT + "/config/worker_nodes.txt"
        self.worker_list = []
        with open(fn, "r") as fp:
            lines = fp.readlines()
            for line in lines:
                if line[0] == "[":
                    continue
                ip = re.split(":", line.rstrip())[0]
                self.worker_list.append(ip)

        self.logger.info(f"[ENV] Worker list: {self.worker_list}")

    def check_memory_usgae(self) -> bool:
        for ip in self.worker_list:
            # Define the command
            command = "ssh " + ip + " 'ps aux | grep sshAGE | grep -v grep | awk \"{print $4}\"'"

            # Execute the command
            process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
            try:
                # Wait for command to complete, with timeout
                output, error = process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                # If the command doesn't complete within 5 seconds, terminate it
                process.terminate()
                output, error = process.communicate()
                return False

            # The output will be a byte string, decode it to a regular string
            memory_usage = output.decode('utf-8').strip().split()[3]

            if float(memory_usage) > 50:
                self.logger.info(f"[ENV] {ip} Memory usage: {memory_usage}")
                return False

        return True
