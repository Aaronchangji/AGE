import utils
from collections import deque
from model import Model
from state import InnerMetrics, RawState
from env import MAX_ACTION_POW, MIN_ACTION_POW
from utils import OUProcess

class Search(Model):
    def __init__(self, params, logger : utils.Logger):
        super(Search, self).__init__()

        self.logger = logger
        self.logger.info(f"#########################################")
        self.logger.info(f"#                                       #")
        self.logger.info(f"# [SearchModel] Init Search Model #")
        self.logger.info(f"#                                       #")
        self.logger.info(f"#########################################")
        self.best_action = 0.0
        self.best_metrics = None
        self.isEnd = False

        self.depth = 0
        self.max_depth = params["max_depth"]
        self.bs_range = params["bs_range"]
        self.num_steps = params["max_step"]
        self._generate_step_len()
        self.use_noise = True
        sigma = self.step_len / 10.0
        self.oup = OUProcess(1, theta=0.15, mu=0, sigma=sigma, sigma_decay_rate=0.95)

        self.action = self.bs_range[0]
        self.original_action = self.bs_range[0]

        self.last_p99_delta = 2 ** 31 - 1
        self.last_que_lats = []
        self.last_net_lats = []
        # used to record the previous and pre-previous metrics and actions
        self.last_metrics = deque(maxlen=2)
        self.last_actions = deque(maxlen=2)

        self.retry = 0
        self.max_retry = 2

        # when the delta is smaller than self.stop_condition%, stop the tuning
        self.stop_condition = params["stop_condition"]

    @staticmethod
    def _get_params(params) -> dict:
        heu_params = {}
        # heu_params["mem_size"] = params.repmem_size
        # heu_params["model_dir"] = params.save_folder
        # heu_params["model_name"] = params.model_name
        heu_params["max_step"] = params.max_steps
        heu_params["max_depth"] = params.max_depth
        heu_params["bs_range"] = [0.0, 1.0]
        heu_params["stop_condition"] = params.stop_condition
        print(f"[SearchModel] Search Params: {heu_params}")
        return heu_params

    def _generate_step_len(self):
        steps = self.num_steps
        if self.depth == 0:
            steps -= 1  # the first depth should search the begin as well

        self.step_len = (self.bs_range[1] - self.bs_range[0]) / steps
        self.logger.info(f"[SearchModel] New step Length: {self.step_len}")

    def update_meta(self, state, metrics):
        self.set_last_lats(state)
        self.last_metrics.append(metrics)

    def reset_meta(self):
        self.last_p99_delta = 2 ** 31 - 1
        self.last_que_lats = []
        self.last_net_lats = []
        self.last_actions[-1] = self.last_actions[0]
        self.last_metrics[-1] = self.last_metrics[0]

    def set_last_lats(self, state):
        self.last_que_lats = RawState.extract_queue_lat(state)
        self.last_net_lats = RawState.extract_net_lat(state)

    def calc_delta(self, previous, current):
        return float(current - previous) / previous

    def update_best(self, action, metrics):
        if self.best_metrics is None:
            self.best_action = action
            self.best_metrics = metrics
            return

        p99_delta = self.calc_delta(self.best_metrics.lat_p99, metrics.lat_p99)
        self.logger.info(f"[SearchModel] previous best p99: {self.best_metrics.lat_p99}, current p99: {metrics.lat_p99}, delta: {p99_delta}")
        if p99_delta < 0:
            self.best_action = action
            self.best_metrics = metrics

    def reset_retry(self):
        self.retry = 0

    def get_new_action(self, append_action=True):
        if append_action:
            self.last_actions.append(self.action)

        noise = 0
        if self.use_noise and self.depth == 0:  # add noise for first step for more exploration
            noise = self.oup.noise()[0]
            self.logger.info(f"[SearchModel] noise: {noise}")
        self.original_action += self.step_len
        self.action = self.original_action + noise
        if self.action < 0:
            self.action = 0.0
        if self.action > 1.0:
            self.action = 1.0

    def go_deeper(self):
        self.bs_range = [self.last_actions[0], self.action]
        if self.bs_range[0] == self.bs_range[1]:
            self.logger.info(f"[SearchModel] Try to go deeper with same border [{self.bs_range[0]}, {self.bs_range[1]}]")
            self.isEnd = True
            return

        self._generate_step_len()
        sigma = self.step_len / 10.0
        self.oup.reset(sigma=sigma)

        self.reset_meta()
        self.logger.info(f"[SearchModel] Go deeper with range: [{self.bs_range[0]}, {self.bs_range[1]}]")
        self.depth += 1
        self.original_action = self.bs_range[0]
        self.action = self.bs_range[0]
        self.get_new_action(False)

    def choose_action(self, step, state, metrics: InnerMetrics) -> list:
        self.print_meta()
        if self.depth == 0:
            # the first
            if step == 0:  # only return step w/o analyse
                return [self.action], step
            elif step == 1:  # there is only one action, wait for two to analyse
                self.update_meta(state, metrics)
                self.update_best(self.action, metrics)
                self.get_new_action()
                return [self.action], step
        else:
            if step == 0:
                # when depth is not 0, there is already one action, need one more to analyze
                self.update_meta(state, metrics)
                self.update_best(self.action, metrics)
                self.get_new_action()
                return [self.action], step

        # normal analysis the adjust the action
        self.update_best(self.action, metrics)

        p99_delta = self.calc_delta(self.last_metrics[-1].lat_p99, metrics.lat_p99)
        self.logger.info(f"[SearchModel] p99 delta: {p99_delta}")

        if p99_delta < 0 or len(self.last_metrics) == 1:
            if (100 * abs(p99_delta)) < self.stop_condition:
                # the performance change is less than stop_condition%, no need to adjust
                self.logger.info(f"[SearchModel] p99 delta {p99_delta} is less than {self.stop_condition}%, stop tuning")
                self.isEnd = True

            self.get_new_action()
        else:
            if p99_delta > 1 and self.depth > 0 and self.retry <= self.max_retry:
                self.logger.info(f"[SearchModel] p99 delta is more than 100% ({metrics.lat_p99} v.s. {self.last_metrics[-1].lat_p99}), rerun ({self.retry+1}) to check")
                self.retry += 1
                return [self.action], step - 1

            # check the thpt
            thpt = metrics.qps
            instant_thpt = metrics.instant_qps
            if thpt < instant_thpt * 0.8:
                # still need to amplify the action to increase the thpt
                self.logger.info(f"Current Que Lat: {RawState.extract_queue_lat(state)}, and last : {self.last_que_lats}")
                self.logger.info(f"Current Net Lat: {RawState.extract_net_lat(state)}, and last : {self.last_net_lats}")
                self.get_new_action()
            else:
                # find the optimal range, go deeper
                if self.depth < self.max_depth:
                    self.go_deeper()
                    step = 0
                    return [self.action], step
                else:
                    self.isEnd = True

        if self.isEnd:
            self.print_best()
            exit(0)

        self.update_meta(state, metrics)
        self.reset_retry()
        return [self.action], step

    def print_best(self):
        real_batch_size = 2 ** (self.best_action * (MAX_ACTION_POW - MIN_ACTION_POW) + MIN_ACTION_POW)
        self.logger.info(f"[SearchModel] Tuning is end. Best Batch Size: {real_batch_size}, Best Metrics: {self.best_metrics}")

    def print_meta(self):
        self.logger.info(f"[SearchModel] Depth: {self.depth}, Action: {self.action}")
        if len(self.last_actions) > 0:
            self.logger.info(f"[SearchModel] LLast Action: {self.last_actions[0]}; LLast Metrics: {self.last_metrics[0]}")
            self.logger.info(f"[SearchModel]  Last Action: {self.last_actions[-1]};  Last Metrics: {self.last_metrics[-1]}")
        self.logger.info(f"[SearchModel] BS Range: {self.bs_range}, step_length: {self.step_len}")