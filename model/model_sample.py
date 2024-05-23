import utils
from model import Model
from state import RawState, InnerMetrics
import env

# This class is used as the baseline
class SampleModel(Model):
    def __init__(self, params, logger : utils.Logger):
        super(SampleModel, self).__init__()

        self.logger = logger
        self.logger.info(f"###################################")
        self.logger.info(f"#                                 #")
        self.logger.info(f"# [SampleModel] Init Sample Model #")
        self.logger.info(f"#                                 #")
        self.logger.info(f"###################################")

        self.num_step = params["max_step"]
        self.num_ms_step = params["max_ms_step"]
        self.repeat_time = params["max_repeat_time"]
        self.repeat = 0
        self.sample_start = 0.0
        self.sample_end = 1.0
        self._prepare_actions()
        # self._prepare_fixed_actions()

        self.best_action = []
        self.best_metrics = None
        self.last_que_lats = []
        self.last_net_lats = []

    def _prepare_fixed_actions(self):
        self.actions = []

        rbs = [13, 16, 18, 14]
        rms = [10, 11, 12, 14]

        for i in range(len(rbs)):
            bs = (rbs[i] - env.MIN_ACTION_POW) / (env.MAX_ACTION_POW - env.MIN_ACTION_POW)
            ms = (rms[i] - env.MIN_MSG_POW) / (env.MAX_MSG_POW - env.MIN_MSG_POW)
            realbs = env.Env.get_action(bs, env.MAX_ACTION_POW, env.MIN_ACTION_POW)
            realms = env.Env.get_action(ms, env.MAX_MSG_POW, env.MIN_MSG_POW)
            self.logger.info(f"[SampleModel] bs: {realbs}, ms: {realms}")
            self.actions.append([bs, ms])

        self.logger.info(f"[SampleModel] sampled actions: {self.actions}")
        self.logger.info(f"[SampleModel] number of sampled actions: {len(self.actions)}")

    def _prepare_actions(self):
        self.actions = []
        step_len = (self.sample_end - self.sample_start) / (self.num_step - 1)
        ms_step_len = (self.sample_end - self.sample_start) / (self.num_ms_step - 1)
        for i in range(self.num_step):
            bs_added = False
            # bs = 0.5
            bs = i * step_len + self.sample_start
            if bs > 1.0:
                bs = 1.0
            real_bs = env.Env.get_action(bs, env.MAX_ACTION_POW, env.MIN_ACTION_POW)

            for j in range(self.num_ms_step):
                # ms = 0.25
                ms = j * ms_step_len + self.sample_start
                if ms > 1.0:
                    ms = 1.0
                real_ms = env.Env.get_action(ms, env.MAX_MSG_POW, env.MIN_MSG_POW)

                self.logger.info(f"[SampleModel] bs: {real_bs}, ms: {real_ms}")
                self.actions.append([bs, ms])

        self.logger.info(f"[SampleModel] sampled actions: {self.actions}")
        self.logger.info(f"[SampleModel] number of sampled actions: {len(self.actions)}")

    def get_max_steps(self):
        return len(self.actions)

    def choose_action(self, step, state, metrics: InnerMetrics):
        if step != 0:
            self.update_best(self.actions[step-1], metrics)
        # metrics
        p50 = metrics.lat_p50
        p99 = metrics.lat_p99
        qps = metrics.qps
        instant_qps = metrics.instant_qps

        self.logger.info(f"[SampleModel] p50: {p50}, p99: {p99}, qps: {qps}, instant_qps: {instant_qps}")

        heavy_ratio = state[1]

        req_thpt = metrics.req_thpt
        byte_thpt = metrics.byte_thpt
        heavy_req_thpt = metrics.heavy_req_thpt
        heavy_byte_thpt = metrics.heavy_byte_thpt

        heavy_byte_ratio = 100 * heavy_byte_thpt / byte_thpt
        heavy_req_ratio = 100 * heavy_req_thpt / req_thpt

        self.logger.info(f"[SampleModel] heavy query ratio: {heavy_ratio}, heavy byte ratio: {heavy_byte_ratio}, heavy req ratio: {heavy_req_ratio}")

        # inner latencies
        queue_lats = RawState.extract_queue_lat(state)
        net_lats = RawState.extract_net_lat(state)
        queue_lat_p50 = queue_lats[2]
        queue_lat_p99 = queue_lats[3]
        net_lat_p50 = net_lats[2]
        net_lat_p99 = net_lats[3]

        self.logger.info(f"[SampleModel] queue lat p50: {queue_lat_p50}, queue lat p99: {queue_lat_p99}, net lat p50: {net_lat_p50}, net lat p99: {net_lat_p99}")

        if len(self.last_que_lats) != 0:
            queue_lat_p50_delta = 100.0 * float(queue_lat_p50 - self.last_que_lats[2]) / self.last_que_lats[2]
            queue_lat_p99_delta = 100.0 * float(queue_lat_p99 - self.last_que_lats[3]) / self.last_que_lats[3]
            net_lat_p50_delta = 100.0 * float(net_lat_p50 - self.last_net_lats[2]) / self.last_net_lats[2]
            net_lat_p99_delat = 100.0 * float(net_lat_p99 - self.last_net_lats[3]) / self.last_net_lats[3]
            self.logger.info(f"[SampleModel] queue lat p50 delta: {queue_lat_p50_delta}, queue lat p99 delta: {queue_lat_p99_delta}, net lat p50 delta: {net_lat_p50_delta}, net lat p99 delta: {net_lat_p99_delat}")

        # update the parameters
        self.set_last_lats(state)

        if self.repeat < self.repeat_time:
            step -= 1
            step = max(step, 0)
        else:
            self.repeat = 0

        self.logger.info(f"[SampleModel][Step {step}][Repeat {self.repeat}] action: {self.actions[step]}")
        self.repeat += 1
        return self.actions[step], step

    @staticmethod
    def _get_params(params) -> dict:
        _params = {}
        _params["max_step"] = params.max_steps
        _params["max_ms_step"] = params.max_ms_steps
        _params["max_repeat_time"] = params.repeat_time
        return _params

    def set_last_lats(self, state):
        self.last_que_lats = RawState.extract_queue_lat(state)
        self.last_net_lats = RawState.extract_net_lat(state)

    def update_best(self, action, metrics):
        if self.best_metrics is None:
            self.best_action = action
            self.best_metrics = metrics
            return

        if metrics.lat_p99 < self.best_metrics.lat_p99:
            self.best_action = action
            self.best_metrics = metrics

    def print_best(self):
        if self.best_action == []:
            self.logger.info(f"[SampleModel] No best action is found")
            return
        real_bs = env.Env.get_action(self.best_action[0], env.MAX_ACTION_POW, env.MIN_ACTION_POW)
        real_ms = env.Env.get_action(self.best_action[1], env.MAX_ACTION_POW, env.MIN_ACTION_POW, use_linear=True)
        self.logger.info(f"[SampleModel] Sampling is done. Best Batch Size: [bs:{real_bs}, ms:{real_ms}], Best Metrics: {self.best_metrics}")
