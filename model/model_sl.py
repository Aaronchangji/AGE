import os
import pickle
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optimizer
from torch.autograd import Variable
from model import Model
from utils import OUProcess
from env import MIN_ACTION_POW, MAX_ACTION_POW

class Nomarlizer(object):
    def __init__(self, mean, variance):
        if isinstance(mean, list):
            mean = np.array(mean)
        if isinstance(variance, list):
            variance = np.array(variance)
        self.mean = mean
        self.std = np.sqrt(variance + 0.00001)

    def normalize(self, x):
        if isinstance(x, list):
            x = np.array(x)
        x = x - self.mean
        x = x / self.std

        return Variable(torch.FloatTensor(x))

    def __call__(self, x, *args, **kwargs):
        return self.normalize(x)

class SLActor(nn.Module):
    def __init__(self, state_dim, action_dim):
        super(SLActor, self).__init__()

        self.layers = nn.Sequential(
            nn.Linear(state_dim, 256),
            nn.LeakyReLU(negative_slope=0.2),
            nn.BatchNorm1d(256),
            nn.Linear(256, 256),
            nn.Tanh(),
            nn.Dropout(0.3),
            nn.Linear(256, 64),
            nn.Tanh(),
            nn.BatchNorm1d(64),
        )
        self.out = nn.Linear(64, action_dim)

        self._init_weight()
        self.act = nn.Sigmoid()

    def _init_weight(self):
        for m in self.layers:
            if type(m) == nn.Linear:
                m.weight.data.normal_(0.0, 1e-2)
                m.bias.data.uniform_(-0.1, 0.1)

    def forward(self, state):
        out = self.act(self.out(self.layers(state)))
        return out

class SLModel(Model):
    def __init__(self, params, ouprocess=True, mean_var_path=None):
        self.state_dim = params["state_dim"]
        self.action_dim = params["action_dim"]

        self.batch_size = params["batch_size"]
        self.alr = params["alr"]
        self.clr = params["clr"]
        self.gamma = params["gamma"]
        self.tau = params["tau"]
        self.do_load_model = params["load_model"]

        if mean_var_path is None:
            mean = np.zeros(self.state_dim)
            var = np.zeros(self.state_dim)
        elif not os.path.exists(mean_var_path):
            mean = np.zeros(self.state_dim)
            var = np.zeros(self.state_dim)
        else:
            with open(mean_var_path, "rb") as f:
                mean, var = pickle.load(f)

        self.normalizer = Nomarlizer(mean, var)
        self._build_actor()

        self.noise = OUProcess(self.action_dim)
        super(SLModel, self).__init__()

    def _build_actor(self):
        self.actor = SLActor(self.state_dim, self.action_dim)
        self.actor_criterion = nn.MSELoss()
        self.actor_optimizer = optimizer.Adam(lr=self.alr, params=self.actor.parameters())

    @staticmethod
    def _get_params(params) -> dict:
        ddpg_params = {}
        ddpg_params["state_dim"] = params.state_dim
        ddpg_params["action_dim"] = params.action_dim
        ddpg_params["mem_size"] = params.repmem_size
        ddpg_params["model_dir"] = params.save_folder
        ddpg_params["model_name"] = params.model_name
        ddpg_params["batch_size"] = params.batch_size
        ddpg_params["load_model"] = params.load_model
        ddpg_params["alr"] = 0.005
        ddpg_params["clr"] = 0.0001
        ddpg_params["tau"] = 0.002
        ddpg_params["gamma"] = 0.99
        return ddpg_params

    @staticmethod
    def _update_target(target, source, tau):
        for target_param, param in zip(target.parameters(), source.parameters()):
            target_param.data.copy_(target_param.data * (1 - tau) + param.data * tau)

    @staticmethod
    def totensor(x):
        # list -> array -> tensor
        return Variable(torch.FloatTensor(np.array(x)))

    def choose_action(self, x):
        self.actor.eval()
        act = self.actor(self.normalizer([x.tolist()])).squeeze(0)
        self.actor.train()
        action = act.data.numpy()
        # action += self.noise.noise()  # apply noise
        return action.clip(0, 1)

    def load_model(self, model_dir, model_name):
        self.load_actor("{}/{}_actor".format(model_dir, model_name))

    def save_model(self, model_dir, model_name):
        self.save_actor("{}/{}_actor".format(model_dir, model_name))

    def save_actor(self, path):
        torch.save(self.actor.state_dict(), path)

    def load_actor(self, path):
        try:
            self.actor.load_state_dict(torch.load(path))
            print("Actor loaded from {}".format(path))
        except FileNotFoundError:
            print("{} Actor NOT found hence not loaded".format(path))

    def reset(self, sigma):
        self.noise.reset(sigma)

    def _normalize_action_when_training(self, action):
        _ret = []
        for ac in action:
            _ret.append((np.log2(ac) - MIN_ACTION_POW) / (MAX_ACTION_POW - MIN_ACTION_POW))
        print(f"{action} and {_ret}")
        return _ret

    def train_actor(self, batch_data, is_train=True, isprint=False):
        states, action = batch_data
        ofname = "output.txt"
        if is_train:
            self.actor.train()
            pred = self.actor(self.normalizer(states))
            action = self.totensor(action)

            if isprint:
                with open(ofname, "a+") as fp:
                    fp.write("Pred: {}\n".format(pred))
                    fp.write("Label: {}\n".format(action))

            _loss = self.actor_criterion(pred, action)

            self.actor_optimizer.zero_grad()
            _loss.backward()
            self.actor_optimizer.step()

        else:
            self.actor.eval()
            pred = self.actor(self.normalizer(states))
            action = self.totensor(action)

            if isprint:
                with open(ofname, "a+") as fp:
                    fp.write("Pred: {}\n".format(pred))
                    fp.write("Label: {}\n".format(action))

            _loss = self.actor_criterion(pred, action)

        return _loss.data.item()

