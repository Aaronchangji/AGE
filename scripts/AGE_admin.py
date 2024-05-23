#!/usr/bin/env python3

import subprocess
import argparse
import sys
import os
from config_util import GetNodeList

age_root = os.getenv("AGE_ROOT")

if age_root == None:
    print("Environment variable AGE_ROOT should be set!")
    exit(1)

parser = argparse.ArgumentParser(description="AGE distributed server launching")
parser.add_argument('-c', '--config', action='store', metavar='CONFIG', type=str, default=age_root + '/config/conf.ini', help='config file path, default value: $AGE_ROOT/config/conf.ini')
parser.add_argument('-w', '--worker', action='store', metavar='WORKER_NODES', type=str, default=age_root + '/config/worker_nodes.txt', help='worker nodes file path, default value: ${AGE_ROOT}/config/worker_nodes.txt')
parser.add_argument('-m', '--master', action='store', metavar='MASTER_NODES', type=str, default=age_root + '/config/master_nodes.txt', help='worker nodes file path, default value: ${AGE_ROOT}/config/master_nodes.txt')
parser.add_argument('-t', '--thpt_conf', action='store', metavar='THROUPUT_TEST_CONFIG_FILE', type=str, default='throughput_test.conf', help='throughput test config file, defulat value: ${AGE_ROOT}/throughput_test.conf')
parser.add_argument('-a', '--action', action='store', metavar='[start stop status]', type=str, default='', help='admin action')
parser.add_argument('-i', '--index', action='store', metavar='AUTO INDEX BUILD', type=bool, default=False, help='whether automatically build the index for auto thpt test')
parser.add_argument('-rl', '--rlmodel', action='store', metavar='Whether RL model is used', type=bool, default=False, help='whether RL model is used')

# ensure action is in [start, stop, status]
def CheckAction(actionStr):
    if not actionStr.lower() in ['start', 'stop', 'status', 'thpt']:
        print('[AGE-admin]: wrong action, available actions: "start", "stop", "status", "thpt"')
        return False
    return True

def CreateRemoteLogDir(address):
    cmd = 'ssh {addr} "mkdir -p {froot}/logs"'.format(addr = address, froot = age_root)
    os.system(cmd)

def LogOutputRedirect(rk, role):
    logOutputRedirect = '> {froot}/logs/{role}_{rank}.log 2>&1 &'.format(froot = age_root, rank = rk, role = role)
    return logOutputRedirect

def StartServerCommandArgs(args, rk, role):
    local_config = args.config
    if role != "master":
        local_config = local_config + "." + role
    cmdArgs = '--config={config} --worker={worker} --rank={rank} --remark=sshAGE_{role}_'. \
                    format(rank = rk, config = local_config, worker = args.worker, role = role)

    if role.lower() == "master":
        cmdArgs += ' --master={master} --rlmodel={rlmodel}'.format(master = args.master, rlmodel = args.rlmodel)

    if role.lower() == "cache" or role.lower() == "compute":
        cmdArgs += ' --index={idx}'.format(idx = args.index)

    #print(cmdArgs)
    return cmdArgs

def RemotePsCommandArgs(args, rk, role):
    local_config = args.config
    if role != "master":
        local_config = local_config + "." + role

    cmdArgs = '--config={config} --worker={worker} --rank={rank} --remark=sshAGE_{role}_'. \
                    format(rank = rk, config = local_config, worker = args.worker, role = role)

    if role.lower() == "master":
        cmdArgs += ' --master={master}'.format(master = args.master)

    return cmdArgs

def ConvertToGrepString(originalString):
    ret = originalString.replace("--", "\--")
    return ret


def GetRemotePsResult(address, args, rk, role):
    grepStr = ConvertToGrepString(RemotePsCommandArgs(args = args, rk = rk, role = role))
    cmd = 'ssh {addr} ps aux | grep "{grepStr}"'.format(addr = address, grepStr = grepStr)
    #print('ps cmd: {}'.format(cmd))
    psResult = subprocess.getoutput(cmd)
    return psResult

def ConvertConfigPath(args):
    args.config = os.path.abspath(args.config)
    args.worker = os.path.abspath(args.worker)
    args.master = os.path.abspath(args.master)

def BinaryAbsPath(role):
    binaryName = "ComputeWorker" if role.lower() == "compute" else \
                 "CacheWorker"   if role.lower() == "cache"   else \
                 "Master"

    binary_abspath = "{froot}/bin/{binary}".format(froot = age_root, binary = binaryName)
    return binary_abspath

def Start(rank, address, args, role):
    binary_abspath = BinaryAbsPath(role)
    CreateRemoteLogDir(address)
    cmd = 'ssh {addr} "{binary} {cmdArgs} {logRedirect}"'. \
        format(addr = address, binary = binary_abspath, cmdArgs = StartServerCommandArgs(args, rank, role), logRedirect = LogOutputRedirect(rk = rank, role = role))

    print('Running command ' + cmd)
    os.system(cmd)
    print('[AGE-admin] {role} Worker {rank} started, log file at {addr}:{froot}/logs/{role}_{rank}.log'. \
            format(addr = address, froot = age_root, rank = rank, role = role))

def Stop(rank, address, args, role):
        psResult = GetRemotePsResult(address = address, args = args, rk = rank, role = role)
        if psResult == '':
            print('[AGE-admin] {role} worker {rank} on {address} not running'.format(role = role, rank = rank, address = address))
            return

        # split by newline
        psResultSp = psResult.split('\n')

        # any item in psREsultSp be like:
        for psLn in psResultSp:
            # split and remove extra space
            lnsp = [sp for sp in psLn.split(' ') if sp]

            # second place is pid
            pid = lnsp[1]
            killCmd = "ssh {addr} kill -s 9 {pid}".format(addr = address, pid = pid)
            os.system(killCmd)
            print('[AGE-admin] {role} worker {rank} on {addr} stopped'.format(role = role, rank = rank, addr = address))

def ThptTest(args):
    cmd = '{froot}/bin/AGE_throughput_test --config={config} --test_config={test_config}'. \
        format(froot = age_root, config = args.config, test_config = args.thpt_conf)

    print('Running command: ' + cmd)
    os.system(cmd)

def Action(rank, address, role, args, action):
    if action == 'start':
        Start(rank = rank, address = address, args = args, role = role)
    elif action == 'stop':
        Stop(rank = rank, address = address, args = args, role = role)
    elif action == 'status':
        psResult = GetRemotePsResult(address = address, args = args, rk = rank, role = role)
        if psResult == '':
            print('[AGE-admin] {role} nodes {rank} on {addr} not running'.format(role=role, rank=rank, addr=address))
        else:
            print('[AGE-admin] {role} nodes {rank} on {addr} is running'.format(role=role, rank=rank, addr=address))

def main():
    args = parser.parse_args()
    ConvertConfigPath(args)

    # Check
    if not CheckAction(args.action):
        return

    if args.action == 'thpt':
        ThptTest(args = args)
        return

    # Get nodes list by role
    masterNodes = GetNodeList(args.master, "master")
    computeNodes = GetNodeList(args.worker, "compute")
    cacheNodes = GetNodeList(args.worker, "cache")

    for rank, address in enumerate(masterNodes):
        Action(rank = rank, address = address, args = args, role = "master", action = args.action)

    for rank, address in enumerate(computeNodes):
        Action(rank = rank, address = address, args = args, role = "compute", action = args.action)

    for rank, address in enumerate(cacheNodes):
        Action(rank = rank, address = address, args = args, role = "cache", action = args.action)


if __name__ == "__main__":
    main()
