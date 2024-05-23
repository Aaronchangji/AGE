#!/usr/bin/env python3

import argparse
import configparser
import os
from config_util import GetNodeList

age_root = os.getenv("AGE_ROOT")
if age_root == None:
    print("Environment variable AGE_ROOT should be set!")
    exit(1)

parser = argparse.ArgumentParser(description="Example: ./scripts/data_importer.py -v \"/home/vertex1.csv\" -e \"/home/edge1.csv\" -output \"./output\"")
parser.add_argument('-v', '--vertices', action='append', default=[], metavar='VERTICES FILE LIST', help='vertices input file lists separated by \',\'')
parser.add_argument('-e', '--edges', action='append', default=[], metavar='EDGES FILE LIST', help='edges input file lists separated by \',\'')
parser.add_argument('-c', '--config', action='store', metavar='CONFIG', type=str, default=age_root + '/config/conf.ini', help='config file path, default value: ${AGE_ROOT}/conf.ini')
parser.add_argument('-w', '--worker', action='store', metavar='WORKER_NODES', type=str, default=age_root + '/config/standlone_nodes.txt', help='worker nodes file path, default value: ${AGE_ROOT}/config/standlone_nodes.txt')
parser.add_argument('-d', '--delimiter', action='store', type=str, default='|', help='csv file delimiter, default value: \'|\'')
parser.add_argument('-o', '--output', action='store', metavar='OUTPUT DIR', type=str, default='./output', help='output path')
parser.add_argument('-s', '--send', action='store_true', default=False, help='Whether to send partitioned graph data to worker or not')

# Notice that this function strong related to
# the implementation of AGE::UniqueNamer
def getRankFromPartitonDir(partitionDirName):
    return int(partitionDirName.split('_')[-1])

def isSchemaDir(dirName):
    return (dirName[-1] == "s")

def quote(s):
    return '"' + s + '"'

def dequote(s):
    if s[0] == '"' and s[-1] == '"':
        return s[1:-1]
    return s

def generateFileGroups(fileListVec):
    ret = ""

    for fileList in fileListVec:
        ret += fileList + "||"

    if len(ret) > 0 :
        ret = ret[:-2]
    return ret

def sendSchema(args):
    config = configparser.ConfigParser()
    config.read(args.config)
    masterAddr = dequote(config['GLOBAL']['MASTER_IP_ADDRESS'])
    graphDir = dequote(config['GLOBAL']['GRAPH_DIR'])

    # Get schema directory name
    schemaDirName = ""
    for name in os.listdir(args.output):
        if name.split('_')[-1] == "s":
            schemaDirName = name

    srcPath = os.path.join(args.output, schemaDirName)
    dstPath = os.path.join(graphDir, schemaDirName)

    mkdirCmd = "ssh {addr} mkdir -p {dst}".format(addr=masterAddr, dst=dstPath)
    #print(mkdirCmd)
    os.system(mkdirCmd)

    scpCmd = "scp -r {src}/* {addr}:{dst}".format(src=srcPath, addr=masterAddr, dst=dstPath)
    #print(scpCmd)
    os.system(scpCmd)

def sendPartition(args):
    nodeList = GetNodeList(args.worker, "cache")
    config = configparser.ConfigParser()
    config.read(args.config)
    graphDir = dequote(config['GLOBAL']['GRAPH_DIR'])

    for dirName in os.listdir(args.output):
        if isSchemaDir(dirName):
            continue
        fullPath = os.path.join(args.output, dirName)
        if not os.path.isdir(fullPath):
            continue
        rank = getRankFromPartitonDir(dirName)
        dstPath = os.path.join(graphDir, dirName)

        mkdirCmd = "ssh {addr} mkdir -p {dst}".format(
            addr=nodeList[rank], dst=dstPath)
        os.system(mkdirCmd)

        scpCmd = "scp -r {src}/* {addr}:{dst}".format(
            src=fullPath, addr=nodeList[rank], dst=dstPath)
        #print(scpCmd)
        os.system(scpCmd)

def main():
    args = parser.parse_args()

    print(args)
    cmd = age_root + "/bin/load_data "
    cmd += " --output=" + quote(args.output)
    cmd += " --delimiter=" + quote(args.delimiter)
    cmd += " --vertices=" + quote(generateFileGroups(args.vertices))
    cmd += " --edges=" + quote(generateFileGroups(args.edges))
    cmd += " --config=" + quote(args.config)
    cmd += " --worker=" + quote(args.worker)

    print('Clearing output dir....')
    os.system('[ ! -d {} ] && mkdir {}'.format(args.output, args.output))
    os.system('[ -d {} ] && rm -r {}/*'.format(args.output, args.output))

    print('Running AGE load_data....')
    print(cmd)

    os.system(cmd)

    #if (args.send):
    #    sendPartition(args)
    #    sendSchema(args)

if __name__ == "__main__":
    main()
