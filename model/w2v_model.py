import os
import sys
import argparse
import pickle
import random
import numpy as np
from gensim.models import Word2Vec
import matplotlib.pyplot as plt
from sklearn.manifold import TSNE

sys.path.append(os.path.join(os.environ['AGE_ROOT'], 'model'))
from state import NUM_OPS, INTER_DATA_TOP_K

random.seed(0)

def get_args():
    parser = argparse.ArgumentParser(description='Extract the workload type from a workload file')
    parser.add_argument('workload_files', nargs='+', help='The paths to the workload folders')
    parser.add_argument('--draw_tsne', action='store_true', help='Draw the t-SNE plot of the workload types')
    return parser.parse_args()

def get_all_workloads(workload_files):
    files = []
    for p in workload_files:
        if os.path.isfile(p):
            files.append(p)

    data = []
    datamap = {}
    for f in files:
        with open(f, 'rb') as f:
            td = pickle.load(f)
            if len(td) == 0:
                print(f)
                continue
            idx = round(len(td) / 2)
            st = td[idx][1]
            inter_data_start_idx = 12 + 5 * NUM_OPS + 9
            interd = st[inter_data_start_idx:-2]
            # we only need the types of interd and also the P99 as the importance
            subinterd = [int(x) for x in interd[0:INTER_DATA_TOP_K]]
            datamap[f] = subinterd
            data.append(subinterd)

    return data, datamap

def draw_tsne(sentences):
    # first group all unique values from sentence
    unique = []
    for s in sentences:
        for wd in s:
            if wd not in unique:
                unique.append(wd)

    # get the embedding for each word
    # load the model
    model = Word2Vec.load(os.environ['AGE_ROOT'] + "/model/saves/inter_data_type.model")
    emb = model.wv[unique]

    # transfer the word (int) to a list of 0 and 1 exactly the same as the binary format of the integer. The length is 16.
    emb = []
    for i in unique:
        emb.append([int(x) for x in list(format(i, '016b'))])
    # transform emb to numpy.ndarray
    emb = np.array(emb)

    print(emb)

    wdlabels = []
    colormap = {}
    colors = []
    for i in unique:
        wdlabels.append([format(i, '016b')[:8], format(i, '016b')[8:]])
        cur_seg = format(i, '016b')[8:] 
        if cur_seg not in colormap:
            # assign a color
            colormap[cur_seg] = (random.random(), random.random(), random.random())
        colors.append(colormap[cur_seg])

    # draw tsne
    X_embedded = TSNE(n_components=2).fit_transform(emb)

    plt.figure(figsize=(8, 6))
    plt.scatter(X_embedded[:, 0], X_embedded[:, 1], c=colors)
    # mark every points with the corresponding word
    for i, txt in enumerate(unique):
        plt.annotate(txt, (X_embedded[i, 0], X_embedded[i, 1]))

    plt.title('t-SNE Visualization')
    plt.xlabel('t-SNE Dimension 1')
    plt.ylabel('t-SNE Dimension 2')
    plt.savefig('tsne.pdf', format='pdf')

if __name__ == '__main__':
    args = get_args()
    data, datamap = get_all_workloads(args.workload_files)
    sentences = data

    if args.draw_tsne:
        draw_tsne(sentences)
        sys.exit(0)
    else:
        print(sentences)
        for key in datamap:
            print(f"{key}: {datamap[key]}")

        model = Word2Vec(sentences, vector_size=64, window=4, min_count=1, workers=8)

        ofn = os.environ['AGE_ROOT'] + "/model/saves/inter_data_type.model"
        model.save(ofn)