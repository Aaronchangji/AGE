import struct
import numpy as np
import utils
import torch

NUM_OPS = 7
NUM_DIST_INFO_BYTES = 40
NUM_PERCENTILE_DIST_INFO_BYTES = 80
INTER_DATA_TOP_K = 10
RAW_INTER_DATA_INFO_LEN = 110  # 10 type + 10 * 10 precentile dist info
PERCENTILE_INFO_LEN = 6

class DistInfo:
    def __init__(self):
        self.mean = -1.0
        self.min = -1.0
        self.p50 = -1.0
        self.p99 = -1.0
        self.max = -1.0

    def set_from_bytes(self, data):
        idxes = [0, 8, 16, 24, 32, 40]
        self.mean = struct.unpack("d", data[idxes[0] : idxes[1]])[0]
        self.min = struct.unpack("d", data[idxes[1] : idxes[2]])[0]
        self.p50 = struct.unpack("d", data[idxes[2] : idxes[3]])[0]
        self.p99 = struct.unpack("d", data[idxes[3] : idxes[4]])[0]
        self.max = struct.unpack("d", data[idxes[4] : idxes[5]])[0]

    def __str__(self):
        return f"{self.mean}|{self.min}|{self.p50}|{self.p99}|{self.max}"

    def append(self, arr, with_minmax=True):
        arr.append(self.mean)
        if with_minmax:
            arr.append(self.min)
        arr.append(self.p50)
        arr.append(self.p99)
        if with_minmax:
            arr.append(self.max)
        return arr

class PercentileDistInfo:
    def __init__(self):
        self.mean = -1.0
        self.sz = -1.0
        self.p50 = -1.0
        self.p90 = -1.0
        self.p95 = -1.0
        self.p99 = -1.0
        self.np50 = -1.0
        self.np90 = -1.0
        self.np95 = -1.0
        self.np99 = -1.0

    def set_from_bytes(self, data):
        idxes = [0, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80]
        self.mean = struct.unpack("d", data[idxes[0] : idxes[1]])[0]
        self.sz = struct.unpack("d", data[idxes[1] : idxes[2]])[0]
        self.p50 = struct.unpack("d", data[idxes[2] : idxes[3]])[0]
        self.p90 = struct.unpack("d", data[idxes[3] : idxes[4]])[0]
        self.p95 = struct.unpack("d", data[idxes[4] : idxes[5]])[0]
        self.p99 = struct.unpack("d", data[idxes[5] : idxes[6]])[0]
        self.np50 = struct.unpack("d", data[idxes[6] : idxes[7]])[0]
        self.np90 = struct.unpack("d", data[idxes[7] : idxes[8]])[0]
        self.np95 = struct.unpack("d", data[idxes[8] : idxes[9]])[0]
        self.np99 = struct.unpack("d", data[idxes[9] : idxes[10]])[0]

    def __str__(self):
        return f"{self.mean}|{self.sz}|{self.p50}|{self.p90}|{self.p95}|{self.p99}|{self.np50}|{self.np90}|{self.np95}|{self.np99}"

    def __len__(self):
        return 6

    def append_all(self, arr):
        arr.append(self.mean)
        arr.append(self.sz)
        arr.append(self.p50)
        arr.append(self.p90)
        arr.append(self.p95)
        arr.append(self.p99)
        arr.append(self.np50)
        arr.append(self.np90)
        arr.append(self.np95)
        arr.append(self.np99)
        return arr

    def append(self, arr, use_normalized=False):
        arr.append(self.mean)
        arr.append(self.sz)
        if use_normalized:
            arr.append(self.np50)
            arr.append(self.np90)
            arr.append(self.np95)
            arr.append(self.np99)
        else:
            arr.append(self.p50)
            arr.append(self.p90)
            arr.append(self.p95)
            arr.append(self.p99)
        return arr

class InnerMetrics:
    def __init__(self):
        self.qps = 0.0
        self.instant_qps = 0.0
        self.lat_p50 = 0.0
        self.lat_p90 = 0.0
        self.lat_p95 = 0.0
        self.lat_p99 = 0.0
        self.batchwidth_p99 = 0.0
        self.req_thpt = 0.0
        self.heavy_req_thpt = 0.0
        self.byte_thpt = 0.0
        self.heavy_byte_thpt = 0.0
        self.cache_req_thpt = 0.0
        self.cache_heavy_req_thpt = 0.0
        self.cache_byte_thpt = 0.0
        self.cache_heavy_byte_thpt = 0.0

    def set_from_bytes(self, data):
        self.qps = struct.unpack("d", data[0:8])[0]
        self.instant_qps = struct.unpack("d", data[8:16])[0]
        self.lat_p50 = struct.unpack("d", data[16:24])[0]
        self.lat_p90 = struct.unpack("d", data[24:32])[0]
        self.lat_p95 = struct.unpack("d", data[32:40])[0]
        self.lat_p99 = struct.unpack("d", data[40:48])[0]
        self.batchwidth_p99 = struct.unpack("d", data[48:56])[0]
        self.req_thpt = struct.unpack("d", data[56:64])[0]
        self.heavy_req_thpt = struct.unpack("d", data[64:72])[0]
        self.byte_thpt = struct.unpack("d", data[72:80])[0]
        self.heavy_byte_thpt = struct.unpack("d", data[80:88])[0]
        self.cache_req_thpt = struct.unpack("d", data[88:96])[0]
        self.cache_heavy_req_thpt = struct.unpack("d", data[96:104])[0]
        self.cache_byte_thpt = struct.unpack("d", data[104:112])[0]
        self.cache_heavy_byte_thpt = struct.unpack("d", data[112:120])[0]

    def __str__(self):
        s = f"{self.qps}|{self.instant_qps}|{self.lat_p50}|{self.lat_p90}|{self.lat_p95}|{self.lat_p99}|{self.batchwidth_p99}\n"
        s += f"{self.req_thpt}|{self.heavy_req_thpt}|{self.byte_thpt}|{self.heavy_byte_thpt}\n"
        s += f"{self.cache_req_thpt}|{self.cache_heavy_req_thpt}|{self.cache_byte_thpt}|{self.cache_heavy_byte_thpt}"
        return s


class RawState:
    def __init__(self):
        self.input_qps = 0.0
        self.heavy_ratio = 0.0
        self.jct = 0.0
        self.queue_lat_dist = DistInfo()
        self.net_lat_dist = DistInfo()
        self.input_size_dists = [DistInfo() for i in range(NUM_OPS)]
        self.num_unfinished_queries = 0
        self.inter_data_infos = []
        self.num_machines = 0
        self.num_threads = 0
        self.retry = 0
        self.inner_metrics = InnerMetrics()

    def set_from_bytes(self, data):
        self.input_qps = struct.unpack("d", data[0:8])[0]
        self.heavy_ratio = struct.unpack("d", data[8:16])[0]
        self.jct = struct.unpack("d", data[16:24])[0]
        que_idx_ends = 24 + NUM_DIST_INFO_BYTES
        self.queue_lat_dist.set_from_bytes(data[24:que_idx_ends])
        net_idx_ends = que_idx_ends + NUM_DIST_INFO_BYTES
        self.net_lat_dist.set_from_bytes(data[que_idx_ends:net_idx_ends])
        len_idx_ends = net_idx_ends + 8
        len = struct.unpack("Q", data[net_idx_ends:len_idx_ends])[0]
        for i in range(len):
            start_pos = len_idx_ends + i * NUM_DIST_INFO_BYTES
            end_pos = len_idx_ends + (i + 1) * NUM_DIST_INFO_BYTES
            self.input_size_dists[i].set_from_bytes(data[start_pos:end_pos])
        un_que_idx = len_idx_ends + len * NUM_DIST_INFO_BYTES
        self.num_unfinished_queries = struct.unpack("Q", data[un_que_idx : un_que_idx + 8])[0]

        inter_data_info_len = struct.unpack("Q", data[un_que_idx + 8 : un_que_idx + 16])[0]
        for i in range(inter_data_info_len):
            start_pos = un_que_idx + 16 + i * (NUM_PERCENTILE_DIST_INFO_BYTES + 2)  # 2 is for inter_data_type
            end_pos = un_que_idx + 16 + (i + 1) * (NUM_PERCENTILE_DIST_INFO_BYTES + 2)
            inter_data_type = struct.unpack("H", data[start_pos:start_pos + 2])[0]
            inter_data_dist = PercentileDistInfo()
            inter_data_dist.set_from_bytes(data[start_pos + 2:end_pos])
            self.inter_data_infos.append([inter_data_type, inter_data_dist])

        cluster_info_idx = un_que_idx + 16 + inter_data_info_len * (NUM_PERCENTILE_DIST_INFO_BYTES + 2)
        self.num_machines = struct.unpack("H", data[cluster_info_idx:cluster_info_idx + 2])[0]
        self.num_threads = struct.unpack("H", data[cluster_info_idx + 2:cluster_info_idx + 4])[0]
        self.retry = struct.unpack("B", data[cluster_info_idx + 4:cluster_info_idx + 5])[0]
        inner_metrics_idx = cluster_info_idx + 5
        self.inner_metrics.set_from_bytes(data[inner_metrics_idx:])

    def print(self):
        print(f"Input QPS: {self.input_qps}|JCT: {self.jct}|Heavy Ratio: {self.heavy_ratio}")
        self.inner_metrics.print()
        self.queue_lat_dist.print("|")
        self.net_lat_dist.print("|")
        for x in self.input_size_dists:
            x.print("|")

    def __str__(self):
        s = f"Input QPS: {self.input_qps}|JCT: {self.jct}|Heavy Ratio: {self.heavy_ratio}\n"
        s += str(self.inner_metrics) + "\n"
        s += str(self.queue_lat_dist) + "\n"
        s += str(self.net_lat_dist) + "\n"
        for x in self.input_size_dists:
            s += str(x) + "\n"
        return s

    def get_all_inter_data_infos(self, state):
        sorted_inter_data_info = sorted(self.inter_data_infos, key=lambda x: x[1].p99, reverse=True)
        for i in range(INTER_DATA_TOP_K):
            if i < len(sorted_inter_data_info):
                state.append(sorted_inter_data_info[i][0])
            else:
                # padding zeros
                state.append(0)

        for i in range(INTER_DATA_TOP_K):
            if i < len(sorted_inter_data_info):
                sorted_inter_data_info[i][1].append_all(state)
            else:
                # padding zeros
                state.extend([0] * 10)

        return state

    def get_inter_data_infos(self, state, use_bitlist=True, use_normalized_inter_data=False,
                             use_embeddings=True, down_data_scale=True, embaug=10, use_inter_value=True, use_workload=True,
                             add_weighted_ir=True, use_sif=False):
        # find top K inter data types with the order of P99
        sorted_inter_data_info = sorted(self.inter_data_infos, key=lambda x: x[1].p99, reverse=True)
        # process types
        inter_data_types = []
        inter_data_types_bitlist = []
        for i in range(INTER_DATA_TOP_K):
            if i < len(sorted_inter_data_info):
                inter_data_types.append(sorted_inter_data_info[i][0])
                bitlist = utils.inter_data_type_to_bitlist(sorted_inter_data_info[i][0])
                inter_data_types_bitlist.extend(bitlist)
            else:
                # padding zeros
                inter_data_types.append(0)
                inter_data_types_bitlist.extend([0] * 16)

        # process values
        inter_data_values = []
        for i in range(INTER_DATA_TOP_K):
            if i < len(sorted_inter_data_info):
                sorted_inter_data_info[i][1].append(inter_data_values, use_normalized=use_normalized_inter_data)
            else:
                # padding zeros
                inter_data_values.extend([0] * PERCENTILE_INFO_LEN)

        # composite
        if use_workload:
            if use_embeddings:
                tmp_inter_data_info = []
                tmp_inter_data_info.extend(inter_data_types)
                tmp_inter_data_info.extend(inter_data_values)
                inter_data_emb = self.transfer_topK_to_embeddings(tmp_inter_data_info, utils.WORD2VECMODEL, full_inter_data=False,
                                                                  down_data_scale=down_data_scale, embaug=embaug, add_weighted_ir=add_weighted_ir,
                                                                  use_sif=use_sif)
                state.extend(inter_data_emb)
            else:
                tmp_inter_data_info = []
                tmp_inter_data_info.extend(inter_data_types_bitlist)
                tmp_inter_data_info.extend(inter_data_values)
                merged_bitlist_and_values = self.merge_data_info(tmp_inter_data_info, full_inter_data=False, down_data_scale=down_data_scale)
                state.extend(merged_bitlist_and_values)

        if down_data_scale:
            inter_data_values = [x / 1000.0 for x in inter_data_values]

        if use_inter_value:
            state.extend(inter_data_values)

        return state

    def extract_all_state(self):
        st = [self.input_qps, self.heavy_ratio]
        st = self.queue_lat_dist.append(st, True)
        st = self.net_lat_dist.append(st, True)
        for x in self.input_size_dists:
            st = x.append(st, True)
        st.append(self.inner_metrics.qps)

        st.append(self.inner_metrics.req_thpt)
        st.append(self.inner_metrics.heavy_req_thpt)
        st.append(self.inner_metrics.byte_thpt)
        st.append(self.inner_metrics.heavy_byte_thpt)

        st.append(self.inner_metrics.cache_req_thpt)
        st.append(self.inner_metrics.cache_heavy_req_thpt)
        st.append(self.inner_metrics.cache_byte_thpt)
        st.append(self.inner_metrics.cache_heavy_byte_thpt)

        st = self.get_all_inter_data_infos(st)

        st.append(self.num_machines)
        st.append(self.num_threads)

        st.append(self.num_unfinished_queries)
        return np.array(st)

    @staticmethod
    def down_data_scale_by_value(st_arr):
        # any value that is larger than 10e4, use log value
        for i in range(len(st_arr)):
            if st_arr[i] < -1:
                continue
            st_arr[i] = np.log2(st_arr[i] + 2)
        return st_arr

    def down_data_scale_by_type(self):
        # qps to Kqps
        self.input_qps = self.input_qps / 1000.0
        self.inner_metrics.qps = self.inner_metrics.qps / 1000.0

        # lat to ms
        self.queue_lat_dist.mean = self.queue_lat_dist.mean / 1000.0
        self.queue_lat_dist.p50 = self.queue_lat_dist.p50 / 1000.0
        self.queue_lat_dist.p99 = self.queue_lat_dist.p99 / 1000.0
        self.net_lat_dist.mean = self.net_lat_dist.mean / 1000.0
        self.net_lat_dist.p50 = self.net_lat_dist.p50 / 1000.0
        self.net_lat_dist.p99 = self.net_lat_dist.p99 / 1000.0

        # bw bytes to Mbytes
        self.inner_metrics.byte_thpt = self.inner_metrics.byte_thpt / 1024.0 / 1024.0
        self.inner_metrics.cache_byte_thpt = self.inner_metrics.cache_byte_thpt / 1024.0 / 1024.0

    '''
    Extract state for RL model
        - use_normalized_inter_data: whether to use normalized inter data (always default)
        - use_embeddings: whether to use embeddings for inter data (adjustable)
        - enhance_cluster_info: whether to enhance cluster info (always default)
        - down_data_scale: whether to down data scale (always default)
        - embaug: embedding augmentation factor 
        - use_inter_value: whether to use inter data value (adjustable)
        - use_workload: whether to use workload information (adjustable)
        - add_weighted_ir: whether to add weighted stat to embedding (adjustable)
    '''
    def extract_state(self, queue=True, network=True, opsize=True, with_minmax=False,
                      use_normalized_inter_data=False, use_embeddings=True, enhance_cluster_info=True, down_data_scale=True,
                      embaug=10, use_inter_value=True, use_workload=True, add_weighted_ir=True, use_sif=False):
        if down_data_scale:
            self.down_data_scale_by_type()

        st = [self.heavy_ratio, self.input_qps]
        if queue:
            st = self.queue_lat_dist.append(st, with_minmax)
        if network:
            st = self.net_lat_dist.append(st, with_minmax)
        if opsize:
            for x in self.input_size_dists:
                st = x.append(st, with_minmax)
        st.append(self.inner_metrics.qps)
        st.append(self.inner_metrics.byte_thpt)
        st.append(self.inner_metrics.cache_byte_thpt)

        st = self.get_inter_data_infos(st, use_bitlist=True, use_normalized_inter_data=use_normalized_inter_data,
                                       use_embeddings=use_embeddings, down_data_scale=down_data_scale, embaug=embaug,
                                       use_inter_value=use_inter_value, use_workload=use_workload, add_weighted_ir=add_weighted_ir,
                                       use_sif=use_sif)

        # enhance cluster info
        st.append(self.num_machines)
        st.append(self.num_threads)
        if enhance_cluster_info:
            for i in range(2):
                st.append(self.num_machines)
                st.append(self.num_threads)

        st.append(self.num_unfinished_queries)
        return np.array(st)

    @staticmethod
    def extract_without_opinfo(state_arr):
        return state_arr[0:13]

    @staticmethod
    def extract_queue_lat(state_arr):
        return state_arr[3:8]

    @staticmethod
    def extract_net_lat(state_arr):
        return state_arr[8:13]

    @staticmethod
    def transfer_topK_to_embeddings(inter_data_arr, word2vecmodel, full_inter_data=False, down_data_scale=True, embaug=10, add_weighted_ir=False, use_sif=False):
        ret = []
        subquery_types = inter_data_arr[0:INTER_DATA_TOP_K]
        values = inter_data_arr[INTER_DATA_TOP_K:]
        subquery_types = [int(x) for x in subquery_types]

        items_per_type = 6
        if full_inter_data:
            items_per_type = 10

        # get importance
        importances = []
        for i in range(INTER_DATA_TOP_K):
            idx = i * items_per_type + 5
            importances.append(values[idx])
        total = sum(importances)
        importances = [x / total for x in importances]

        # get embeddings
        subquery_embs = word2vecmodel.wv[subquery_types]
        if use_sif:
            frequency = []
            for i in range(INTER_DATA_TOP_K):
                idx = i * items_per_type + 1
                frequency.append(values[idx])
            ftotal = sum(frequency)
            frequency = [x / ftotal for x in frequency]
            frequency = np.array(frequency)
            semb = utils.SIF_Embedding(subquery_embs, frequency, a=1e-3)
            semb = embaug * semb
            ret.extend(semb)
        else:
            for i in range(len(subquery_embs)):
                for j in range(len(subquery_embs[i])):
                    subquery_embs[i][j] = subquery_embs[i][j] * importances[i]
            weighted_emb = subquery_embs.sum(axis=0)
            weighted_emb = [x * embaug for x in weighted_emb]
            ret.extend(weighted_emb)

        # get other information weighted sum
        weighted_stat = [0] * items_per_type
        for i in range(INTER_DATA_TOP_K):
            start_idx = i * items_per_type
            for j in range(items_per_type):
                weighted_stat[j] += values[start_idx + j] * importances[i]
        if down_data_scale:
            weighted_stat = [x / 1000.0 for x in weighted_stat]

        if add_weighted_ir:
            ret.extend(weighted_stat)

        return ret

    '''
        inter_data_info, value part is already been down scaled
    '''
    @staticmethod
    def merge_data_info(inter_data_info, full_inter_data=False, down_data_scale=False):
        bitlists = inter_data_info[:INTER_DATA_TOP_K * 16]
        values = inter_data_info[INTER_DATA_TOP_K * 16:]

        items_per_type = 6
        if full_inter_data:
            items_per_type = 10

        importances = []
        for i in range(INTER_DATA_TOP_K):
            idx = i * items_per_type + 5
            importances.append(values[idx])
        total = sum(importances)
        importances = [x / total for x in importances]

        bitlists = [bitlists[i * 16:(i + 1) * 16] for i in range(INTER_DATA_TOP_K)]

        ret = []
        for i in range(INTER_DATA_TOP_K):
            for j in range(len(bitlists[i])):
                bitlists[i][j] = bitlists[i][j]
        # sum all columns in bitlists
        weighted_bitlist = np.sum(bitlists, axis=0)
        ret.extend(weighted_bitlist)

        # get other information weighted sum
        weighted_stat = [0] * items_per_type
        for i in range(INTER_DATA_TOP_K):
            start_idx = i * items_per_type
            for j in range(items_per_type):
                weighted_stat[j] += values[start_idx + j]
        if down_data_scale:
            weighted_stat = [x / 1000.0 / INTER_DATA_TOP_K for x in weighted_stat]
        ret.extend(weighted_stat)

        return ret