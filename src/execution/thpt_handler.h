// Copyright 2021 HDL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <glog/logging.h>
#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "base/thpt_metrics.h"
#include "brpc/controller.h"
#include "execution/monitor.h"
#include "execution/numeric_monitor.h"
#include "model/rl_state.h"
#include "proto/compute_worker.pb.h"
#include "util/config.h"
#include "util/tool.h"

#pragma once

namespace AGE {

using MLModel::percentile_dist_info_t;

class ThptHandler {
   public:
    ThptHandler() : thpt_start_(false), thpt_send_end_(false) {}

    virtual void thpt_start() {
        thpt_start_ = true;
        thpt_start_time_ = Tool::getTimeNs();
    }

    virtual void thpt_send_end() { thpt_send_end_ = true; }

    virtual void reset() {
        thpt_start_ = false;
        thpt_send_end_ = false;
        inner_metrics_.reset();
        rl_state_.reset();

        query_idxed_lat_p50_.clear();
        query_idxed_lat_p99_.clear();
        query_idxed_qos_.clear();
        query_idxed_batchwidth_p99_.clear();
    }

    inline bool thpt_running() { return thpt_start_ ^ thpt_send_end_; }
    inline bool thpt_is_start() { return thpt_start_; }
    inline bool thpt_is_send_end() { return thpt_send_end_; }
    inline uint64_t thpt_start_ts() { return thpt_start_time_; }

    inline void set_retry() { rl_state_.set_retry(); }

    void StateToString(string* s) {
        // inner_metrics_.ToString(s, Config::GetInstance()->enable_timeout_);
        rl_state_.ToString(s);
    }

    void StateFromString(const string& s, size_t& pos) {
        // inner_metrics_.FromString(s, pos, Config::GetInstance()->enable_timeout_);
        rl_state_.FromString(s, pos);
    }

    void collect_idxed_metrics(unordered_map<int, double>& p50, unordered_map<int, double>& p99,
                               unordered_map<int, double>& qos) {
        query_idxed_lat_p50_ = p50;
        query_idxed_lat_p99_ = p99;
        query_idxed_qos_ = qos;
    }

    void collect_idxed_batchwidth(unordered_map<int, double>& bd) { query_idxed_batchwidth_p99_ = bd; }

    InnerMetrics& get_metrics() { return inner_metrics_; }
    MLModel::RLState& get_state() { return rl_state_; }

    void get_idxed_data(unordered_map<int, double>& p50, unordered_map<int, double>& p99,
                        unordered_map<int, double>& qos, unordered_map<int, double>& bd) {
        p50 = query_idxed_lat_p50_;
        p99 = query_idxed_lat_p99_;
        qos = query_idxed_qos_;
        bd = query_idxed_batchwidth_p99_;
    }

    void generate_state_for_rl_model(string* s) {
        rl_state_.ToString(s);
        inner_metrics_.ToString(s, Config::GetInstance()->enable_timeout_);
    }

   protected:
    bool thpt_start_;
    bool thpt_send_end_;
    uint64_t thpt_start_time_;

    InnerMetrics inner_metrics_;
    MLModel::RLState rl_state_;

    // Metrics that used for analyse
    std::unordered_map<int, double> query_idxed_lat_p50_;  // ms
    std::unordered_map<int, double> query_idxed_lat_p99_;  // ms
    std::unordered_map<int, double> query_idxed_qos_;
    std::unordered_map<int, double> query_idxed_batchwidth_p99_;  // tuple cnt
};

class MasterThptHandler : public ThptHandler {
   public:
    explicit MasterThptHandler(vector<shared_ptr<brpc::Channel>>& channels, vector<shared_ptr<brpc::Channel>>& cache_channels)
        : ThptHandler(), compute_channels_(channels), cache_channels_(cache_channels) {}

    void check_thpt_finish() {
        uint64_t ts = Tool::getTimeNs();
        size_t num_workers = compute_channels_.size();
        bool is_force_break = false;
        int try_cnt = 0;
        while (true) {
            int num_finished_workers = 0;
            LOG(INFO) << "Try check_thpt_finish: " << try_cnt++;
            int cur_num_finished_queries = 0;
            for (size_t i = 0; i < num_workers; i++) {
                std::shared_ptr<brpc::Channel> ch = compute_channels_[i];
                ProtoBuf::ComputeWorker::Service_Stub stub(ch.get());
                ProtoBuf::ComputeWorker::CheckThptReq req;
                ProtoBuf::ComputeWorker::CheckThptResp resp;
                brpc::Controller cntl;
                req.set_num_queries(num_sent_queries_per_worker_[i]);
                stub.CheckThpt(&cntl, &req, &resp, nullptr);

                if (cntl.Failed()) {
                    LOG(WARNING) << cntl.ErrorText();
                    continue;
                }

                cur_num_finished_queries += resp.num_finished_queries();

                if (resp.finish()) {
                    LOG(INFO) << "Compute Worker " << i << " finishes the thpt";
                    num_finished_workers++;
                }
            }

            if (try_cnt == 1) {
                double qps = (double)cur_num_finished_queries / send_duration_;
                inner_metrics_.set_instant_qps(qps);
                LOG(INFO) << "Instant QPS: " << qps;
            }

            if (num_finished_workers == num_workers) break;

            uint64_t dur = Tool::getTimeNs() - ts;
            if (dur > (uint64_t)(wait_threshold_ * 1E9)) {
                // stop waiting, directly proceed
                is_force_break = true;
                break;
            }

            Tool::thread_sleep_ms(1000);
        }
        LOG(INFO) << "Thpt finishes via " << (is_force_break ? "force break" : "all worker finishes");
    }

    bool get_thpt_metrics(bool with_rl_model = false, bool is_stream = false) {
        size_t num_workers = compute_channels_.size();
        uint64_t num_query = 0;
        uint64_t max_duration_ns = 0;
        string del = "|";
        std::unordered_set<size_t> finished_workers;
        int retry = 0;
        while (true) {
            if (retry++ > 10) {
                LOG(ERROR) << "Compute worker dead";
                return false;
            }

            for (size_t i = 0; i < num_workers; i++) {
                if (finished_workers.count(i)) continue;

                std::shared_ptr<brpc::Channel> ch = compute_channels_[i];
                ProtoBuf::ComputeWorker::Service_Stub stub(ch.get());
                ProtoBuf::ComputeWorker::GetThptMetricsReq req;
                ProtoBuf::ComputeWorker::GetThptMetricsResp resp;
                brpc::Controller cntl;

                string ofname = of_prefix_ + to_string(i);
                req.set_ofname(ofname);
                req.set_num_sent_queries(num_sent_queries_per_worker_[i]);
                req.set_max_lat(send_duration_ + wait_threshold_);
                req.set_with_idxed_lat(true);
                req.set_with_idxed_width(true);
                req.set_with_model_state(with_rl_model);
                req.set_is_stream(is_stream);

                stub.GetThptMetrics(&cntl, &req, &resp, nullptr);

                if (cntl.Failed()) LOG(WARNING) << cntl.ErrorText();

                if (resp.success()) {
                    LOG(INFO) << "[Worker " << i << "] send back " << resp.num_queries() << " queries with "
                              << resp.duration() << " ns";
                    finished_workers.insert(i);
                    num_query += resp.num_queries();
                    max_duration_ns = std::max(max_duration_ns, resp.duration());
                    size_t pos = 0;
                    InnerMetrics tmp;
                    tmp.FromString(resp.metrics(), pos, Config::GetInstance()->enable_timeout_);
                    inner_metrics_.merge(tmp);

                    for (auto& idxed_data : resp.lats()) {
                        LOG(INFO) << "[Worker " << i << "] Query " << idxed_data.qt_idx()
                                  << ": P50: " << idxed_data.p50() << ", P99: " << idxed_data.p99()
                                  << ", QoS: " << idxed_data.qos() << ", P99 batch width: " << idxed_data.p99_width();
                    }

                    if (with_rl_model) {
                        size_t pos = 0;
                        MLModel::RLState tmp_state;
                        tmp_state.FromString(resp.model_state(), pos);
                        rl_state_.merge(tmp_state);
                    }
                } else {
                    LOG(WARNING) << "Failed to read the metrics of worker " << i;
                    continue;
                }
            }

            if (finished_workers.size() == num_workers) break;
        }

        // collect network info from cache workers
        for (size_t i = 0; i < cache_channels_.size(); i++) {
            std::shared_ptr<brpc::Channel> ch = cache_channels_[i];
            ProtoBuf::CacheWorker::Service_Stub stub(ch.get());
            ProtoBuf::CacheWorker::GetThptMetricsReq req;
            ProtoBuf::CacheWorker::GetThptMetricsResp resp;
            brpc::Controller cntl;

            req.set_duration(max_duration_ns);
            stub.GetThptMetrics(&cntl, &req, &resp, nullptr);

            if (cntl.Failed()) LOG(WARNING) << cntl.ErrorText();
            if (resp.success()) {
                InnerMetrics tmp;
                size_t pos = 0;
                tmp.FromString(resp.metrics(), pos, Config::GetInstance()->enable_timeout_);
                inner_metrics_.merge(tmp);
            }
        }

        // Get the intermediate data infos from all compute workers
        std::map<uint16_t, vector<uint64_t>> inter_data_map;
        for (size_t i = 0; i < compute_channels_.size(); i++) {
            std::shared_ptr<brpc::Channel> ch = compute_channels_[i];
            ProtoBuf::ComputeWorker::Service_Stub stub(ch.get());
            ProtoBuf::ComputeWorker::GetInterDataReq req;
            ProtoBuf::ComputeWorker::GetInterDataResp resp;
            brpc::Controller cntl;

            stub.GetInterData(&cntl, &req, &resp, nullptr);

            if (cntl.Failed()) LOG(WARNING) << cntl.ErrorText();
            if (resp.success()) {
                for (auto & d : resp.data()) {
                    size_t pos = 0;
                    uint16_t type;
                    uint64_t size;
                    Serializer::readU16(d, pos, &type);
                    Serializer::readU64(d, pos, &size);
                    inter_data_map[type].emplace_back(size);
                }
            }
        }

        vector<uint16_t> inter_data_types;
        vector<percentile_dist_info_t> inter_data_dists;

        for (auto & item : inter_data_map) {
            uint16_t stage_type = item.first;
            auto & data = item.second;
            std::sort(data.begin(), data.end());
            auto [mean, var] = Tool::calc_distribution(data);
            // LOG(INFO) << "Inter stage " << stage_type << " has mean: " << mean << ", var: " << var << " out of " << data.size() << " records";
            int p50 = Tool::get_percentile_data(data, 50);
            int p90 = Tool::get_percentile_data(data, 90);
            int p95 = Tool::get_percentile_data(data, 95);
            int p99 = Tool::get_percentile_data(data, 99);
            // LOG(INFO) << "Inter stage " << stage_type << " has P50: " << p50 << ", P90: " << p90 << ", P95: " << p95 << ", P99: " << p99;

            double np50 = Tool::normalization(p50, mean, var);
            double np90 = Tool::normalization(p90, mean, var);
            double np95 = Tool::normalization(p95, mean, var);
            double np99 = Tool::normalization(p99, mean, var);

            percentile_dist_info_t dist;
            dist.collect(mean, data.size(), p50, p90, p95, p99, np50, np90, np95, np99);

            inter_data_types.emplace_back(stage_type);
            inter_data_dists.emplace_back(dist);
        }
        rl_state_.set_inter_data(inter_data_types, inter_data_dists);

        // After get all infos from worker, calculate the qps
        LOG(INFO) << "[Overall] There are " << num_query << " queries, finished in " << max_duration_ns << " ns";
        double qps = (double)num_query * 1E9 / max_duration_ns;
        inner_metrics_.settle_down(num_workers);
        inner_metrics_.set_qps(qps);

        if (with_rl_model) {
            // calc input qps
            uint64_t num_input_query = 0;
            for (auto& i : num_sent_queries_per_worker_) num_input_query += i;
            rl_state_.settle_down(num_workers, num_input_query);
            double input_qps = (double)num_input_query / send_duration_;
            rl_state_.set_input_qps(input_qps);
        }

        // Get cluster infos
        int num_machines = compute_channels_.size() + cache_channels_.size();
        int num_threads = Config::GetInstance()->num_threads_ + Config::GetInstance()->num_mailbox_threads_;
        rl_state_.set_env(num_machines, num_threads);

        LOG(INFO) << "[Overall] Final inner metrics: " << inner_metrics_.print(del);
        LOG(INFO) << "[Overall] Final rl state: " << rl_state_.print(del);
        return true;
    }

    void thpt_send_end(const std::vector<int>& num_queries) {
        num_sent_queries_per_worker_ = num_queries;
        ThptHandler::thpt_send_end();
    }

    void reset() override {
        ThptHandler::reset();
        num_sent_queries_per_worker_.clear();
    }

    void clear_monitors() {
        // Clear compute workers
        size_t num_workers = compute_channels_.size();
        for (size_t i = 0; i < num_workers; i++) {
            std::shared_ptr<brpc::Channel> ch = compute_channels_[i];
            ProtoBuf::ComputeWorker::Service_Stub stub(ch.get());
            ProtoBuf::AnnounceReq req;
            ProtoBuf::AnnounceResp resp;
            brpc::Controller cntl;
            stub.ClearThptMonitor(&cntl, &req, &resp, nullptr);
        }

        reset();
    }

    void set_prefix(string& prefix) { of_prefix_ = prefix; }
    void set_wait_threshold(uint64_t thrs) { wait_threshold_ = thrs; }
    void set_send_duration(uint64_t dur) { send_duration_ = dur; }

   private:
    string of_prefix_;
    uint64_t wait_threshold_;  // sec
    uint64_t send_duration_;   // sec
    std::vector<int> num_sent_queries_per_worker_;
    vector<shared_ptr<brpc::Channel>>& compute_channels_;
    vector<shared_ptr<brpc::Channel>>& cache_channels_;
};

class WorkerThptHandler : public ThptHandler {
   public:
    WorkerThptHandler() = delete;
    WorkerThptHandler(shared_ptr<OpMonitor> op_monitor, shared_ptr<Monitor<thpt_input_data_t>> thpt_input_monitor,
                      shared_ptr<Monitor<thpt_result_data_t>> thpt_result_monitor,
                      shared_ptr<Monitor<thpt_result_data_t>> thpt_result_timeout_monitor,
                      shared_ptr<Monitor<inter_data_info_t>> inter_data_monitor,
                      shared_ptr<NetworkMonitors> network_monitors)
        : ThptHandler(),
          num_queries_(0),
          thpt_duration_(0),
          op_monitor_(op_monitor),
          thpt_input_monitor_(thpt_input_monitor),
          thpt_result_monitor_(thpt_result_monitor),
          thpt_result_timeout_monitor_(thpt_result_timeout_monitor),
          inter_data_monitor_(inter_data_monitor),
          network_monitors_(network_monitors) {}

    void reset() override {
        ThptHandler::reset();

        num_queries_ = 0;
        thpt_duration_ = 0;
        op_monitor_->clear_data();
        thpt_input_monitor_->clear_data();
        thpt_result_monitor_->clear_data();
        thpt_result_timeout_monitor_->clear_data();
        inter_data_monitor_->clear_data();
        network_monitors_->reset();
    }

    void disable_monitors() {
        op_monitor_->disable();
        thpt_input_monitor_->disable();
        thpt_result_monitor_->disable();
        thpt_result_timeout_monitor_->disable();
        inter_data_monitor_->disable();
    }

    void collect_thpt_metrics(string& prefix, int num_sent_queries, uint64_t /*us*/ max_lat, int /*sec*/ duration = 0,
                              bool collect_lats = false, bool collect_batchwidth = false) {
        vector<thpt_result_data_t> period_data;
        vector<thpt_result_data_t> period_oot_data;
        bool collect_timeout = Config::GetInstance()->enable_timeout_;
        bool get_all_data = duration == 0;
        uint64_t duration_ns = duration * 1E9;
        if (get_all_data) {
            thpt_result_monitor_->get_all_data(period_data);
            if (collect_timeout) thpt_result_timeout_monitor_->get_all_data(period_oot_data);
            duration_ns = thpt_result_monitor_->get_last_data_ts() - thpt_start_time_;
        } else {
            thpt_result_monitor_->get_data(period_data, duration_ns);
            if (collect_timeout) thpt_result_timeout_monitor_->get_data(period_oot_data, duration_ns);
        }

        int num_incomplete_queries = 0;
        num_queries_ = period_data.size();
        thpt_duration_ = duration_ns;
        if (get_all_data) {
            num_incomplete_queries = num_sent_queries - num_queries_;
            num_incomplete_queries = num_incomplete_queries < 0 ? 0 : num_incomplete_queries;
        }
        rl_state_.set_unfinished_queries(num_incomplete_queries);

        LOG(INFO) << "Got " << num_queries_ << " queries and incomplete " << num_incomplete_queries << " queries"
                  << " with duration " << duration_ns << " ns";

        if (collect_lats) collect_idxed_lat(period_data, period_oot_data, prefix);
        if (collect_batchwidth) collect_idxed_batchwidth(period_data);

        double qps = (double)num_queries_ * 1E9 / duration_ns;
        double qos = 0;
        if (collect_timeout) qos = (double)period_data.size() * 100.0 / (period_data.size() + period_oot_data.size());

        if (num_incomplete_queries) {
            LOG(INFO) << "Insert " << num_incomplete_queries << " queries to result with max_lat: " << max_lat;
            period_data.insert(period_data.end(), num_incomplete_queries, thpt_result_data_t(max_lat, 0));
            LOG(INFO) << "There are total " << period_data.size() << " records";
        }

        // Collect JCT
        double lat_sum = 0.0;
        for (auto& item : period_data) lat_sum += item.latency_;
        rl_state_.set_jct(lat_sum);

        std::sort(period_data.begin(), period_data.end());
        double p50_lat = (double)(Tool::get_percentile_data(period_data, 50).latency_) / 1000.0;
        double p90_lat = (double)(Tool::get_percentile_data(period_data, 90).latency_) / 1000.0;
        double p95_lat = (double)(Tool::get_percentile_data(period_data, 95).latency_) / 1000.0;
        double p99_lat = (double)(Tool::get_percentile_data(period_data, 99).latency_) / 1000.0;
        int p99_qt_idx = Tool::get_percentile_data(period_data, 99).query_template_idx_;
        dump_full_cdf(period_data);

        std::sort(period_data.begin(), period_data.end(), thpt_result_data_t::order_by_batchwidth);
        u64 p99_bandwidth = Tool::get_percentile_data(period_data, 99).batchwidth_;

        collect_network_info(duration_ns);
        inner_metrics_.collect(qps, p50_lat, p90_lat, p95_lat, p99_lat, p99_bandwidth, qos);

        string del = "|";
        LOG(INFO) << "Collect thpt metrics: " << inner_metrics_.print(del, collect_timeout)
                  << " where P99 qt_idx: " << p99_qt_idx;

        collect_inter_data_info();
    }

    void collect_rl_state(int /*sec*/ duration = 0) {
        bool collect_all_data = duration == 0;
        rl_state_.collect_op_metrics(op_monitor_, collect_all_data);

        // Get number of heavy queries
        size_t num_heavy_queries = 0;
        if (collect_all_data) {
            num_heavy_queries = thpt_input_monitor_->get_all_data_size();
        } else {
            num_heavy_queries = thpt_input_monitor_->get_data_size(duration);
        }
        // For workers, this field is temp used as the number of heavy queries
        rl_state_.set_heavy_ratio(num_heavy_queries);

        string del = "|";
        LOG(INFO) << "Collect rl state: " << rl_state_.print(del);
    }

    void write_idxed_cdf_file(string fn_prefix, int query_template_idx, vector<u64>& lats) {
        string ofname = fn_prefix + ".query" + to_string(query_template_idx);
        std::fstream cdf_file;
        cdf_file.open(ofname, std::ios::out);
        for (auto& lat : lats) cdf_file << lat << "\n";
        cdf_file.close();
    }

    void collect_idxed_lat(const vector<thpt_result_data_t>& lats, const vector<thpt_result_data_t>& oot_lats,
                           string& fn_prefix) {
        std::unordered_map<int, vector<uint64_t>> grouped_lats;
        for (auto& item : lats) {
            grouped_lats[item.query_template_idx_].emplace_back(item.latency_);
        }

        bool collect_timeout = Config::GetInstance()->enable_timeout_;
        std::unordered_map<int, int> grouped_num_oot;
        if (collect_timeout) {
            for (auto& item : oot_lats) {
                if (grouped_num_oot.count(item.query_template_idx_)) {
                    grouped_num_oot[item.query_template_idx_]++;
                } else {
                    grouped_num_oot[item.query_template_idx_] = 1;
                }
            }
        }

        std::unordered_map<int, double> p50, p99, qos;
        for (auto itr = grouped_lats.begin(); itr != grouped_lats.end(); itr++) {
            int q_template_idx = itr->first;
            auto& lats_vec = itr->second;

            if (Config::GetInstance()->verbose_) write_idxed_cdf_file(fn_prefix, q_template_idx, lats_vec);

            if (Config::GetInstance()->enable_timeout_) {
                if (grouped_num_oot.count(q_template_idx)) {
                    qos[q_template_idx] =
                        100 * lats_vec.size() / (double)(grouped_num_oot[q_template_idx] + lats_vec.size());
                } else {
                    qos[q_template_idx] = 100.0;
                }
            }

            std::sort(lats_vec.begin(), lats_vec.end());
            p50[q_template_idx] = (double)Tool::get_percentile_data(lats_vec, 50) / 1E3;
            p99[q_template_idx] = (double)Tool::get_percentile_data(lats_vec, 99) / 1E3;
        }

        ThptHandler::collect_idxed_metrics(p50, p99, qos);
    }

    void collect_idxed_batchwidth(const vector<thpt_result_data_t>& batchwidths) {
        std::unordered_map<int, vector<double>> grouped_batchwidths;
        for (auto& item : batchwidths) {
            grouped_batchwidths[item.query_template_idx_].emplace_back(item.batchwidth_);
        }
        std::unordered_map<int, double> grouped_bandwidth_p99;
        for (auto itr = grouped_batchwidths.begin(); itr != grouped_batchwidths.end(); itr++) {
            int q_template_idx = itr->first;
            std::sort(itr->second.begin(), itr->second.end());
            grouped_bandwidth_p99[q_template_idx] = (double)Tool::get_percentile_data(itr->second, 99);
            double p50 = (double) Tool::get_percentile_data(itr->second, 50);
            double p95 = (double) Tool::get_percentile_data(itr->second, 95);
            LOG(INFO) << "Query " << q_template_idx << " batchwdith has P50: " << p50 << ", P95: " << p95 << ", P99: " << grouped_bandwidth_p99[q_template_idx];
        }

        ThptHandler::collect_idxed_batchwidth(grouped_bandwidth_p99);
    }

    void collect_network_info(uint64_t duration_ns) {
        uint64_t msgs, bytes, heavy_msgs, heavy_bytes;
        network_monitors_->get(msgs, heavy_msgs, bytes, heavy_bytes);
        double msg_thpt = (double)msgs * 1E9 / duration_ns;
        double heavy_msg_thpt = (double)heavy_msgs * 1E9 / duration_ns;
        double byte_thpt = (double)bytes * 1E9 / duration_ns;
        double heavy_byte_thpt = (double)heavy_bytes * 1E9 / duration_ns;
        LOG(INFO) << "Network thpt: " << msg_thpt << " msgs/s, " << byte_thpt << " bytes/s, " << heavy_msg_thpt
                  << " heavy msgs/s, " << heavy_byte_thpt << " heavy bytes/s";
        inner_metrics_.collect_request_info(msg_thpt, heavy_msg_thpt, byte_thpt, heavy_byte_thpt);
    }

    void collect_inter_data(vector<string>& v) {
        vector<inter_data_info_t> all_data;
        inter_data_monitor_->get_all_data(all_data);

        for (auto & data : all_data) {
            string s;
            Serializer::appendU16(&s, data.inter_stage_type_);
            Serializer::appendU64(&s, data.size_);
            v.emplace_back(std::move(s));
        }
    }

    void collect_inter_data_info() {
        vector<inter_data_info_t> all_data;
        inter_data_monitor_->get_all_data(all_data);

        std::map<uint16_t, vector<uint64_t>> inter_data_map;
        vector<uint64_t> all_inter_data_size;
        for (auto & info : all_data) {
            inter_data_map[info.inter_stage_type_].emplace_back(info.size_);
            if (info.size_ != 0) { all_inter_data_size.emplace_back(info.size_); }
        }

        for (auto & item : inter_data_map) {
            uint16_t stage_type = item.first;
            auto & data = item.second;
            std::sort(data.begin(), data.end());
            auto [mean, var] = Tool::calc_distribution(data);
            // LOG(INFO) << "Inter stage " << stage_type << " has mean: " << mean << ", var: " << var;
            int p50 = Tool::get_percentile_data(data, 50);
            int p90 = Tool::get_percentile_data(data, 90);
            int p95 = Tool::get_percentile_data(data, 95);
            int p99 = Tool::get_percentile_data(data, 99);
            // LOG(INFO) << "Inter stage " << stage_type << " has P50: " << p50 << ", P90: " << p90 << ", P95: " << p95 << ", P99: " << p99;
        }

        std::sort(all_inter_data_size.begin(), all_inter_data_size.end());
        string s = "Full CDF of Intermediate Data Size:\n";
        for (int i = 0; i < 20; i++) {
            int idx = i * 5;
            if (idx == 0) { idx = 1; }
            double sz_ = Tool::get_percentile_data(all_inter_data_size, idx);
            s += "\tP" + to_string(idx) + ":\t" + to_string(sz_) + "\n";
        }
        for (int i = 96; i < 100; i++) {
            double sz_ = Tool::get_percentile_data(all_inter_data_size, i);
            s += "\tP" + to_string(i) + ":\t" + to_string(sz_) + "\n";
        }
        LOG(INFO) << s;
    }

    void dump_full_cdf(vector<thpt_result_data_t>& data) {
        string s = "Full CDF:\n";
        for (int i = 0; i < 20; i++) {
            int idx = i * 5;
            if (idx == 0) { idx = 1; }
            double lat = (double)(Tool::get_percentile_data(data, idx).latency_) / 1000.0;  // ms
            s += "\tP" + to_string(idx) + ":\t" + to_string(lat) + " ms\n";
        }
        for (int i = 96; i < 100; i++) {
            double lat = (double)(Tool::get_percentile_data(data, i).latency_) / 1000.0;  // ms
            s += "\tP" + to_string(i) + ":\t" + to_string(lat) + " ms\n";
        }
        LOG(INFO) << s;
    }

    inline uint64_t get_num_queries() { return num_queries_; }
    inline uint64_t get_thpt_duration() { return thpt_duration_; }

   private:
    uint64_t num_queries_;
    uint64_t thpt_duration_;  // ns

    vector<std::pair<uint16_t, vector<int>>> inter_data_info_pairs_;

    std::shared_ptr<OpMonitor> op_monitor_;
    std::shared_ptr<Monitor<thpt_input_data_t>> thpt_input_monitor_;
    std::shared_ptr<Monitor<thpt_result_data_t>> thpt_result_monitor_;
    std::shared_ptr<Monitor<thpt_result_data_t>> thpt_result_timeout_monitor_;
    std::shared_ptr<Monitor<inter_data_info_t>> inter_data_monitor_;

    std::shared_ptr<NetworkMonitors> network_monitors_;
};

}  // namespace AGE
