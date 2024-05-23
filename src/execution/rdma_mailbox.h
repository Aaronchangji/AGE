/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)

*/

#pragma once

#include <emmintrin.h>
#include <glog/logging.h>
#include <mutex>
#include <string>
#include <vector>

#include "base/node.h"
#include "base/rdma.h"
#include "util/config.h"
#include "util/tool.h"

namespace AGE {

#define CLINE 64

// Single buf for send/recv
#define RDMA_SEND_BUF_SZ_MB 512
#define RDMA_RECV_BUF_SZ_MB 512

#define MB2B(x) ((x)*1024ul * 1024ul)

class RdmaBuffer {
   public:
    RdmaBuffer() = delete;
    RdmaBuffer(int local_rank, int num_remote) : local_rank_(local_rank) {
        num_threads_ = Config::GetInstance()->num_mailbox_threads_;
        uint64_t send_buf_sz = num_threads_ * RDMA_SEND_BUF_SZ_MB;
        LOG(INFO) << "RdmaBuffer: send_buf_sz: " << send_buf_sz << " MB";
        send_buf_sz = MB2B(send_buf_sz);
        uint64_t recv_buf_sz = num_remote * num_threads_ * RDMA_RECV_BUF_SZ_MB;
        LOG(INFO) << "RdmaBuffer: recv_buf_sz: " << recv_buf_sz << " MB";
        recv_buf_sz = MB2B(recv_buf_sz);

        uint64_t local_head_buf_sz = num_remote * num_threads_ * sizeof(uint64_t);
        uint64_t remote_head_buf_sz = num_remote * num_threads_ * sizeof(uint64_t);

        buffer_sz_ = send_buf_sz + recv_buf_sz + local_head_buf_sz + remote_head_buf_sz;
        LOG(INFO) << "RdmaBuffer: buffer_sz: " << buffer_sz_ << " bytes";

        buffer_ = new char[buffer_sz_];
        memset(buffer_, 0, buffer_sz_);

        send_buf_offset_ = 0;
        recv_buf_offset_ = send_buf_sz;
        local_head_offset_ = recv_buf_offset_ + recv_buf_sz;
        remote_head_offset_ = local_head_offset_ + local_head_buf_sz;

        send_buf_ = buffer_ + send_buf_offset_;
        recv_buf_ = buffer_ + recv_buf_offset_;
        local_head_buf_ = buffer_ + local_head_offset_;
        remote_head_buf_ = buffer_ + remote_head_offset_;
    }

    ~RdmaBuffer() { delete[] buffer_; }

    int get_index(int nid, int tid) {
        // nid = nid < ref_nid ? nid : nid - 1;
        return nid * num_threads_ + tid;
    }

    inline char* get_send_buf(int index) {
        CHECK_LT(index, Config::GetInstance()->num_mailbox_threads_);
        return send_buf_ + index * MB2B(RDMA_SEND_BUF_SZ_MB);
    }

    // Used to access the local buffer in rdma memory
    inline char* get_recv_buf(int nid, int tid) { return recv_buf_ + get_index(nid, tid) * MB2B(RDMA_RECV_BUF_SZ_MB); }
    inline char* get_local_head_buf(int nid, int tid) {
        return local_head_buf_ + get_index(nid, tid) * sizeof(uint64_t);
    }
    inline char* get_remote_head_buf(int nid, int tid) {
        return remote_head_buf_ + get_index(nid, tid) * sizeof(uint64_t);
    }

    // Used to find the offset for buffer on remote rdma memory
    inline uint64_t get_recv_buf_offset(int tid) {
        return recv_buf_offset_ + get_index(local_rank_, tid) * MB2B(RDMA_RECV_BUF_SZ_MB);
    }
    inline uint64_t get_local_head_offset(int tid) {
        return local_head_offset_ + get_index(local_rank_, tid) * sizeof(uint64_t);
    }
    inline uint64_t get_remote_head_offset(int tid) {
        return remote_head_offset_ + get_index(local_rank_, tid) * sizeof(uint64_t);
    }

    inline char* get_buf() { return buffer_; }
    inline uint64_t get_buf_sz() { return buffer_sz_; }

   private:
    int local_rank_;
    int num_threads_;
    uint64_t buffer_sz_;

    char* buffer_;
    char* send_buf_;
    char* recv_buf_;
    char* local_head_buf_;
    char* remote_head_buf_;

    uint64_t send_buf_offset_;
    uint64_t recv_buf_offset_;
    uint64_t local_head_offset_;
    uint64_t remote_head_offset_;
};

class RdmaMailbox {
   public:
    RdmaMailbox(int local_rank, const Nodes& remote_nodes) : local_rank_(local_rank), remote_nodes_(remote_nodes) {
        num_threads_ = Config::GetInstance()->num_mailbox_threads_;
        buf_ = new RdmaBuffer(local_rank, remote_nodes_.getWorldSize());
        rbf_sz_ = MB2B(RDMA_RECV_BUF_SZ_MB);
    }

    ~RdmaMailbox() { delete buf_; }

    void init() {
        RDMA::RDMA_init(remote_nodes_.getWorldSize(), num_threads_, local_rank_, buf_->get_buf(), buf_->get_buf_sz(),
                        remote_nodes_);

        // init rmetas and lmetas
        int num_rbfs = remote_nodes_.getWorldSize() * num_threads_;
        LOG(INFO) << "rbf_r/lmeta_t::size: " << sizeof(rbf_rmeta_t);
        rmetas = (rbf_rmeta_t*)malloc(sizeof(rbf_rmeta_t) * num_rbfs);
        memset(rmetas, 0, sizeof(rbf_rmeta_t) * num_rbfs);
        for (int i = 0; i < num_rbfs; i++) {
            rmetas[i].tail = 0;
            pthread_spin_init(&rmetas[i].lock, 0);
        }

        lmetas = (rbf_lmeta_t*)malloc(sizeof(rbf_lmeta_t) * num_rbfs);
        memset(lmetas, 0, sizeof(rbf_lmeta_t) * num_rbfs);
        for (int i = 0; i < num_rbfs; i++) {
            lmetas[i].head = 0;
            pthread_spin_init(&lmetas[i].lock, 0);
        }

        // init recv locks
        recv_locks = (pthread_spinlock_t*)malloc(sizeof(pthread_spinlock_t) * num_threads_);
        for (int i = 0; i < num_threads_; i++) {
            pthread_spin_init(&recv_locks[i], 0);
        }

        // schedulers
        schedulers.resize(num_threads_, 0);
    }

    int get_nid(const string& ip) {
        // Using this ip to search node
        int nid;
        for (nid = 0; nid < remote_nodes_.getWorldSize(); nid++) {
            const Node& node = remote_nodes_.get(nid);
            if (node.host == ip) {
                break;
            }
        }
        if (nid == remote_nodes_.getWorldSize()) {
            LOG(ERROR) << "Cannot find node with ip: " << ip;
            nid = -1;
        }
        return nid;
    }

    bool send_data(int tid, int dst_nid, int dst_tid, std::string& data) {
        size_t data_sz = data.size();
        uint64_t msg_sz = sizeof(uint64_t) * 2 + Tool::align_ceil(data_sz, sizeof(uint64_t));

        // LOG(INFO) << "Got msg of size " << data_sz << " bytes, send to " << dst_nid << ":" << dst_tid
        //           << ", the overall data to send: " << msg_sz << " bytes";

        if (msg_sz > MB2B(RDMA_SEND_BUF_SZ_MB)) {
            LOG(ERROR) << "Message size " << msg_sz << " exceeds the limit " << MB2B(RDMA_SEND_BUF_SZ_MB);
            LOG(ERROR) << "Please size down the message size, or enlarge the send buffer size!";
            exit(1);
        }

        rbf_rmeta_t* rmeta = &rmetas[get_index(dst_nid, dst_tid)];
        pthread_spin_lock(&rmeta->lock);
        // detect overflow and update tail
        if (is_buf_full(dst_nid, dst_tid, rmeta->tail, msg_sz)) {
            pthread_spin_unlock(&rmeta->lock);
            LOG(ERROR) << "remote recv remote buffer doesn't have enough space for size " << msg_sz;
            return false;
        }
        uint64_t off = rmeta->tail;
        rmeta->tail += msg_sz;
        pthread_spin_unlock(&rmeta->lock);

        // write the send data to local registered rdma send buffer
        char* rdma_buf = buf_->get_send_buf(tid);
        *((uint64_t*)rdma_buf) = data_sz;  // header
        rdma_buf += sizeof(uint64_t);
        memcpy(rdma_buf, data.c_str(), data_sz);  // data content
        rdma_buf += Tool::align_ceil(data_sz, sizeof(uint64_t));
        *((uint64_t*)rdma_buf) = data_sz;  // footer

        // Send the data in send buffer to remote recv buffer
        RDMA& rdma = RDMA::get_rdma();
        uint64_t rdma_off = buf_->get_recv_buf_offset(dst_tid);
        if (Config::GetInstance()->verbose_) {
            uint64_t rdma_off_printable = rdma_off / 1024 / 1024;
            uint64_t start_off = rdma_off + (off % rbf_sz_);
            LOG(INFO) << "Write to remote buffer (" << dst_nid << ", " << dst_tid << ", " << rdma_off_printable
                      << "), starting from " << start_off;
        }
        pthread_spin_lock(&rmeta->lock);
        if (off / rbf_sz_ == (off + msg_sz - 1) / rbf_sz_) {
            // does not exceed the boundary
            rdma.dev->RdmaWrite(dst_tid, dst_nid, buf_->get_send_buf(tid), msg_sz, rdma_off + (off % rbf_sz_));
        } else {
            // ring buffer, need to write from head
            LOG_IF(INFO, Config::GetInstance()->verbose_) << "[RdmaMailbox] Ring buffer is invoked";
            uint64_t _sz = rbf_sz_ - (off % rbf_sz_);
            rdma.dev->RdmaWrite(dst_tid, dst_nid, buf_->get_send_buf(tid), _sz, rdma_off + (off % rbf_sz_));
            rdma.dev->RdmaWrite(dst_tid, dst_nid, buf_->get_send_buf(tid) + _sz, msg_sz - _sz, rdma_off);
        }
        pthread_spin_unlock(&rmeta->lock);

        return true;
    }

    bool try_recv(int tid, std::string& data, std::string& remote_ip) {
        pthread_spin_lock(&recv_locks[tid]);
        for (int i = 0; i < remote_nodes_.getWorldSize(); i++) {
            int remote_nid = (schedulers[tid]++) % remote_nodes_.getWorldSize();
            if (check_recv_buf(tid, remote_nid)) {
                fetch_data_from_recv_buf(tid, remote_nid, data);
                remote_ip = remote_nodes_.get(remote_nid).host;
                pthread_spin_unlock(&recv_locks[tid]);
                return true;
            }
        }
        pthread_spin_unlock(&recv_locks[tid]);
        return false;
    }

   private:
    struct rbf_rmeta_t {
        // used to record where to write to remote
        uint64_t tail;
        pthread_spinlock_t lock;
    } __attribute__((aligned(CLINE)));

    struct rbf_lmeta_t {
        // used to record where to read in local recv buffer
        uint64_t head;
        pthread_spinlock_t lock;
    } __attribute__((aligned(CLINE)));

    int get_index(int nid, int tid) {
        // nid = nid < ref_nid ? nid : nid - 1;
        return nid * num_threads_ + tid;
    }

    inline bool is_buf_full(int dst_nid, int dst_tid, uint64_t tail, uint64_t msg_sz) {
        // uint64_t rbf_sz = MB2B(RDMA_RECV_BUF_SZ_MB);
        uint64_t head = *(uint64_t*)buf_->get_remote_head_buf(dst_nid, dst_tid);
        return rbf_sz_ < (tail - head + msg_sz);
    }

    bool check_recv_buf(int dst_tid, int dst_nid) {
        rbf_lmeta_t* lmeta = &lmetas[get_index(dst_nid, dst_tid)];
        char* rbf = buf_->get_recv_buf(dst_nid, dst_tid);
        volatile uint64_t msg_size = *(volatile uint64_t*)(rbf + lmeta->head % rbf_sz_);
        return msg_size != 0;
    }

    void fetch_data_from_recv_buf(int tid, int remote_nid, string& data) {
        rbf_lmeta_t* lmeta = &lmetas[get_index(remote_nid, tid)];
        char* rbf = buf_->get_recv_buf(remote_nid, tid);
        // read the header
        volatile uint64_t data_sz = *(volatile uint64_t*)(rbf + lmeta->head % rbf_sz_);

        uint64_t to_footer = sizeof(uint64_t) + Tool::align_ceil(data_sz, sizeof(uint64_t));
        volatile uint64_t* footer = (volatile uint64_t*)(rbf + (lmeta->head + to_footer) % rbf_sz_);

        if (data_sz) {
            // Ensure the data is ready
            while (*footer != data_sz) {
                // LOG(INFO) << "[n:" << remote_nid << ", t:" << tid << "] Read from " << lmeta->head;
                _mm_pause();
            }

            uint64_t data_start_idx = (lmeta->head + sizeof(uint64_t)) % rbf_sz_;
            uint64_t data_end_idx = (lmeta->head + sizeof(uint64_t) + data_sz) % rbf_sz_;

            LOG_IF(INFO, Config::GetInstance()->verbose_)
                << "Try to recv " << data_sz << " bytes in buf(n:" << remote_nid << ", t:" << tid << ") from " << data_start_idx << " in bytes";

            char* tmp = new char[data_sz];
            if (data_start_idx > data_end_idx) {
                // ring
                LOG_IF(INFO, Config::GetInstance()->verbose_) << "[RdmaMailbox] Ring buffer is invoked";
                memcpy(tmp, rbf + data_start_idx, data_sz - data_end_idx);
                memcpy(tmp + data_sz - data_end_idx, rbf, data_end_idx);
                data.assign(tmp, data_sz);
                memset(rbf + data_start_idx, 0, data_sz - data_end_idx);
                memset(rbf, 0, Tool::align_ceil(data_end_idx, sizeof(uint64_t)));
            } else {
                memcpy(tmp, rbf + data_start_idx, data_sz);
                data.assign(tmp, data_sz);
                memset(rbf + data_start_idx, 0, Tool::align_ceil(data_sz, sizeof(uint64_t)));
            }

            // clean header and footer
            *(uint64_t*)(rbf + lmeta->head % rbf_sz_) = 0;
            *footer = 0;

            // move forward the head
            lmeta->head += sizeof(uint64_t) * 2 + Tool::align_ceil(data_sz, sizeof(uint64_t));

            // update heads of ring buffer to writer
            const uint64_t threshold = rbf_sz_ / 16;
            char* head = buf_->get_local_head_buf(remote_nid, tid);

            if (lmeta->head - *(uint64_t*)head > threshold) {
                LOG_IF(INFO, Config::GetInstance()->verbose_) << "Update head of ring buffer to writer";
                *(uint64_t*)head = lmeta->head;
                rbf_rmeta_t* rmeta = &rmetas[get_index(remote_nid, tid)];
                RDMA& rdma = RDMA::get_rdma();
                uint64_t off = buf_->get_remote_head_offset(tid);
                pthread_spin_lock(&rmeta->lock);
                rdma.dev->RdmaWrite(tid, remote_nid, head, sizeof(uint64_t), off);
                pthread_spin_unlock(&rmeta->lock);
            }
        }
    }

    RdmaBuffer* buf_;
    uint64_t rbf_sz_;
    rbf_rmeta_t* rmetas = NULL;  // remote recv buf meta
    rbf_lmeta_t* lmetas = NULL;  // local recv buf meta
    pthread_spinlock_t* recv_locks = NULL;
    std::vector<uint64_t> schedulers;

    const Nodes& remote_nodes_;
    int local_rank_;
    int num_threads_;
};

}  // namespace AGE
