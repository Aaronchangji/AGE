/*
 * Copyright (c) 2016 Shanghai Jiao Tong University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://ipads.se.sjtu.edu.cn/projects/wukong
 *
 */

/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#pragma once

#pragma GCC diagnostic warning "-fpermissive"

#include <assert.h>
#include <glog/logging.h>
#include <fstream>   // std::ifstream
#include <iostream>  // std::cout
#include <string>
#include <vector>

#include "base/node.h"
#include "rdmalib/rdmaio.hpp"
#include "util/tool.h"

namespace AGE {

class RDMA {
    class RDMA_Device {
       public:
        rdmaio::RdmaCtrl *ctrl = NULL;

        RDMA_Device(int num_nodes, int num_threads, int nid, char *mem, uint64_t mem_sz,
                    const AGE::Nodes &remote_nodes)
            : num_threads_(num_threads) {
            // record IPs of ndoes
            vector<string> ipset;
            for (int i = 0; i < remote_nodes.getWorldSize(); i++) ipset.emplace_back(remote_nodes.get(i).host);

            for (auto &ip : ipset) LOG(INFO) << "Remote Node IP: " << ip;

            int rdma_port = remote_nodes.get(0).rdma_port;
            // initialization of new librdma
            // nid, ipset, port, thread_id-no use, enable single memory region
            ctrl = new rdmaio::RdmaCtrl(nid, ipset, rdma_port, true);
            ctrl->open_device();
            ctrl->set_connect_mr(mem, mem_sz);
            ctrl->register_connect_mr();  // single
            ctrl->start_server();

            // send qp | recv qp
            for (uint i = 1; i < num_nodes + 1; ++i) {
                // Currently, we only create qps for RdmaWrite, read is not used
                for (uint j = 0; j < num_threads_; ++j) {
                    rdmaio::Qp *qp = ctrl->create_rc_qp(j, i, 0, 1);
                    CHECK(qp != NULL);
                }
            }

            while (true) {
                int connected = 0;
                for (uint i = 0; i < num_nodes; ++i) {
                    for (uint j = 0; j < num_threads_; ++j) {
                        rdmaio::Qp *qp = ctrl->create_rc_qp(j, i, 0, 1);
                        if (qp->inited_) {
                            connected += 1;
                        } else {
                            if (qp->connect_rc()) {
                                connected += 1;
                            }
                        }
                    }
                }
                if (connected == num_nodes * num_threads_) {
                    break;
                } else {
                    sleep(1);
                }
            }
        }

        // 0 on success, -1 otherwise
        int RdmaRead(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
            // virtual tid for read
            int vir_tid = dst_tid + num_threads_;

            rdmaio::Qp *qp = ctrl->get_rc_qp(vir_tid, dst_nid);
            qp->rc_post_send(IBV_WR_RDMA_READ, local, size, off, IBV_SEND_SIGNALED);
            if (!qp->first_send()) qp->poll_completion();

            qp->poll_completion();
            return 0;
            // return rdmaOp(dst_tid, dst_nid, local, size, off, IBV_WR_RDMA_READ);
        }

        int RdmaWrite(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
            rdmaio::Qp *qp = ctrl->get_rc_qp(dst_tid, dst_nid);

            // int flags = (qp->first_send() ? IBV_SEND_SIGNALED : 0);
            int flags = IBV_SEND_SIGNALED;
            qp->rc_post_send(IBV_WR_RDMA_WRITE, local, size, off, flags);

            // if(qp->need_poll())
            qp->poll_completion();

            return 0;
            // return rdmaOp(dst_tid, dst_nid, local, size, off, IBV_WR_RDMA_WRITE);
        }

        int RdmaWriteSelective(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
            rdmaio::Qp *qp = ctrl->get_rc_qp(dst_tid, dst_nid);
            int flags = (qp->first_send() ? IBV_SEND_SIGNALED : 0);
            // int flags = IBV_SEND_SIGNALED;

            qp->rc_post_send(IBV_WR_RDMA_WRITE, local, size, off, flags);

            if (qp->need_poll()) qp->poll_completion();
            return 0;
            // return rdmaOp(dst_tid, dst_nid, local, size, off, IBV_WR_RDMA_WRITE);
        }

        int RdmaWriteNonSignal(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
            rdmaio::Qp *qp = ctrl->get_rc_qp(dst_tid, dst_nid);
            int flags = 0;
            qp->rc_post_send(IBV_WR_RDMA_WRITE, local, size, off, flags);
            return 0;
        }

       private:
        int num_threads_;
    };

   public:
    RDMA_Device *dev = NULL;

    RDMA() {}

    ~RDMA() {
        if (dev != NULL) delete dev;
    }

    void init_dev(int num_nodes, int num_threads, int nid, char *mem, uint64_t mem_sz, const AGE::Nodes &nodes) {
        dev = new RDMA_Device(num_nodes, num_threads, nid, mem, mem_sz, nodes);
    }

    inline static bool has_rdma() { return true; }

    static RDMA &get_rdma() {
        static RDMA rdma;
        return rdma;
    }

    static void RDMA_init(int num_nodes, int num_threads, int nid, char *mem, uint64_t mem_sz,
                          const AGE::Nodes &nodes) {
        uint64_t t = AGE::Tool::getTimeNs();

        // init RDMA device
        RDMA &rdma = RDMA::get_rdma();
        rdma.init_dev(num_nodes, num_threads, nid, mem, mem_sz, nodes);

        t = AGE::Tool::getTimeNs() - t;
        LOG(INFO) << "Initializing RDMA done (" << t / 1000 / 1000 << " ms)" << std::flush;
    }
};

}  // namespace AGE
