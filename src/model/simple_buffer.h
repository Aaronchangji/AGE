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

#pragma once

#include <fcntl.h>
#include <glog/logging.h>
#include <semaphore.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstdint>
#include <cstring>
#include <string>

namespace AGE {
namespace MLModel {

using std::string;

/**
 * Size (8bytes) | Content
 */
class SimpleBuffer {
   public:
    SimpleBuffer() = delete;
    SimpleBuffer(size_t size, string fn_prefix) : size_(size) {
        LOG(INFO) << "Init semaphore" << std::flush;
        s2m_sem_fn_ = fn_prefix + "_sys_to_model";
        s2m_sem_ = sem_open(s2m_sem_fn_.c_str(), O_CREAT | O_RDWR, 0666, 0);
        m2s_sem_fn_ = fn_prefix + "_model_to_sys";
        m2s_sem_ = sem_open(m2s_sem_fn_.c_str(), O_CREAT | O_RDWR, 0666, 0);

        model_ready_sem_fn_ = fn_prefix + "_model_ready";
        model_ready_sem_ = sem_open(model_ready_sem_fn_.c_str(), O_CREAT | O_RDWR, 0666, 0);
        sys_ready_sem_fn_ = fn_prefix + "_sys_ready";
        sys_ready_sem_ = sem_open(sys_ready_sem_fn_.c_str(), O_CREAT | O_RDWR, 0666, 0);

        LOG(INFO) << "Open data region of shared memory" << std::flush;
        string shm_name = fn_prefix + "_simple_buffer";
        int fd = shm_open(shm_name.c_str(), O_CREAT | O_RDWR, 0666);
        if (fd == -1) {
            LOG(ERROR) << "shm_open failed" << std::flush;
            exit(1);
        }

        if (ftruncate(fd, size_) == -1) {
            LOG(ERROR) << "ftruncate failed" << std::flush;
            exit(1);
        }

        LOG(INFO) << "Map shared memory to process address space" << std::flush;
        buffer_ = static_cast<char*>(mmap(NULL, size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
    }

    void write(const char* data, size_t size) {
        LOG(INFO) << "Write data to shared memory" << std::flush;
        if (size > size_) {
            LOG(ERROR) << "Data size is larger than shared memory size" << std::flush;
            exit(1);
        }
        memcpy(buffer_, &size, 8);
        memcpy(buffer_ + 8, data, size);
        sem_post(s2m_sem_);  // notify consumer
    }

    void read(char* data) {
        sem_wait(m2s_sem_);  // wait for model
        LOG(INFO) << "Read data from shared memory" << std::flush;
        size_t size;
        memcpy(&size, buffer_, 8);
        LOG(INFO) << "Got data size: " << size << std::flush;
        memcpy(data, buffer_ + 8, size);
    }

    void notified() {
        sem_wait(m2s_sem_);
        LOG(INFO) << "Notified by model" << std::flush;
    }

    void system_ready() {
        sem_post(sys_ready_sem_);
        LOG(INFO) << "Notify model that system is ready" << std::flush;
    }

   private:
    char* buffer_;
    size_t size_;

    sem_t* s2m_sem_;
    sem_t* m2s_sem_;
    sem_t* model_ready_sem_;
    sem_t* sys_ready_sem_;
    string s2m_sem_fn_;
    string m2s_sem_fn_;
    string model_ready_sem_fn_;
    string sys_ready_sem_fn_;
};

}  // namespace MLModel
}  // namespace AGE
