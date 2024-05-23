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

#include "model/ring_buffer.h"

namespace AGE {
namespace MLModel {

using std::string;

/**
 * Head(8bytes) | Tail(8bytes) | Ring Buffer
 * This SharedBuffer is one-side. The C++ side will be only for producer or consumer.
 * That is, only one semaphore is required.
 */
class SharedBuffer {
   public:
    SharedBuffer() = delete;
    SharedBuffer(size_t buffer_size, string fn_prefix) {
        LOG(INFO) << "Init semaphore" << std::flush;
        rw_sem_fn_ = fn_prefix + "_rw";
        rw_sem_ = sem_open(rw_sem_fn_.c_str(), O_CREAT | O_RDWR, 0666, 0);
        head_sem_fn_ = fn_prefix + "_head";
        head_sem_ = sem_open(head_sem_fn_.c_str(), O_CREAT | O_RDWR, 0666, 1);

        LOG(INFO) << "Open data region of shared memory" << std::flush;
        int fd = shm_open("shared_buffer", O_CREAT | O_RDWR, 0666);
        if (fd == -1) {
            perror("shm_open");
            exit(1);
        }

        if (ftruncate(fd, buffer_size + 16) == -1) {
            perror("ftruncate");
            exit(1);
        }

        LOG(INFO) << "Map to shared memory" << std::flush;
        buf_start_ = mmap(NULL, buffer_size + 16, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        LOG(INFO) << "buf: " << buf_start_ << std::flush;
        if (buf_start_ == MAP_FAILED) {
            perror("mmap");
            exit(1);
        }

        shared_head_ = static_cast<char*>(buf_start_);
        shared_tail_ = static_cast<char*>(buf_start_) + 8;
        buffer_ = new RingBuffer(static_cast<char*>(buf_start_) + 16, buffer_size);

        LOG(INFO) << "Init head and tail to zero" << std::flush;
        size_t init = 0;
        std::memcpy(shared_head_, &init, 8);
        std::memcpy(shared_tail_, &init, 8);
    }

    ~SharedBuffer() {
        LOG(INFO) << "~SharedBuffer()";
        sem_close(rw_sem_);
        // LOG(INFO) << "close";
        // sem_unlink(rw_sem_fn_.c_str());
        // LOG(INFO) << "unlink";
        sem_close(head_sem_);
        // sem_unlink(head_sem_fn_.c_str());
        size_t buffer_size = buffer_->size();
        // LOG(INFO) << "delete";
        delete buffer_;

        LOG(INFO) << buf_start_ << std::flush;
        // munmap(buf_start_, buffer_size + 16);
        // shm_unlink("shared_buffer");
    }

    void write(const void* data, size_t size) {
        // get updated head
        sem_wait(head_sem_);
        uint64_t head = *reinterpret_cast<uint64_t*>(shared_head_);
        sem_post(head_sem_);
        LOG(INFO) << "Get updated head " << std::to_string(head);

        buffer_->set_head(head);
        if (!buffer_->write(data, size)) {
            LOG(ERROR) << "Buffer is full" << std::flush;
        }
        uint64_t new_tail = buffer_->tail();
        std::memcpy(shared_tail_, &new_tail, 8);
        sem_post(rw_sem_);
    }

    void read(void* data, size_t size) {
        sem_wait(rw_sem_);

        if (!buffer_->read(data, size)) {
            LOG(ERROR) << "Read failed" << std::flush;
        }

        uint64_t new_head = buffer_->head();
        sem_wait(head_sem_);
        std::memcpy(shared_head_, &new_head, 8);
        sem_post(head_sem_);
    }

    void check_mem() {
        uint64_t head = *reinterpret_cast<uint64_t*>(shared_head_);
        uint64_t tail = *reinterpret_cast<uint64_t*>(shared_tail_);

        LOG(INFO) << "Head: " << std::to_string(head);
        LOG(INFO) << "Tail: " << std::to_string(tail);
    }

   private:
    char* shared_head_;
    char* shared_tail_;
    void* buf_start_;
    sem_t* rw_sem_;
    sem_t* head_sem_;
    string rw_sem_fn_;
    string head_sem_fn_;

    RingBuffer* buffer_;
};

}  // namespace MLModel
}  // namespace AGE
