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

#include <cstdint>
#include <cstring>
#include <string>

namespace AGE {
namespace MLModel {

class RingBuffer {
   public:
    RingBuffer(char* buffer, size_t size) : buffer_(buffer), size_(size), head_(0), tail_(0) {}

    bool write(const void* data, size_t size) {
        if (size > size_) return false;

        // Calculate the space available in the buffer
        size_t spaceAvailable = (tail_ >= head_) ? size_ - (tail_ - head_) : head_ - tail_;

        if (size > spaceAvailable) {
            return false;  // Not enough space in the buffer
        }

        // Write data to the buffer
        if (tail_ + size <= size_) {
            std::memcpy(buffer_ + tail_, data, size);
        } else {
            // Wrap around to the beginning of the buffer
            size_t firstPartSize = size_ - tail_;
            std::memcpy(buffer_ + tail_, data, firstPartSize);
            std::memcpy(buffer_, static_cast<const char*>(data) + firstPartSize, size - firstPartSize);
        }

        // Move the tail forward
        tail_ = (tail_ + size) % size_;

        return true;
    }

    bool read(void* data, size_t size) {
        if (size > size_) {
            return false;  // Requested size exceeds buffer size
        }

        // Calculate the available data in the buffer
        size_t dataAvailable = (tail_ >= head_) ? tail_ - head_ : size_ - (head_ - tail_);

        if (size > dataAvailable) {
            return false;  // Not enough data in the buffer
        }

        // Read data from the buffer
        if (head_ + size <= size_) {
            std::memcpy(data, buffer_ + head_, size);
        } else {
            // Wrap around to the beginning of the buffer
            size_t firstPartSize = size_ - head_;
            std::memcpy(data, buffer_ + head_, firstPartSize);
            std::memcpy(static_cast<char*>(data) + firstPartSize, buffer_, size - firstPartSize);
        }

        // Move the head forward
        head_ = (head_ + size) % size_;
        return true;
    }

    size_t size() const { return size_; }
    size_t head() const { return head_; }
    size_t tail() const { return tail_; }
    void set_head(size_t head) { head_ = head; }
    void set_tail(size_t tail) { tail_ = tail; }

   private:
    char* buffer_;
    size_t size_;
    size_t head_;
    size_t tail_;
};

}  // namespace MLModel
}  // namespace AGE
