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
#include <cmath>
#include <iostream>

#pragma once

namespace AGE {
namespace MLModel {

class OptTarget {
   public:
    OptTarget() : lat_weight(1), qos_weight(1) {}
    OptTarget(double min, double max, double lat_weight, double qos_weight)
        : min(min), max(max), lat_weight(lat_weight), qos_weight(qos_weight) {}

    double calculate_target(double qos, double tail_lat) {
        double scaled_lat = (tail_lat - min) / (max - min);
        // double tail_lat_side = 1.0 - 1.0 / exp(scaled_lat);
        double target = scaled_lat + qos / 100.0;
        // LOG(INFO) << "tail_lat_side: " << scaled_lat << ", qos: " << qos << ", target: " << target;
        return target;
    }

   private:
    double min;
    double max;
    double lat_weight;
    double qos_weight;
};

}  // namespace MLModel
}  // namespace AGE
