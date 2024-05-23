from random import sample
import numpy as np

MIN_QPS = 1000
MAX_QPS = 3000
MIN_NUM = 120
MAX_NUM = 180

NUM_WORKLOADS = 6

def generate_balanced_ratios(selected_items, min_ratios):
    num_selected = len(selected_items)
    remaining_ratio = 1 - sum(min_ratios)
    
    if num_selected == 2:
        # Directly adjust the remaining ratio to evenly distribute it, if only 2 items are selected
        extra_ratio_per_item = remaining_ratio / 2
        final_ratios = [min_ratio + extra_ratio_per_item for min_ratio in min_ratios]
    else:
        # For more than 2 items, use a random distribution as before
        additional_ratios = np.random.random(num_selected-1)
        additional_ratios /= additional_ratios.sum() / remaining_ratio
        additional_ratios = np.append(additional_ratios, remaining_ratio - additional_ratios.sum())
        final_ratios = min_ratios + additional_ratios
    
    return final_ratios

def generate_selected_items_and_balanced_ratios(S, min_ratios, num_items=0):
    # Select a random number of items between 2 and 5
    num_selected = np.random.randint(2, 6)
    if num_items != 0:
        num_selected = num_items
    # Randomly select items from the set
    selected_items = sample(S, num_selected)
    
    # Corresponding minimum ratios for the selected items
    selected_min_ratios = [min_ratios[S.index(item)] for item in selected_items]
    
    # Generate balanced final ratios
    final_ratios = generate_balanced_ratios(selected_items, selected_min_ratios)
    
    return selected_items, final_ratios

# Run the function to generate selected items and their balanced ratios

# Amazon
S = ['ac1', 'ac2', 'ac3', 'ac4', 'ac5', 'ac6', 'ac7']
min_ratios = [1/10, 1/10, 1/10, 1/10, 1/10, 1/10, 1/10]
# LDBC
# S = ['ic2', 'ic5', 'ic6', 'ic7', 'ic8', 'ic9', 'ic11', 'ic12']
# min_ratios = [1/10, 1/10, 1/10, 1/10, 1/10, 1/10, 1/10, 1/10]

for i in range(NUM_WORKLOADS):
    cur_num = np.random.randint(MIN_NUM, MAX_NUM)
    cur_qps = np.random.randint(MIN_QPS, MAX_QPS)
    num_queries = np.random.randint(2, len(S))
    print(cur_num, cur_qps, num_queries)
    selected_items_balanced, final_ratios_balanced = generate_selected_items_and_balanced_ratios(S, min_ratios, num_queries)
    final_ratios_balanced = [x * cur_num for x in final_ratios_balanced]
    print(selected_items_balanced, final_ratios_balanced, sum(final_ratios_balanced), cur_qps)