from util import *
from itertools import permutations
import gurobipy as gp
from gurobipy import GRB, abs_
import sys
import random

debugging = True
def get_solution(K:int, all_orders:list[Order], rider:Rider, used_order:list[int], dist_mat, maxnum_order, sorted_index, blacklist = [],trial = 0):
    if trial > K:
        return [], []
    # Create a new model
    m = gp.Model()

    # Create variables
    list_var = []
    for i in range(K):
        list_var.append(m.addVar(vtype='B'))
    
    volume_constraint = 0
    number_constraint = 0
    used_order_constraint = 0

    i = 0
    for ord in all_orders:
        volume_constraint += ord.volume * list_var[i] #sum of rider's order volume
        number_constraint += list_var[i] #number of rider's order
        i += 1
        
    for j in used_order:
        used_order_constraint += list_var[j] # there should be no used order (duplicated order)

    maxi = m.addVar(vtype=GRB.INTEGER,name="maxi",lb=0)
    mini = m.addVar(vtype=GRB.INTEGER,name="mini",lb=0)

    for i in range(K):
        m.addConstr(maxi>=sorted_index[i]*list_var[i])
        m.addConstr(mini<=sorted_index[i]*list_var[i])

    objective_function = maxi-mini
    m.setObjective(objective_function, GRB.MINIMIZE)

    # Add constraints
    m.addConstr(volume_constraint <= rider.capa)
    m.addConstr(number_constraint == maxnum_order)
    m.addConstr(used_order_constraint == 0)

    for i in range(len(blacklist)//2):
        m.addConstr(list_var[blacklist[2*i]] + list_var[blacklist[2*i+1]] <= 1)

    # Solve it!
    m.optimize()

    if m.Status == GRB.OPTIMAL:
        tmp_seq = [i for i in range(K) if list_var[i].X==1]
        for shop_perm in permutations(tmp_seq):
            for dlv_perm in permutations(tmp_seq):
                if test_route_feasibility(all_orders, rider, shop_perm, dlv_perm) == 0:
                    return list(shop_perm), list(dlv_perm)

        blacklist_max = max(tmp_seq,key=lambda x:sorted_index[x])
        blacklist_min = min(tmp_seq,key=lambda x:sorted_index[x])
        return get_solution(K,all_orders,rider,used_order,dist_mat,maxnum_order,sorted_index,blacklist+[blacklist_max,blacklist_min],trial+1)
    else:
        return [], []


def algorithm(K, all_orders, all_riders, dist_mat, timelimit=60):

    start_time = time.time()
    sorted_list_with_indices = sorted(enumerate(all_orders), key=lambda x:x[1].ready_time, reverse=True)
    sorted_index = [0] * K
    for sorted_i, (original_index, value) in enumerate(sorted_list_with_indices):
        sorted_index[original_index] = sorted_i

    for r in all_riders:
        r.T = np.round(dist_mat/r.speed + r.service_time)

    # A solution is a list of bundles
    solution = []
    avg_cost = sys.maxsize

    #------------- Custom algorithm code starts from here --------------#

    rider = {}
    for r in all_riders:
        if r.type == 'WALK':
            rider["walk"] = r
        if r.type == 'BIKE':
            rider["bike"] = r
        if r.type == 'CAR':
            rider["car"] = r


    #searching_order = [("car",4),("bike",3),("car",3),("walk",2),("bike",2),("walk",1),("car",2),("bike",1)]

    searching_orders = [
        #[("bike",4), ("bike",3),("car",3),("car",2),("bike",2),("walk",1),("bike",1),("car",1)],
        #[("car",4),("walk",2),("bike",3),("car",3),("bike",2),("walk",1),("car",2),("bike",1)],
        [("bike",4),("car",4),("bike",3),("car",3),("bike",2),("car",2),("walk",1),("bike",1)]
    ]

    for searching_order in searching_orders:
        all_bundles = []
        used_order = []
        available_number = {"walk":rider["walk"].available_number,"bike":rider["bike"].available_number,"car":rider["car"].available_number}
        searching_order_done = False
        for wbc, n_order in searching_order:
            while available_number[wbc]>0:
                if time.time() - start_time > timelimit:
                        break
                shop_pem, dlv_pem = get_solution(K, all_orders, rider[wbc], used_order, dist_mat, n_order, sorted_index)
                used_order += shop_pem

                if shop_pem:
                    new_bundle = Bundle(all_orders, rider[wbc], shop_pem, dlv_pem, get_total_volume(all_orders,shop_pem), get_total_distance(K,dist_mat,shop_pem, dlv_pem))
                    available_number[wbc] -= 1
                    all_bundles.append(new_bundle)
                else:
                    break
                if len(used_order)==K:
                    searching_order_done = True
                    break

            if searching_order_done:
                if avg_cost > get_avg_cost(all_orders, all_bundles):
                    solution = [[bundle.rider.type, bundle.shop_seq, bundle.dlv_seq] for bundle in all_bundles]
                break

        if len(used_order)!=K:
            for i in range(K):
                if i not in used_order:
                    new_bundle = Bundle(all_orders, rider["car"], [i], [i], get_total_volume(all_orders,[i]), get_total_distance(K,dist_mat,[i], [i]))
                    all_bundles.append(new_bundle)
            if avg_cost > get_avg_cost(all_orders, all_bundles):
                    solution = [[bundle.rider.type, bundle.shop_seq, bundle.dlv_seq] for bundle in all_bundles]
        
    return solution



