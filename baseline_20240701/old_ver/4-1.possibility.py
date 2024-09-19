from util import *
from itertools import permutations
import gurobipy as gp
from gurobipy import GRB, abs_
import sys
import random

debugging = True
def get_solution(K:int, all_orders:list[Order], sorted_orders:list[list[int,Order,int]], rider:Rider, used_order:set[int], dist_mat, pivot, set_number=3):
    possibility_index = sorted_orders[pivot][2]
    if set_number == 1:
        if test_route_feasibility(all_orders, rider, [sorted_orders[pivot][0]], [sorted_orders[pivot][0]]) == 0:
            return [sorted_orders[pivot][0]], [sorted_orders[pivot][0]], [pivot]
        return [], [], []
    # Create a new model
    m = gp.Model()

    # Create variables
    list_var = []
    for i in range(pivot,possibility_index+1):
        list_var.append(m.addVar(vtype='B'))
    
    volume_constraint = 0
    number_constraint = 0
    used_order_constraint = 0

    for i in range(pivot,possibility_index+1):
        volume_constraint += sorted_orders[i][1].volume * list_var[i-pivot] #sum of rider's order volume
        number_constraint += list_var[i-pivot] #number of rider's order
        
    for j in used_order:
        if pivot <= j <= possibility_index:
            used_order_constraint += list_var[j-pivot] # there should be no used order (duplicated order)

    objective_function = number_constraint
    m.setObjective(objective_function, GRB.MAXIMIZE)

    # Add constraints
    m.addConstr(volume_constraint <= rider.capa)
    m.addConstr(number_constraint == set_number)
    m.addConstr(used_order_constraint == 0)
    m.addConstr(list_var[0] == 1)

    # Solve it!
    m.optimize()

    if m.Status == GRB.OPTIMAL:
        tmp_seq = [sorted_orders[i][0] for i in range(pivot,possibility_index+1) if list_var[i-pivot].X==1]
        sorted_used = [i for i in range(pivot, possibility_index+1) if list_var[i-pivot].X==1]
        for shop_perm in permutations(tmp_seq):
            for dlv_perm in permutations(tmp_seq):
                if test_route_feasibility(all_orders, rider, shop_perm, dlv_perm) == 0:
                    return list(shop_perm), list(dlv_perm), sorted_used

        return get_solution(K,all_orders,sorted_orders,rider,used_order,dist_mat,pivot,set_number-1)
    else:
        return [], [], []


def algorithm(K, all_orders, all_riders, dist_mat, timelimit=60):

    start_time = time.time()
    sorted_orders = [[i,all_orders[i],-1] for i in range(K)] # [original index, Order, possibility index]
    sorted_orders.sort(key=lambda x:x[1].ready_time)

    for i in range(K):
        possibility_index = i
        while True:
            if possibility_index >= K:
                possibility_index = K-1
                break
            if sorted_orders[i][1].deadline < sorted_orders[possibility_index][1].ready_time:
                possibility_index -= 1
                if possibility_index < i:
                    possibility_index = i
                break
            possibility_index += 1
        
        sorted_orders[i][2] = possibility_index

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

    all_bundles = []
    used_order = set()
    available_number = {"walk":rider["walk"].available_number,"bike":rider["bike"].available_number,"car":rider["car"].available_number}
    for i in range(K):
        if time.time() - start_time > timelimit:
            break
        if i in used_order:
            continue
        if sorted_orders[i][1].volume <= rider["walk"].capa//2 and available_number["walk"] > 0:
            wbc = "walk"
        elif sorted_orders[i][1].volume <= rider["bike"].capa//2 and available_number["bike"] > 0:
            wbc = "bike"
        else:
            wbc = "car"
        shop_pem, dlv_pem, new_used_orders = get_solution(K, all_orders, sorted_orders, rider[wbc], used_order, dist_mat, i)

        used_order.update(new_used_orders)
        if shop_pem:
            new_bundle = Bundle(all_orders, rider[wbc], shop_pem, dlv_pem, get_total_volume(all_orders,shop_pem), get_total_distance(K,dist_mat,shop_pem, dlv_pem))
            available_number[wbc] -= 1
            all_bundles.append(new_bundle)

    if len(used_order)!=K:
        for i in range(K):
            if i not in used_order:
                tmpi = sorted_orders[i][0]
                new_bundle = Bundle(all_orders, rider["car"], [tmpi], [tmpi], get_total_volume(all_orders,[tmpi]), get_total_distance(K,dist_mat,[tmpi], [tmpi]))
                all_bundles.append(new_bundle)
    solution = [[bundle.rider.type, bundle.shop_seq, bundle.dlv_seq] for bundle in all_bundles]
        
    return solution



