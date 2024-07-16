from util import *
from itertools import permutations
import gurobipy as gp
from gurobipy import GRB
import random

debugging = True
def get_solution(K:int, all_orders:list[Order], rider:Rider, used_order:list[int],maxnum_order=4):
    if maxnum_order==0:
        return [],[]
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
   
    objective_function = 0
   
    m.setObjective(number_constraint, gp.GRB.MAXIMIZE)

    # Add constraints
    m.addConstr(volume_constraint <= rider.capa)
    m.addConstr(number_constraint <= min(5,maxnum_order))
   
    m.addConstr(used_order_constraint == 0)

    # Solve it!
    m.optimize()
    tmp_seq = [i for i in range(K) if list_var[i].X==1]
    for shop_perm in permutations(tmp_seq):
        for dlv_perm in permutations(tmp_seq):
            with open("loglog.txt", "a") as f:
                f.write(" ".join(map(str,list(shop_perm))) + "\n")
            if test_route_feasibility(all_orders, rider, shop_perm, dlv_perm) == 0:
                return list(shop_perm), list(dlv_perm)

    return get_solution(K,all_orders,rider,used_order,maxnum_order-1)
   


def algorithm(K, all_orders:list[Order], all_riders:list[Rider], dist_mat, timelimit=60):

    start_time = time.time()

    for r in all_riders:
        r.T = np.round(dist_mat/r.speed + r.service_time)

    # A solution is a list of bundles
    solution = []

    #------------- Custom algorithm code starts from here --------------#

    car_rider = None
    for r in all_riders:
        if r.type == 'WALK':
            walk_rider = r
        if r.type == 'BIKE':
            bike_rider = r
        if r.type == 'CAR':
            car_rider = r
   
    all_bundles = []
    walk_available_number = walk_rider.available_number
    bike_available_number = bike_rider.available_number
    car_available_number = car_rider.available_number
   
    used_order = []
    while walk_available_number>0:
        if time.time() - start_time > timelimit:
                break
        shop_pem, dlv_pem = get_solution(K, all_orders, walk_rider, used_order)
        used_order += shop_pem

        if shop_pem:
            new_bundle = Bundle(all_orders, walk_rider, shop_pem, dlv_pem, get_total_volume(all_orders,shop_pem), get_total_distance(K,dist_mat,shop_pem, dlv_pem))
            walk_available_number -= 1
            all_bundles.append(new_bundle)
        else:
            break
        if len(used_order)==K:
            return [[bundle.rider.type, bundle.shop_seq, bundle.dlv_seq] for bundle in all_bundles]

    while bike_available_number>0:
        if time.time() - start_time > timelimit:
                break
        shop_pem, dlv_pem = get_solution(K, all_orders, bike_rider, used_order)
        used_order += shop_pem

        if shop_pem:
            new_bundle = Bundle(all_orders, bike_rider, shop_pem, dlv_pem, get_total_volume(all_orders,shop_pem), get_total_distance(K,dist_mat,shop_pem, dlv_pem))
            bike_available_number -= 1
            all_bundles.append(new_bundle)
        else:
            break
        if len(used_order)==K:
            return [[bundle.rider.type, bundle.shop_seq, bundle.dlv_seq] for bundle in all_bundles]

    while car_available_number>0:
        if time.time() - start_time > timelimit:
                break
        shop_pem, dlv_pem = get_solution(K, all_orders, car_rider, used_order)
        used_order += shop_pem

        if shop_pem:
            new_bundle = Bundle(all_orders, car_rider, shop_pem, dlv_pem, get_total_volume(all_orders,shop_pem), get_total_distance(K,dist_mat,shop_pem, dlv_pem))
            car_available_number -= 1
            all_bundles.append(new_bundle)
        if len(used_order)==K:
            return [[bundle.rider.type, bundle.shop_seq, bundle.dlv_seq] for bundle in all_bundles]
    
    return [[bundle.rider.type, bundle.shop_seq, bundle.dlv_seq] for bundle in all_bundles]



