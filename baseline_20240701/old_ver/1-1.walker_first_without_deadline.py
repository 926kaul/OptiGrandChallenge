from util import *
import gurobipy as gp
import random

debugging = True
def get_solution(K:int, all_orders:list[Order], rider:Rider, available_number:int, used_order:list[int]):
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
        volume_constraint = volume_constraint + ord.volume * list_var[i] #sum of rider's order volume
        number_constraint = number_constraint + list_var[i] #number of rider's order
        i += 1
   
    for j in used_order:
        used_order_constraint = used_order_constraint + list_var[j] # there should be no used order (duplicated order)
   
    objective_function = 0
   
    m.setObjective(number_constraint, gp.GRB.MAXIMIZE)

    # Add constraints
    m.addConstr(volume_constraint <= rider.capa)
    if available_number < 5:
        m.addConstr(number_constraint <= available_number)
    else:
        m.addConstr(number_constraint <= 5)
   
    m.addConstr(used_order_constraint == 0)

    # Solve it!
    m.setParam('LogFile', 'gurobi.log')
    m.optimize()
    sol_seq = []
    number_of_one = 0
    print(list_var)
    for i in range(K):
        if list_var[i].X == 1:
            sol_seq.append(i)
            number_of_one += 1
           
    
    return sol_seq, number_of_one
   


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
        sequence, num = get_solution(K, all_orders, walk_rider, walk_available_number, used_order)
        used_order += sequence

        if num!=0:
            new_bundle = Bundle(all_orders, walk_rider, sequence, sequence, get_total_volume(all_orders,sequence), get_total_distance(K,dist_mat,sequence,sequence))
            walk_available_number -= num
            all_bundles.append(new_bundle)
        if len(used_order)==K:
            return [[bundle.rider.type, bundle.shop_seq, bundle.dlv_seq] for bundle in all_bundles]

    while bike_available_number>0:
        if time.time() - start_time > timelimit:
                break
        sequence, num = get_solution(K, all_orders, bike_rider, bike_available_number, used_order)
        used_order += sequence

        if num!=0:
            new_bundle = Bundle(all_orders, bike_rider, sequence, sequence, get_total_volume(all_orders,sequence), get_total_distance(K,dist_mat,sequence,sequence))
            bike_available_number -= num
            all_bundles.append(new_bundle)
        if len(used_order)==K:
            return [[bundle.rider.type, bundle.shop_seq, bundle.dlv_seq] for bundle in all_bundles]

    while car_available_number>0:
        if time.time() - start_time > timelimit:
                break
        sequence, num = get_solution(K, all_orders, car_rider, car_available_number, used_order)
        used_order += sequence

        if num!=0:
            new_bundle = Bundle(all_orders, car_rider, sequence, sequence, get_total_volume(all_orders,sequence), get_total_distance(K,dist_mat,sequence,sequence))
            car_available_number -= num
            all_bundles.append(new_bundle)
        if len(used_order)==K:
            return [[bundle.rider.type, bundle.shop_seq, bundle.dlv_seq] for bundle in all_bundles]
    
    return [[bundle.rider.type, bundle.shop_seq, bundle.dlv_seq] for bundle in all_bundles]



