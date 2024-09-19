from util import *
from multiprocessing import Process, Queue
import gurobipy as gp
from gurobipy import GRB
from collections import defaultdict, deque
import itertools
import sys


# 가능한 order 조합을 찾으면 possible_bundle 클래스로 저장하여 활용한다
# pickling 가능하게 하기 위해 init을 제외한 메소드를 사용하지 않음
class possible_bundle:
    def __init__(self, rider : str, shop_seq, dlv_seq, cost):
        self.rider = rider
        self.shop_seq = shop_seq
        self.dlv_seq = dlv_seq
        self.cost = cost


def worker(task_queue, result_queue, K, all_orders, all_riders, dist_mat, start_time, timelimit):
    print('worker started')
    
    rider = {}
    for r in all_riders:
        r = Rider([r.type, r.speed, r.capa, r.var_cost, r.fixed_cost, r.service_time, r.available_number])
        if r.type == 'WALK':
            rider["WALK"] = r
        if r.type == 'CAR':
            rider["CAR"] = r
        if r.type == 'BIKE':
            rider["BIKE"] = r
        
        r.T = np.round(dist_mat/r.speed + r.service_time)
    
    task_count = 0

    while True:
        if time.time() - start_time > timelimit:
            print('worker finished')
            return

        try:
            task = task_queue.get()
            task_count += len(task)
            print('task received', task_count)
        except:
            print('no more work')
            continue

        result = []
        
        for task_tuple in task:
            order_id, order1, order2 = task_tuple
            tmp_tuple = tuple(sorted([order_id, order1, order2]))

            tmp_dist = dist_mat[order_id][order1] + dist_mat[order_id][order2] + dist_mat[order1][order2] - max(dist_mat[order_id][order1],dist_mat[order_id][order2],dist_mat[order1][order2])
            tmp_min_shop_dlv = min([dist_mat[shop][dlv+K] for shop in tmp_tuple for dlv in tmp_tuple])
            tmp_dist += min([dist_mat[shop][dlv+K] for shop in tmp_tuple for dlv in tmp_tuple])
            tmp_dist += dist_mat[order_id+K][order1+K] + dist_mat[order_id+K][order2+K] + dist_mat[order1+K][order2+K] - max(dist_mat[order_id+K][order1+K],dist_mat[order_id+K][order2+K],dist_mat[order1+K][order2+K])
            min_readytime = min([all_orders[odid].ready_time for odid in tmp_tuple])
            max_deadline = max([all_orders[odid].deadline for odid in tmp_tuple])
            max_readytime = max([all_orders[odid].ready_time for odid in tmp_tuple])
            min_deadline = min([all_orders[odid].deadline for odid in tmp_tuple])

            for rider_type, rider_inst in rider.items():
                optimal_route_found = None
                cost = np.inf
                tmp_time = tmp_dist/rider_inst.speed + 5*rider_inst.service_time

                total_vol = get_total_volume(all_orders, tmp_tuple)
                if total_vol > rider_inst.capa:
                    continue
                
                if tmp_time <= (max_deadline-min_readytime) and tmp_min_shop_dlv/rider_inst.speed + rider_inst.service_time <= (min_deadline-max_readytime):
                    for shop_seq in permutations(task_tuple):
                        for dlv_seq in permutations(task_tuple):
                            if test_route_feasibility(all_orders, rider_inst, shop_seq, dlv_seq) == 0:
                                cur_cost = rider_inst.calculate_cost(get_total_distance(K, dist_mat, shop_seq, dlv_seq))
                                if cur_cost < cost:
                                    cost = cur_cost
                                    optimal_route_found = possible_bundle(rider_type, shop_seq, dlv_seq, cur_cost)
                
                if optimal_route_found:
                    result.append(optimal_route_found)
        
        result_queue.put(result)




# 찾은 bundle 중에서 최적화된 route를 찾는 함수 
# p0이 사용함
def get_solution(K,available_numbers, optimal_route):

    # set up gurobi
    m = gp.Model()
    m.setParam('OutputFlag', 0)

    h = len(optimal_route)

    # Create variables
    list_var = []
    for j in range(h):
        list_var.append(m.addVar(vtype='B'))

    # [rider_type, shop_seq, dlv_seq, cur_cost]
    
    for i in range(K):
        used_order_constraint = 0
        for j in range(h):
            if i in optimal_route[j].shop_seq:
                used_order_constraint += list_var[j]
        m.addConstr(used_order_constraint == 1)

    bike_constraint = 0
    walk_constraint = 0
    car_constraint = 0
    for j in range(h):
        if optimal_route[j].rider == "BIKE":
            bike_constraint += list_var[j]
        elif optimal_route[j].rider == "WALK":
            walk_constraint += list_var[j]
        elif optimal_route[j].rider == "CAR":
            car_constraint += list_var[j]
    m.addConstr(bike_constraint <= available_numbers["BIKE"])
    m.addConstr(walk_constraint <= available_numbers["WALK"])
    m.addConstr(car_constraint <= available_numbers["CAR"])
        

    objective_function = 0
    for j in range(h):
        objective_function += list_var[j] * optimal_route[j].cost
    m.setObjective(objective_function, GRB.MINIMIZE)

    # Solve it!
    m.optimize()

    if m.Status == GRB.OPTIMAL:
        return [[optimal_route[j].rider, optimal_route[j].shop_seq, optimal_route[j].dlv_seq] for j in range(h) if list_var[j].X==1], m.objVal
    else:
        return None, np.inf



def can_merge(K, all_orders, bundle_index, rider):
    shops = defaultdict(lambda : (np.inf, tuple(bundle_index)))
    dlvs = defaultdict(lambda : (0, tuple(bundle_index)))

    for shop_seq in permutations(bundle_index):
        k = shop_seq[0]
        t = all_orders[k].ready_time
        for next_k in shop_seq[1:]:
            t = max(t+rider.T[k, next_k], all_orders[next_k].ready_time)
            k = next_k
        
        shops[shop_seq[-1]] = min(shops[shop_seq[-1]], (t, shop_seq))
    
    for dlv_seq in permutations(bundle_index):
        k = dlv_seq[-1]
        t = all_orders[k].deadline
        for next_k in reversed(dlv_seq[:-1]):
            t = min(t-rider.T[next_k + K, k + K], all_orders[next_k].deadline)
            k = next_k
        
        dlvs[dlv_seq[0]] = max(dlvs[dlv_seq[0]], (t, dlv_seq))
    

    for shop_end in bundle_index:
        for dlv_start in bundle_index:
            shop_end_time, shop_seq = shops[shop_end]
            dlv_start_time, dlv_seq = dlvs[dlv_start]
            if shop_end_time + rider.T[shop_end, dlv_start+K] <= dlv_start_time:
                return shop_seq, dlv_seq
    
    return None, None


# 메인 함수 p0
def algorithm(K, all_orders, all_riders, dist_mat, timelimit=58):
    start_time = time.time()

    for r in all_riders:
        r.T = np.round(dist_mat/r.speed + r.service_time)


    #------------- Custom algorithm code starts from here --------------#

    rider = {}
    available_numbers = {}
    for r in all_riders:
        if r.type == 'WALK':
            rider["WALK"] = r
            available_numbers["WALK"] = r.available_number
        if r.type == 'CAR':
            rider["CAR"] = r
            available_numbers["CAR"] = r.available_number
        if r.type == 'BIKE':
            rider["BIKE"] = r
            available_numbers["BIKE"] = r.available_number

    task_queue = Queue()
    result_queue = Queue()
    
    merge_possible = {}
    merge_possible["WALK"] = defaultdict(lambda : set())
    merge_possible["CAR"] = defaultdict(lambda : set())
    merge_possible["BIKE"] = defaultdict(lambda : set())
    merge_possible["ALL"] = defaultdict(lambda : set())

    optimal_route = []

    for i in range(K):
        for r in all_riders:
            if test_route_feasibility(all_orders, r, [i], [i]) == 0:
                optimal_route.append(possible_bundle(r.type, [i], [i], r.calculate_cost(get_total_distance(K, dist_mat, [i], [i]))))
    
    # connectivity matrix 구하기
    for i in range(K):
        for j in range(i+1, K):
            optimal_route_found = {}
            cost = np.inf
            for shop_seq in permutations([i,j]):
                for dlv_seq in permutations([i,j]):
                    for rider_type, rider_inst in rider.items():
                        if test_route_feasibility(all_orders, rider[rider_type], shop_seq, dlv_seq) == 0:
                            merge_possible[rider_type][i].add(j)
                            merge_possible[rider_type][j].add(i)
                            merge_possible["ALL"][i].add(j)
                            merge_possible["ALL"][j].add(i)
                            cur_cost = rider_inst.calculate_cost(get_total_distance(K, dist_mat, shop_seq, dlv_seq))
                            if cur_cost < cost:
                                cost = cur_cost
                                optimal_route_found[rider_type] = possible_bundle(rider_type, shop_seq, dlv_seq, cur_cost)
            if optimal_route_found:
                optimal_route.extend(optimal_route_found.values())



    # workers은 현재 프로세스(p0)이 배정해준 3order bundle을 찾아주는 역할을 함
    # 현재 worker은 3명
    workers = [Process(target=worker, args=(task_queue, result_queue, K, all_orders, all_riders, dist_mat, start_time, timelimit)) for _ in range(3)]
    for w in workers:
        w.start()
    
    task_count = 0
    queue = []

    # p0의 task 배정 과정
    # considered는 중복된 task를 제거하기 위한 set
    # 모든 order에 대해 가능한 조합을 찾아서 10,000개를 묶어서 task_queue에 넣어줌
    # 묶지 않으면 성능저하가 일어남
    considered = set()
    rider_of_choice = "ALL"
    for order_id in range(K):
        for order1, order2 in itertools.combinations(merge_possible[rider_of_choice][order_id], 2):
            tmp_tuple = tuple(sorted([order_id, order1, order2]))
            if tmp_tuple in considered:
                continue
            considered.add(tmp_tuple)
            task_count += 1

            queue.append(tmp_tuple)

            if task_count % 10000 == 0:
                task_queue.put(queue)
                queue = []

    task_queue.put(queue)

    print('finished putting tasks', task_count)

    # 구로비가 마지막에 최적화를 찾기 위해 필요한 시간(초)
    time_for_gurobi = 15


    print('finding 4,5,6 order bundles', task_count)
    while True:
        while not result_queue.empty():
            optimal_route.extend(result_queue.get())
        
        if time.time() - start_time > timelimit - time_for_gurobi:
            break
        
        # 여기에 4 order 다른 프로세스에 넘겨주는 로직 구성할 필요가 있음
        
        


    print('get solution initiated', time.time()-start_time)

    solution = get_solution(K, available_numbers, optimal_route)

    #------------- End of custom algorithm code--------------/


    return [[rider, list(shop_seq), list(dlv_seq)] for rider, shop_seq, dlv_seq in solution[0]]


