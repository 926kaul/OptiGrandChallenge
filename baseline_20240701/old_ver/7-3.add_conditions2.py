from util import *
from multiprocessing import Process, Queue
import gurobipy as gp
from gurobipy import GRB
from collections import defaultdict
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

# rider_of_choice는 CAR, BIKE, ALL중 combination을 만들 때 사용할 rider 그래프를 결정 p1
# sparse graph(1_1, 1_2, 1_17 등)은 20초 안에 3개의 order합친 bundle을 전부 찾아냄 - 1_1로 테스트함
# dense graph(1_10, 1_18)은 1분 안에 3개의 order합친 bundle을 전부 찾아내지 못함 - 1_18로 테스트함
def merge(q, K, all_orders, all_riders, dist_mat, merge_possible, rider_of_choice, start_time, timelimit):
    print("merge set and ready to go")

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

    queue = []
    considered = set()
    
    for order_id in range(K):
        for order1, order2 in itertools.combinations(merge_possible[rider_of_choice][order_id], 2):
            tmp_tuple = tuple(sorted([order_id, order1, order2]))
            if tmp_tuple in considered:
                continue
            considered.add(tmp_tuple)

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

                if tmp_time <= (max_deadline-min_readytime) and tmp_min_shop_dlv/rider_inst.speed + rider_inst.service_time <= (min_deadline-max_readytime):
                    for shop_seq in permutations(tmp_tuple):
                        for dlv_seq in permutations(tmp_tuple):
                            if test_route_feasibility(all_orders, rider_inst, shop_seq, dlv_seq) == 0:
                                cur_cost = rider_inst.calculate_cost(get_total_distance(K, dist_mat, shop_seq, dlv_seq))
                                if cur_cost < cost:
                                    cost = cur_cost
                                    optimal_route_found = possible_bundle(rider_type, shop_seq, dlv_seq, cur_cost)
                
                if optimal_route_found:
                    queue.append(optimal_route_found)
        
        print("p1", order_id, end = ' ')

        if len(queue) > 1000:
            q.put(queue)
            queue = []
            print("sent", end=' ')
        
        print()
        
        
        if time.time() - start_time > timelimit:
            return
    
    q.put(queue)
    print("3 merge complete")

    rider.pop("WALK")

    # 3개의 order을 합친 bundle을 전부 찾으면
    # 다음으로 4개의 order을 합친 bundle 찾기
    rider_of_choice = "CAR"
    for order_id in range(K):
        for order1, order2 in itertools.combinations(merge_possible[rider_of_choice][order_id], 2):
            possible_order_set = merge_possible[rider_of_choice][order_id].intersection(merge_possible[rider_of_choice][order1]).intersection(merge_possible[rider_of_choice][order2])
            for order3 in possible_order_set:
                tmp_tuple = tuple(sorted([order_id, order1, order2, order3]))
                if tmp_tuple in considered:
                    continue
                considered.add(tmp_tuple)

                for rider_type, rider_inst in rider.items():
                    if test_route_feasibility(all_orders, rider_inst, tmp_tuple, tmp_tuple) == -1:
                        continue
                    optimal_route_found = None
                    cost = np.inf
                    for shop_seq in permutations(tmp_tuple):
                        for dlv_seq in permutations(tmp_tuple):
                            if test_route_feasibility(all_orders, rider_inst, shop_seq, dlv_seq) == 0:
                                cur_cost = rider_inst.calculate_cost(get_total_distance(K, dist_mat, shop_seq, dlv_seq))
                                if cur_cost < cost:
                                    cost = cur_cost
                                    optimal_route_found = possible_bundle(rider_type, shop_seq, dlv_seq, cur_cost)
                    
                    if optimal_route_found:
                        queue.append(optimal_route_found)
                
            if time.time() - start_time > timelimit:
                return
        
        print("p1", order_id, end = ' ')

        if len(queue) > 100:
            q.put(queue)
            queue = []
            print("sent", end=' ')
        
        print()
                    

# 4개 이상의 order를 합치는 함수 p2
# order K-1에서 시작해서 거슬러 올라감(merge함수가 3 order bundle 찾으면 order 0부터 시작)
# sparse graph는 1분 안에 merge, merge_4과 합쳐 100개 정도의 4 order bundle을 찾아냄 - 1_1로 테스트함
# dense graph는 1분 안에 단 하나의 4 order bundle을 찾아냄(즉, K-1만 찾아냄) - 1_18로 테스트함
def merge_4(q, K, all_orders, all_riders, dist_mat, merge_possible, rider_of_choice, start_time, timelimit):
    print("merge_4 set and ready to go")

    rider = {}
    for r in all_riders:
        r = Rider([r.type, r.speed, r.capa, r.var_cost, r.fixed_cost, r.service_time, r.available_number])
        if r.type == 'CAR':
            rider["CAR"] = r
        if r.type == 'BIKE':
            rider["BIKE"] = r
        
        r.T = np.round(dist_mat/r.speed + r.service_time)

    queue = []
    considered = set()

    for order_id in reversed(range(K)):
        for order1, order2 in itertools.combinations(merge_possible[rider_of_choice][order_id], 2):
            possible_order_set = merge_possible[rider_of_choice][order_id].intersection(merge_possible[rider_of_choice][order1]).intersection(merge_possible[rider_of_choice][order2])
            for order3 in possible_order_set:
                tmp_tuple = tuple(sorted([order_id, order1, order2, order3]))
                if tmp_tuple in considered:
                    continue
                considered.add(tmp_tuple)

                for rider_type, rider_inst in rider.items():
                    if test_route_feasibility(all_orders, rider_inst, tmp_tuple, tmp_tuple) == -1:
                        continue
                    optimal_route_found = None
                    cost = np.inf
                    for shop_seq in permutations(tmp_tuple):
                        for dlv_seq in permutations(tmp_tuple):
                            if test_route_feasibility(all_orders, rider_inst, shop_seq, dlv_seq) == 0:
                                cur_cost = rider_inst.calculate_cost(get_total_distance(K, dist_mat, shop_seq, dlv_seq))
                                if cur_cost < cost:
                                    cost = cur_cost
                                    optimal_route_found = possible_bundle(rider_type, shop_seq, dlv_seq, cur_cost)
                    
                    if optimal_route_found:
                        queue.append(optimal_route_found)
                
            if time.time() - start_time > timelimit:
                return
            

            if len(queue) > 200:
                queue = []
                break
        
        print("p2 sent", order_id, end = ' ')
        
        q.put(queue)
        
        
        print()


# 찾은 bundle 중에서 최적화된 route를 찾는 함수 
# p0이 사용함
def get_solution(K,available_numbers, optimal_route,started_time,timelimit):
    #print(len(optimal_route))
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

    m.setParam('TimeLimit', max(0,timelimit-(time.time()-started_time)))
    # Solve it!
    m.optimize()

    if m.Status == GRB.OPTIMAL:
        return [[optimal_route[j].rider, optimal_route[j].shop_seq, optimal_route[j].dlv_seq] for j in range(h) if list_var[j].X==1], m.objVal
    else:
        return None, np.inf



# 구로비로 최대의 complete graph 찾기 
# p3에서 사용됨
#
#
# 해당 코드는 버그가 있음
# matrix가 walk 그래프 기준으로 되어 있음
# 그러기에 rider_of_choice가 walk일 때만 정상적으로 작동함
#
#
#
#
def find_max_complete_graph(merge_possible, matrix, rider_of_choice, max_index):

    m = gp.Model()
    m.setParam('OutputFlag', 0)
    
    key_map = dict()
    sum_var = 0
    for i in merge_possible[rider_of_choice][max_index]:
        key_map[i] = m.addVar(vtype='B')
        sum_var += key_map[i]
    for i, j in itertools.combinations(merge_possible[rider_of_choice][max_index], 2):
        edge = m.addVar(vtype='B')
        m.addConstr(edge * (1-matrix[i][j]) == 0)
        m.addConstr(edge >= key_map[i] + key_map[j] - 1)
    
    m.setObjective(sum_var, GRB.MAXIMIZE)

    m.optimize()

    if m.Status == GRB.OPTIMAL:
        tmp_seq = [i for i, val in key_map.items() if val.X==1]
        
        return len(tmp_seq) + 1
    
    return None



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



# 최대로 묶일 수 있는 번들부터 찾는 함수 p3
# 1_18은 여러개를 찾고 1개의 결과만이 활용됨
# 1_1은 제대로 작동하지 않음(버그 해결 필요)
def top_down_merge(q, K, matrix, matrix_all, all_orders, dist_mat, all_riders, merge_possible, start_time, timelimit):
    print("top_down_merge set and ready to go")

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

    rider_of_choice = "WALK"

    # it just returns the route that comes first so optimation can be done
    def make_n_len_route(n, rider_of_chocie, index):
        for combi_list in itertools.combinations(merge_possible[rider_of_choice][index], n):
            tmp_set = merge_possible[rider_of_chocie][combi_list[0]]
            for i in combi_list:
                tmp_set = tmp_set & merge_possible[rider_of_chocie][i]
            if not tmp_set:
                continue

            combi_list = list(combi_list)
            combi_list.append(index)
            
            if test_route_feasibility(all_orders, rider["CAR"], combi_list, combi_list) != -1:
                shop_seq, dlv_seq = can_merge(K, all_orders, combi_list, rider["CAR"])
                if shop_seq:
                    return possible_bundle("CAR", shop_seq, dlv_seq, rider["CAR"].calculate_cost(get_total_distance(K, dist_mat, shop_seq, dlv_seq)))
            
            if test_route_feasibility(all_orders, rider["BIKE"], combi_list, combi_list) != -1:
                shop_seq, dlv_seq = can_merge(K, all_orders, combi_list, rider["BIKE"])
                if shop_seq:
                    return possible_bundle("BIKE", shop_seq, dlv_seq, rider["BIKE"].calculate_cost(get_total_distance(K, dist_mat, shop_seq, dlv_seq)))
            
        return None


    index_order = sorted([i for i in range(K)], key = lambda x : len(merge_possible["WALK"][x]), reverse = True)
    while True:
        for index in index_order:
            max_order_len = find_max_complete_graph(merge_possible, matrix, rider_of_choice, index)
            if rider_of_choice == "WALK" and (not max_order_len or max_order_len <= 4):
                print('change of rider detected')
                rider_of_choice = "ALL"
                matrix = matrix_all
                index_order = sorted([i for i in range(K)], key = lambda x : len(merge_possible["ALL"][x]), reverse = True)
                break
            if not max_order_len:
                continue

            # 탑 다운 방식으로 route 찾기
            for i in reversed(range(1, min(max_order_len+1, 6))):
                route = make_n_len_route(i, rider_of_choice, index)
                if route:
                    print(route.rider, route.shop_seq, route.dlv_seq, route.cost)
                    route_list = [route]
                    
                    for i in route.shop_seq:
                        shop_seq =list(route.shop_seq) 
                        dlv_seq = list(route.dlv_seq)
                        shop_seq.remove(i)
                        dlv_seq.remove(i)
                        for r in all_riders:
                            if test_route_feasibility(all_orders, r, shop_seq, dlv_seq) == 0:
                                cost = r.calculate_cost(get_total_distance(K, dist_mat, shop_seq, dlv_seq))
                                route_list.append(possible_bundle(r.type, shop_seq, dlv_seq, cost))
                    
                    q.put(route_list)
                    break
            if time.time() - start_time > timelimit:
                break
        
        if time.time() - start_time > timelimit:
            break
    
    


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

    merge_possible = {}
    merge_possible["WALK"] = defaultdict(set)
    merge_possible["CAR"] = defaultdict(set)
    merge_possible["BIKE"] = defaultdict(set)
    merge_possible["ALL"] = defaultdict(set)

    optimal_route = []

    for i in range(K):
        for r in all_riders:
            if test_route_feasibility(all_orders, r, [i], [i]) == 0:
                optimal_route.append(possible_bundle(r.type, [i], [i], r.calculate_cost(get_total_distance(K, dist_mat, [i], [i]))))
    
    # connectivity matrix
    matrix = [[0 for i in range(K)] for j in range(K)]
    matrix_all = [[0 for i in range(K)] for j in range(K)]
    
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
                            if rider_type == "WALK":
                                matrix[i][j] = 1
                                matrix[j][i] = 1
                            if rider_type == "ALL":
                                matrix_all[i][j] = 1
                                matrix_all[j][i] = 1
            if optimal_route_found:
                optimal_route.extend([val for val in optimal_route_found.values()])

    
    # 프로세스 분리
    # 원래는 __name__ == '__main__'으로 실행해야 하지만, 이 경우는 함수로 실행
    # 그래서 multiprocessing은 windows에서 불가능 (wsl 사용 추천)
    # https://docs.python.org/ko/3/library/multiprocessing.html
    #
    # 참고로 파이썬 특성상 멀티쓰레딩은 효과가 없음
    #
    # 프로세스 간 통신을 하는 방법은 두가지가 있음
    # 하나는 Pipe, 다른 하나는 Queue
    #
    # 또한 pickle이라는 개념때문에 넘긴 class는 메서드나 nested class 사용 불가
    # 그래서 merge함수 안에는 메서드를 사용하지 않음
    # 아니면 해당 함수 안에서 다시 클래스를 정의해야 함
    q1 = Queue()
    p1 = Process(target=merge, args=(q1, K, all_orders, all_riders, dist_mat, merge_possible, "ALL", start_time, timelimit-1))
    p1.start()

    q2 = Queue()
    p2 = Process(target=merge_4, args=(q2, K, all_orders, all_riders, dist_mat, merge_possible, "CAR", start_time, timelimit-1))
    p2.start()

    q3 = Queue()
    p3 = Process(target=top_down_merge, args=(q3, K, matrix, matrix_all, all_orders, dist_mat, all_riders, merge_possible, start_time, timelimit-1))
    p3.start()

    # A solution is a list of bundles
    solution = []
    best_cost = np.inf

    while True:
        if time.time() - start_time > timelimit:
            break

        # 다른 프로세스의 결과 가져오기
        while not q1.empty():
            optimal_route.extend(q1.get())

        while not q2.empty():
            optimal_route.extend(q2.get())

        while not q3.empty():
            optimal_route.extend(q3.get())

        # merge result 나오는 형태
        
        tmp_solution, tmp_cost = get_solution(K,available_numbers,optimal_route,start_time,timelimit)

        if tmp_cost <= best_cost:
            best_cost = tmp_cost
            solution = tmp_solution
        # 현재 solution 결과 생성\

    p1.terminate()
    p2.terminate()
    p3.terminate()

    #------------- End of custom algorithm code--------------/


    return [[rider, list(shop_seq), list(dlv_seq)] for rider, shop_seq, dlv_seq in solution]


