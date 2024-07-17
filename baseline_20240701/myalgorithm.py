from util import *
from multiprocessing import Process, Queue
import gurobipy as gp
from gurobipy import GRB
import sys

# 프로세스 하나가 이 함수를 실행함
def merge(q, K, all_orders, all_riders, dist_mat, initial_merge, timelimit):
    start_time = time.time()

    # [i], 'BIKE'/'CAR'/'WALK', [i], [i], dist_mat[i][i+k]
    merge_result = initial_merge
    i, j = 0, 1
    j_start = 1
    last_point = len(merge_result)

    # 최소 거리를 찾는 함수
    # 이 부분에서 최적화 할 수 있음
    # (가장 짧은 거리가 가능한 배달경로라는 보장이 없기 때문)
    def min_dist(orders):
        best_dist = np.inf
        best_shop_seq = None
        best_dlv_seq = None

        for shop_seq in permutations(orders):
            for dlv_seq in permutations(orders):
                dist = get_total_distance(K, dist_mat, shop_seq, dlv_seq)
                if dist < best_dist:
                    best_dist = dist
                    best_shop_seq = shop_seq
                    best_dlv_seq = dlv_seq

        return list(best_shop_seq), list(best_dlv_seq), best_dist

    # 두 번들을 합치는 함수
    # 위 가장 min_dist()를 사용하여 route 생성,
    # test_route_feasibility 함수를 사용하여 가능한 경로인지 확인
    def merge_bundles(i, j):
        orders = merge_result[i][0] + merge_result[j][0]
        rider = merge_result[i][1]
        shop_seq, dlv_seq, dist = min_dist(orders)

        if test_route_feasibility(all_orders, rider, shop_seq, dlv_seq) == 0:
            return (orders, rider, shop_seq, dlv_seq, dist)
        return None

    # 두 번들을 합치는 함수를 반복하는 함수
    # for문을 사용하지 않은 이유는 1000번 반복하면 q에 결과를 넣어주기 위함
    while True:
        iter = 0
        max_merge_iter = 1000
        while iter < max_merge_iter:
            if merge_result[i][1] == merge_result[j][1] and (not(set(merge_result[i][0])&set(merge_result[j][0]))):
                # 원래 merge_result의 뒤에만 추가
                # 즉, 두 프로세스 전부 앞의 순서는 동일함
                new_bundle = merge_bundles(i, j)
                if new_bundle is not None:
                    merge_result.append(new_bundle)

                else:
                    iter += 1

            j += 1
            if j == last_point:
                i += 1
                j = max(i + 1, j_start)
        
            if i == last_point - 1:
                i, j = 0, last_point
                j_start = last_point
                if last_point == len(merge_result):
                    break
                last_point = len(merge_result)
        
        # queue 결과 보내는 함수
        q.put(merge_result)

        if time.time() - start_time > timelimit:
            break


def get_solution(K,available_numbers,merge_result):
    # Create a new model
    m = gp.Model()
    h = len(merge_result)
    # Create variables
    list_var = []
    for j in range(h):
        list_var.append(m.addVar(vtype='B'))
    
    for i in range(K):
        used_order_constraint = 0
        for j in range(h):
            if i in merge_result[j][0]:
                used_order_constraint += list_var[j]
        m.addConstr(used_order_constraint == 1)

    bike_constraint = 0
    walk_constraint = 0
    car_constraint = 0
    for j in range(h):
        if merge_result[j][1].type == "BIKE":
            bike_constraint += list_var[j]
        elif merge_result[j][1].type == "WALK":
            walk_constraint += list_var[j]
        elif merge_result[j][1].type == "CAR":
            car_constraint += list_var[j]
    m.addConstr(bike_constraint <= available_numbers["BIKE"])
    m.addConstr(walk_constraint <= available_numbers["WALK"])
    m.addConstr(car_constraint <= available_numbers["CAR"])
        

    objective_function = 0
    for j in range(h):
        objective_function += list_var[j] * (merge_result[j][1].fixed_cost + merge_result[j][1].var_cost*merge_result[j][4]/100.0)
    m.setObjective(objective_function, GRB.MINIMIZE)

    # Solve it!
    m.optimize()

    if m.Status == GRB.OPTIMAL:
        return [[merge_result[j][1].type,merge_result[j][2],merge_result[j][3]] for j in range(h) if list_var[j].X==1], m.objVal
    else:
        return None, np.inf

def algorithm(K, all_orders, all_riders, dist_mat, timelimit=58):
    start_time = time.time()

    for r in all_riders:
        r.T = np.round(dist_mat/r.speed + r.service_time)


    #------------- Custom algorithm code starts from here --------------#

    available_numbers = {}
    for rider in all_riders:
        if rider.type == 'BIKE':
            available_numbers['BIKE'] = rider.available_number
        elif rider.type == 'CAR':
            available_numbers['CAR'] = rider.available_number
        elif rider.type == 'WALK':
            available_numbers['WALK'] = rider.available_number

    merge_result = []
    for i in range(K):
        for rider in all_riders:
            if test_route_feasibility(all_orders, rider, [i], [i]) == 0:
                merge_result.append(([i],rider,[i],[i],dist_mat[i][i+K]))
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
    q = Queue()
    p = Process(target=merge, args=(q, K, all_orders, all_riders, dist_mat, merge_result, timelimit-1))
    p.start()

    # A solution is a list of bundles
    solution = []
    best_cost = np.inf

    while True:
        if time.time() - start_time > timelimit:
            break

        # 다른 프로세스의 결과 가져오기
        if not q.empty():
            merge_result = q.get()

        # merge result 나오는 형태
        
        tmp_solution, tmp_cost = get_solution(K,available_numbers,merge_result)

        if tmp_cost <= best_cost:
            best_cost = tmp_cost
            solution = tmp_solution
        # 현재 solution 결과 생성\

    p.terminate()

    #------------- End of custom algorithm code--------------/


    return solution


