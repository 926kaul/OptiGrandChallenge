from util import *
from multiprocessing import Process, Queue

# 프로세스 하나가 이 함수를 실행함
def merge(q, K, all_orders, all_riders, dist_mat, initial_merge, timelimit):
    start_time = time.time()

    # [i], ['BIKE', 'CAR', 'WALK'], [i], [i], []
    merge_result = initial_merge
    i, j = 0, 1
    j_start = 1
    last_point = len(merge_result)
    finish = False

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

        return best_shop_seq, best_dlv_seq

    # 두 번들을 합치는 함수
    # 위 가장 mindist()를 사용하여 route 생성,
    # test_route_feasibility 함수를 사용하여 가능한 경로인지 확인
    def merge_bundles(i, j):
        orders = list(set(merge_result[i][0] + merge_result[j][0]))
        init_riders = list(rider for rider in all_riders if rider.type in set(merge_result[i][1] + merge_result[j][1]))
        shop_seq, dlv_seq = min_dist(orders)

        riders = []
        for rider in init_riders:
            if test_route_feasibility(all_orders, rider, shop_seq, dlv_seq) == 0:
                riders.append(rider.type)
        
        if riders == []:
            return None
        return (orders, riders, shop_seq, dlv_seq, [])

    # 두 번들을 합치는 함수를 반복하는 함수
    # for문을 사용하지 않은 이유는 1000번 반복하면 q에 결과를 넣어주기 위함
    while True:
        iter = 0
        max_merge_iter = 10000
        
        while iter < max_merge_iter:
            
            # 원래 merge_result의 뒤에만 추가
            # 즉, 두 프로세스 전부 앞의 순서는 동일함
            new_bundle = merge_bundles(i, j)
            if new_bundle is not None:
                merge_result.append(new_bundle)
                merge_result[i][4].append(len(merge_result) - 1)
                merge_result[j][4].append(len(merge_result) - 1)

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


def algorithm(K, all_orders, all_riders, dist_mat, timelimit=58):
    start_time = time.time()

    for r in all_riders:
        r.T = np.round(dist_mat/r.speed + r.service_time)


    #------------- Custom algorithm code starts from here --------------#

    rider_info = {}
    for rider in all_riders:
        if rider.type == 'BIKE':
            rider_info['BIKE'] = rider
            bikers = rider.available_number
        elif rider.type == 'CAR':
            rider_info['CAR'] = rider
            cars = rider.available_number
        elif rider.type == 'WALK':
            rider_info['WALK'] = rider
            walkers = rider.available_number

    merge_result = [([i], list(sorted(rider.type for rider in all_riders if test_route_feasibility(all_orders, rider, [i],[i]) == 0)), [i], [i], []) for i in range(K)]

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
        # [[i], ['BIKE', 'CAR', 'WALK'], [i], [i], []]
        # 중간 ['BIKE', 'CAR', 'WALK']로 라이더 분리
        order_ids = [i for i in range(K)]
        random.shuffle(order_ids)
        used = [False] * K
        bundle_rider = {
            ('BIKE',) : [],
            ('CAR',) : [],
            ('WALK',) : [],
            ('BIKE', 'CAR') : [],
            ('BIKE', 'WALK') : [],
            ('CAR', 'WALK') : [],
            ('BIKE', 'CAR', 'WALK') : []
        }

        # i번째 order 선택, 이미 사용된 order는 제외
        # 랜덤으로 묶을 수 있는 order 찾아서 랜덤하게 묶기
        # 최대한 묶을 수 있는 order들은 묶음
        # 묶을 수 있는 order이란 이미 사용되지 않는 order들만을 가진 묶음을 말함
        # 또한 rider count를 계산하여 묶을 수 없으면 묶지 않음
        for i in range(K):
            if used[i]:
                continue
            bundle = merge_result[i]

            while True:
                stop = True
                children = bundle[4]
                random.shuffle(children)
                for child_index in children:
                    get_next_child = False
                    for index in merge_result[child_index][0]:
                        if used[index]:
                            get_next_child = True
                            break
                    if get_next_child:
                        continue

                    child = merge_result[child_index]
                    possible_riders = tuple(sorted(child[1]))

                    if possible_riders == ('BIKE', 'CAR'):
                        if len(bundle_rider[('BIKE',)]) + len(bundle_rider[('CAR',)]) + len(bundle_rider[('BIKE', 'CAR')]) >= bikers + cars:
                            continue
                    elif possible_riders == ('BIKE', 'WALK'):
                        if len(bundle_rider[('BIKE',)]) + len(bundle_rider[('WALK',)]) + len(bundle_rider[('BIKE', 'WALK')]) >= bikers + walkers:
                            continue
                    elif possible_riders == ('CAR', 'WALK'):
                        if len(bundle_rider[('CAR',)]) + len(bundle_rider[('WALK',)]) + len(bundle_rider[('CAR', 'WALK')]) >= cars + walkers:
                            continue
                    elif possible_riders == ('BIKE',):
                        if len(bundle_rider[('BIKE',)]) >= bikers:
                            continue
                    elif possible_riders == ('CAR',):
                        if len(bundle_rider[('CAR',)]) >= cars:
                            continue
                    elif possible_riders == ('WALK',):
                        if len(bundle_rider[('WALK',)]) >= walkers:
                            continue

                    stop = False
                    bundle = child
                    break
                if stop:
                    break
            
            bundle_rider[tuple(sorted(bundle[1]))].append(bundle)
            for index in bundle[0]:
                used[index] = True
        
        # 현재 solution 결과 생성
        current_solution = []
        bike_count = 0
        car_count = 0
        walk_count = 0
        for bundle in bundle_rider[('BIKE',)]:
            current_solution.append(['BIKE', bundle[2], bundle[3]])
            bike_count += 1
        for bundle in bundle_rider[('CAR',)]:
            current_solution.append(['CAR', bundle[2], bundle[3]])
            car_count += 1
        for bundle in bundle_rider[('WALK',)]:
            current_solution.append(['WALK', bundle[2], bundle[3]])
            walk_count += 1
        for bundle in bundle_rider[('BIKE', 'WALK')]:
            if walk_count < walkers:
                current_solution.append(['WALK', bundle[2], bundle[3]])
                walk_count += 1
            else:
                current_solution.append(['BIKE', bundle[2], bundle[3]])
                bike_count += 1
        for bundle in bundle_rider[('BIKE', 'CAR')]:
            if car_count < cars:
                current_solution.append(['CAR', bundle[2], bundle[3]])
                car_count += 1
            else:
                current_solution.append(['BIKE', bundle[2], bundle[3]])
                bike_count += 1
        for bundle in bundle_rider[('CAR', 'WALK')]:
            if walk_count < walkers:
                current_solution.append(['WALK', bundle[2], bundle[3]])
                walk_count += 1
            else:
                current_solution.append(['CAR', bundle[2], bundle[3]])
                car_count += 1
        for bundle in bundle_rider[('BIKE', 'CAR', 'WALK')]:
            if walk_count < walkers:
                current_solution.append(['WALK', bundle[2], bundle[3]])
                walk_count += 1
            elif car_count < cars:
                current_solution.append(['CAR', bundle[2], bundle[3]])
                car_count += 1
            else:
                current_solution.append(['BIKE', bundle[2], bundle[3]])
                bike_count += 1
        
        # calculate cost
        cost = 0
        for rider, shop_seq, dlv_seq in current_solution:
            cost += rider_info[rider].calculate_cost(get_total_distance(K, dist_mat, shop_seq, dlv_seq))
        
        if best_cost > cost:
            best_cost = cost
            solution = current_solution
    
    # bike나 car이 walk로 바꿀 수 있으면 바꾸는 기능
    walk_count = sum(1 for rider, _, _ in solution if rider == 'WALK')
    for i in range(len(solution)):
        rider, shop_seq, dlv_seq = solution[i]
        if walk_count + 1 >= walkers:
            break
        if walk_count + len(shop_seq) < walkers:
            if rider == 'BIKE':
                if sum(test_route_feasibility(all_orders, rider_info['WALK'], [j], [j]) for j in shop_seq) != 0:
                    continue
                walker_cost = sum(rider_info['WALK'].calculate_cost(get_total_distance(K, dist_mat, [j], [j])) for j in shop_seq)
                rider_cost = rider_info['BIKE'].calculate_cost(get_total_distance(K, dist_mat, shop_seq, dlv_seq))
                if walker_cost < rider_cost:
                    solution[i] = ['WALK', (shop_seq[0],), (shop_seq[0],)]
                    walk_count += 1
                    for j in range(1, len(shop_seq)):
                        solution.append(['WALK', (shop_seq[j],), (shop_seq[j],)])
                        walk_count += 1
            elif rider == "CAR":
                if sum(test_route_feasibility(all_orders, rider_info['WALK'], [j], [j]) for j in shop_seq) != 0:
                    continue
                walker_cost = sum(rider_info['WALK'].calculate_cost(get_total_distance(K, dist_mat, [j], [j])) for j in shop_seq)
                rider_cost = rider_info['CAR'].calculate_cost(get_total_distance(K, dist_mat, shop_seq, dlv_seq))
                if walker_cost < rider_cost:
                    solution[i] = ['WALK', (shop_seq[0],), (shop_seq[0],)]
                    walk_count += 1
                    for j in range(1, len(shop_seq)):
                        solution.append(['WALK', (shop_seq[j],), (shop_seq[j],)])
                        walk_count += 1
    

    # bike가 car로 변경 가능하면 바꿔주는 기능
    car_count = sum(1 for rider, _, _ in solution if rider == 'CAR')
    for i in range(len(solution)):
        rider, shop_seq, dlv_seq = solution[i]
        if car_count + 1 >= cars:
            break
        if car_count + len(shop_seq) < cars:
            if rider == 'BIKE':
                if sum(test_route_feasibility(all_orders, rider_info['CAR'], [j], [j]) for j in shop_seq) != 0:
                    continue
                car_cost = sum(rider_info['CAR'].calculate_cost(get_total_distance(K, dist_mat, [j], [j])) for j in shop_seq)
                rider_cost = rider_info['BIKE'].calculate_cost(get_total_distance(K, dist_mat, shop_seq, dlv_seq))
                if car_cost < rider_cost:
                    solution[i] = ['CAR', (shop_seq[0],), (shop_seq[0],)]
                    car_count += 1
                    for j in range(1, len(shop_seq)):
                        solution.append(['CAR', (shop_seq[j],), (shop_seq[j],)])
                        car_count += 1


    p.terminate()

    #------------- End of custom algorithm code--------------#



    return [[sol[0], list(sol[1]),list(sol[2])] for sol in solution]
    