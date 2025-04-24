from redis.cluster import RedisCluster, ClusterNode
import random
import time
import threading


startup_nodes = [ClusterNode("redis-cluster-0.redis-cluster-headless.default.svc.cluster.local", 6379)]
r = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)


total_ops = 0
total_time = 0
lock = threading.Lock()


def run_workload(client_id, num_ops=50):
    global total_ops, total_time
    local_latency = 0
    success = 0


    for _ in range(num_ops):
        key = f"key:{random.randint(1, 10000)}"
        value = f"value:{random.randint(1, 10000)}"
        start = time.time()


        try:
            r.set(key, value)
            r.get(key)
            latency = (time.time() - start) * 1000
            local_latency += latency
            success += 1
            print(f"[Client {client_id}] Latency: {latency:.2f} ms")
        except Exception as e:
            print(f"[Client {client_id}] Error: {e}")


    with lock:
        total_ops += success * 2
        total_time += local_latency


    if success > 0:
        avg_latency = local_latency / success
        print(f"[Client {client_id}] Avg Latency: {avg_latency:.2f} ms")


threads = []
start_time = time.time()


for i in range(1, 4):
    t = threading.Thread(target=run_workload, args=(i,))
    t.start()
    threads.append(t)


for t in threads:
    t.join()


end_time = time.time()
elapsed = end_time - start_time


print(f"Total operations: {total_ops}")
print(f"Total time: {elapsed:.2f} sec")
print(f"Throughput: {total_ops / elapsed:.2f} ops/sec")