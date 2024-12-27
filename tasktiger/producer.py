
import redis

host="r-uf6d6b41010f6184.redis.rds.aliyuncs.com:6379"
port=6379
username = "mm_ray_test"
password = "Msf1n22UVreitw2pQC"
cli = redis.RedisCluster(host, int(port), username=username, password=password)

target_key=[]
# @namespace_func
for hash_key in cli.keys("*ACTOR"):
    # 初始游标
    cursor = 0
    # 存储所有字段名的列表
    fields = []
    fields_and_sizes = []
    print(hash_key)


