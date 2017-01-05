# encoding: utf-8

import threading
import copy
import QWQCLoger
import sys

logger = QWQCLoger.QWQCLogger.get_logger(__name__)


class CacheData(object):

    def __init__(self, *args, **kwargs):
        '''
        :param args: tuples of cache data in memory
                     for example ({'name':'icp','value':icp_dict},{'name':'ip_location','value':ip_location_dict})
        :param kwargs:
                    cache_file_name:indicate the cache_data file's name(path)
        '''

        self.ARGS_CACHE_FILE_NAME_KEY = 'cache_file_name'

        self.DATA_LAST_TIME_KEY = 'time'
        self.DATA_VALUE_KEY = 'value'
        self.DATA_TTL_KEY = 'ttl'

        self.CACHE_NAME_KEY = 'name'
        self.CACHE_VALUE_KEY = 'value'

        self.SPLITTER = '&&'

        self.cache_data = {}
        self.cache_locks = {}
        if len(args) > 0:
            for cache in args:
                cache_name = copy.deepcopy(cache[self.CACHE_NAME_KEY])
                value = copy.deepcopy([self.CACHE_VALUE_KEY])
                self.cache_data[cache_name] = value
                self.cache_locks[cache_name] = threading.Lock()
        self.cache_file_name = kwargs[self.ARGS_CACHE_FILE_NAME_KEY]
        self.load_cache_from_file()
        logger.info("loading cache data success")

    def load_cache_from_file(self):
        try:
            with open(self.cache_file_name,'r') as cache_file:
                for line in cache_file:
                    (cache_name,key,value) = line.strip().split(self.SPLITTER)
                    if cache_name not in self.cache_data:
                        self.cache_data[cache_name] = {}
                        self.cache_locks[cache_name] = threading.Lock()
                    self.cache_data[cache_name][key] = eval(value)

        except Exception as e:
            logger.error("load cache error ", exc_info=sys.exc_info())
            sys.exit(1)

    def add_cache(self, cache_name):
        try:
            if cache_name in self.cache_data:
                return False
            else:
                self.cache_locks[cache_name] = threading.Lock()
                self.cache_locks[cache_name].acquire()
                self.cache_data[cache_name] = {}
                self.cache_locks[cache_name].release()
        except Exception as e:
            logger.error("adding cache error", exc_info=sys.exc_info())

    def get(self, cache_name, key):
        locked = False
        try:
            if cache_name not in self.cache_data:
                return None
            if key in self.cache_data[cache_name]:
                # copy target data from cache
                self.cache_locks[cache_name].acquire()
                locked = True
                real_value = copy.deepcopy(self.cache_data[cache_name][key])
                self.cache_locks[cache_name].release()
                locked = False

                cur_time = time.time()
                ttl = real_value[self.DATA_TTL_KEY]
                time_interval = cur_time - real_value[self.DATA_LAST_TIME_KEY]
                if -1 < ttl < time_interval:
                    return None
                return real_value[self.DATA_VALUE_KEY]
            else:
                return None
        except Exception as e:
            logger.error("getting cache exception cache_name(%s),key(%s)"%(cache_name, key), exc_info=sys.exc_info())
            if locked:
                self.cache_locks[cache_name].release()
            return None

    def set_cache_data(self,cache_name, key, value, ttl=-1):
        locked = False
        try:
            value_to_set = copy.deepcopy(value)
            store_value = {self.DATA_LAST_TIME_KEY : time.time(),
                           self.DATA_TTL_KEY : ttl,
                           self.DATA_VALUE_KEY : value_to_set
                           }

            self.cache_locks[cache_name].acquire()
            locked = True
            self.cache_data[cache_name][key] = store_value
            self.cache_locks[cache_name].release()
            locked = False
            return value
        except Exception as e:
            logger.error("setting cache exception cache_name(%s),key(%s),value(%s)" % (cache_name, key,value),
                           exc_info=sys.exc_info())
            if locked:
                self.cache_locks[cache_name].release()
            return None

    def add_cache_data(self, cache_name, key, value, ttl=-1):
        locked = False
        try:
            value_to_set = copy.deepcopy(value)
            key_to_add = copy.deepcopy(key)
            store_value = {self.DATA_LAST_TIME_KEY: time.time(),
                           self.DATA_TTL_KEY: ttl,
                           self.DATA_VALUE_KEY: value_to_set
                           }
            self.cache_locks[cache_name].acquire()
            locked = True
            self.cache_data[cache_name][key_to_add] = store_value
            self.cache_locks[cache_name].release()
            locked = False
            return value
        except Exception as e:
            logger.error("adding cache key exception cache_name(%s),key(%s),value(%s)" % (cache_name, key, value),
                           exc_info=sys.exc_info())
            if locked:
                self.cache_locks[cache_name].release()
            return None

    def persist_cache(self):
        locked = False
        try:
            with open(self.cache_file_name, 'w') as file:
                for cache_name in self.cache_data:
                    self.cache_locks[cache_name].acquire()
                    locked = True
                    cache_data = copy.deepcopy(self.cache_data[cache_name])
                    self.cache_locks[cache_name].release()
                    locked = False
                    self.persist_to_file(file, cache_name, cache_data)
        except Exception as e:
            logger.error("persist cache exception",
                           exc_info=sys.exc_info())
            if locked:
                self.cache_locks[cache_name].release()
            return None
        pass

    def persist_to_file(self, file_persist, cache_name, cache_data):
        try:
            for key, value in cache_data.items():
                store_content = self.SPLITTER.join([cache_name,key,str(value)])+'\n'
                file_persist.write(store_content)

        except Exception as e:
            raise e

if __name__ == "__main__":
    import time
    cache = CacheData(cache_file_name="../data/cache_data.txt")
    cache.add_cache('icp')
    cache.add_cache_data('icp','baidu.com','山东',5)
    cache.add_cache_data('icp', '163.com', str({'icp':'东beu','time':time.time()}))
    cache.add_cache_data('icp', 'google.com', 'beijing')
    cache.add_cache('ip_location')
    cache.add_cache_data('ip_location','baidu.com','山东',100)
    cache.add_cache_data('ip_location', '163.com', '东beu')
    cache.add_cache_data('ip_location', 'google.com', 'beijing')

    time.sleep(10)
    baidu_icp = cache.get('icp','baidu.com')
    baidu_ip_location = cache.get('ip_location', 'baidu.com')
    assert(baidu_icp == None)
    assert(baidu_ip_location == '山东')
    print 'icp baidu.com:%s'%baidu_icp
    print 'ip_location baidu.com:%s' % baidu_ip_location
    cache.set_cache_data('icp', 'baidu.com','广东')
    cache.set_cache_data('icp', '163.com', 'xxxxzzzz',100)
    cache.set_cache_data('ip_location', 'google.com', 'ggggggggg',5)
    time.sleep(10)
    baidu_icp = cache.get('icp','baidu.com')
    ipc_163 = cache.get('icp', '163.com')
    google_location = cache.get('ip_location', 'google.com')
    assert(baidu_icp == '广东')
    assert(ipc_163 ==  'xxxxzzzz')
    assert(google_location == None)
    print 'after set icp baidu.com:%s' % baidu_icp
    print 'after set icp 163.com:%s' % ipc_163
    print 'after set icp google.com:%s' % google_location
    cache.persist_cache()
