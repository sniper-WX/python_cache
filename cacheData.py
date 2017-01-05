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
        self.cache_data = {}
        self.cache_locks = {}
        if len(args) > 0:
            for cache in args:
                cache_name = cache['name']
                value = cache['value']
                self.cache_data[cache_name] = value
                self.cache_locks[cache_name] = threading.Lock()
        self.cache_file_name = kwargs['cache_file_name']
        self.splitter = '&&'
        self.load_cache_from_file()
        logger.info("loading cache data success")

    def load_cache_from_file(self):
        try:
            with open(self.cache_file_name,'r') as cache_file:
                for line in cache_file:
                    (cache_name,key,value) = line.strip().split(self.splitter)
                    if cache_name not in self.cache_data:
                        self.cache_data[cache_name] = {}
                        self.cache_locks[cache_name] = threading.Lock()
                    self.cache_data[cache_name][key] = value

        except Exception as e:
            logger.error("load cache error ",exc_info=sys.exc_info())
            sys.exit(1)

    def add_cache(self, cache_name):
        try:
            if cache_name in self.cache_data:
                return False
            else:
                self.cache_data[cache_name] = {}
                self.cache_locks[cache_name] = threading.Lock()
        except Exception as e:
            logger.warning("adding cache error", exc_info=sys.exc_info())

    def get(self, cache_name, key, ttl=-1):
        locked = False
        try:
            if key in self.cache_data[cache_name]:
                self.cache_locks[cache_name].acquire()
                locked = True
                value = copy.deepcopy(self.cache_data[cache_name][key])
                valueeval(value)
                self.cache_locks[cache_name].release()
                locked = False
                return value
            else:
                return None
        except Exception as e:
            logger.warning("getting cache exception cache_name(%s),key(%s)"%(cache_name, key), exc_info=sys.exc_info())
            if locked:
                self.cache_locks[cache_name].release()
            return None

    def set_cache(self,cache_name, key, value):
        locked = False
        try:
            self.cache_locks[cache_name].acquire()
            locked = True
            value_to_set = copy.deepcopy(value)
            self.cache_data[cache_name][key] = str(value_to_set)
            self.cache_locks[cache_name].release()
            locked = False
            return value
        except Exception as e:
            logger.warning("setting cache exception cache_name(%s),key(%s),value(%s)" % (cache_name, key,value),
                           exc_info=sys.exc_info())
            if locked:
                self.cache_locks[cache_name].release()
            return None

    def add_cache_key(self, cache_name, key, value):
        locked = False
        try:
            self.cache_locks[cache_name].acquire()
            locked = True
            value_to_set = copy.deepcopy(value)
            key_to_add = copy.deepcopy(key)
            self.cache_data[cache_name][key_to_add] = str(value_to_set)
            self.cache_locks[cache_name].release()
            locked = False
            return value
        except Exception as e:
            logger.warning("adding cache key exception cache_name(%s),key(%s),value(%s)" % (cache_name, key, value),
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
                store_content = self.splitter.join([cache_name,key,value])+'\n'
                file_persist.write(store_content)

        except Exception as e:
            raise e

if __name__ == "__main__":
    import time
    cache = CacheData(cache_file_name="../data/cache_data.txt")
    cache.add_cache('icp')
    cache.add_cache_key('icp','baidu.com','山东')
    cache.add_cache_key('icp', '163.com', str({'icp':'东beu','time':time.time()}))
    cache.add_cache_key('icp', 'google.com', 'beijing')
    cache.add_cache('ip_location')
    cache.add_cache_key('ip_location','baidu.com','山东')
    cache.add_cache_key('ip_location', '163.com', '东beu')
    cache.add_cache_key('ip_location', 'google.com', 'beijing')
    ipc_163 = eval(cache.get('icp', '163.com'))
    print 'icp of 163.com:%s time:%s'%(ipc_163['icp'],time.asctime(time.localtime(ipc_163['time'])))
    print 'icp baidu.com:%s'%cache.get('icp','baidu.com')
    print 'ip_location baidu.com:%s' % cache.get('ip_location', 'baidu.com')
    cache.set_cache('icp', 'baidu.com','广东')
    print 'after set icp baidu.com:%s' % cache.get('icp', 'baidu.com')
    cache.persist_cache()
