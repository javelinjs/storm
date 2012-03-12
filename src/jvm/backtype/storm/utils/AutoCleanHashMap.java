/**
 * @(#) AutoCleanHashMap.java  1.0 2012-02-23
 *
 * Copyright 2009-2012 Mediav Inc. All rights reserved.
 */
package backtype.storm.utils;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.CountDownLatch;

/**
 * @author Yizhi Liu (liuyz@mediav.com)
 * @version 1.0 2012-02-23
 */
public class AutoCleanHashMap<K, V> {
  private static final int DEFAULT_NUM_BUCKETS = 1;

  /* interfaces */
  public static interface OperateCallback<K, V> {
    public void expire(K key, V val);
    public void getOpr(K key, V val);
    public void putOpr(K key, V val);
  }
  public static interface AutoCleanStrategy<K, V> {
    public boolean cleanIt(K key, V val);
  }
  public static interface OperateStrategy<K, V> {
    public V updateValue(K key, V val);
    public V initValue(K key);
  }

  /* snapshot related */
  private ArrayList<HashMap<K, V>> __snapshotMaps;

  /* buckets */
  private ArrayList<HashMap<K, V>> __buckets;
  private ArrayList<LinkedBlockingQueue<Request>> __queues;
  private ArrayList<Thread> __threads;

  private Thread __cleaner;

  private enum OperateType {
    GET,
    PUT,
    UPDATE,
    REMOVE, // not done
    TRAVERSAL_READ,
    TRAVERSAL_UPDATE,
    SNAPSHOT_SYNC,
    CLEAN;
  }
  /* Request struct */
  private class Request {
    public OperateType oprType;
    public K key;
    public V value;
    OperateStrategy<K, V> oprStrategy;
    OperateCallback<K, V> oprCallback;
    public CountDownLatch latch;
  }

  /* Operate Thread */
  private class Operator implements Runnable {
    private int _idx;
    private OperateCallback<K, V> _callback;
    private AutoCleanStrategy<K, V> _cleanStrategy;
    private OperateStrategy<K, V> _oprStrategy;

    public Operator(int idx, OperateCallback<K, V> callback, 
                      AutoCleanStrategy<K, V> cleanStrategy, OperateStrategy<K, V> oprStrategy) {
      _idx = idx;
      _callback = callback;
      _cleanStrategy = cleanStrategy;
      _oprStrategy = oprStrategy;
    }

    public void run() {
      while(true) {
        Request req;
        try {
          req = __queues.get(_idx).take();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }

        V value = null;
        OperateStrategy<K, V> oprs = (req.oprStrategy == null ? _oprStrategy : req.oprStrategy);
        OperateCallback<K, V> callback = (req.oprCallback == null ? _callback : req.oprCallback);

        switch (req.oprType) {
        case GET:
          if (callback != null) {
            if (__buckets.get(_idx).containsKey(req.key)) {
              value = __buckets.get(_idx).get(req.key);
            } else {
              value = null;
            }
            callback.getOpr(req.key, value);
          }
          break;
        case UPDATE:
          if (__buckets.get(_idx).containsKey(req.key) && (oprs != null)) {
            value = oprs.updateValue(req.key, __buckets.get(_idx).get(req.key));
          } else if (oprs != null) {
            value = oprs.initValue(req.key);
          } else {
            value = null;
          }
          __buckets.get(_idx).put(req.key, value);
          if (callback != null) {
            callback.putOpr(req.key, value);  
          }
          break;
        case PUT:
          __buckets.get(_idx).put(req.key, req.value);
          if (callback != null) {
            callback.putOpr(req.key, req.value);
          }
          break;
        case TRAVERSAL_READ:
          if (callback != null) {
            for (K key : __buckets.get(_idx).keySet()) {
              callback.getOpr(key, __buckets.get(_idx).get(key));
            }
          }
          break;
        case TRAVERSAL_UPDATE:
          if (oprs != null) {
            Iterator<Map.Entry<K, V>> iterator = __buckets.get(_idx).entrySet().iterator();  
            while (iterator.hasNext()) {  
              Map.Entry<K, V> entry = iterator.next();  
              value = oprs.updateValue(entry.getKey(), entry.getValue());
              if (value != null) {
                __buckets.get(_idx).put(entry.getKey(), value);
                if (callback != null) {
                  callback.putOpr(entry.getKey(), value);
                }
              }
            }
          }
          break;
        case SNAPSHOT_SYNC:
          __snapshotMaps.set(_idx, new HashMap<K, V>(__buckets.get(_idx)));
          req.latch.countDown();
          break;
        case CLEAN:
          Iterator<Map.Entry<K, V>> iterator = __buckets.get(_idx).entrySet().iterator();  
          while (iterator.hasNext()) {  
            Map.Entry<K, V> entry = iterator.next();  
            if (_cleanStrategy != null && _cleanStrategy.cleanIt(entry.getKey(), entry.getValue())) {
              iterator.remove(); //to avoid java.util.ConcurrentModificationException
              if (callback != null) {
                callback.expire(entry.getKey(), entry.getValue());
              }
            }
          }  
          break;
        }
      }
    }
  }

  /* Clean Thread */
  private class Cleaner implements Runnable {
    long _sleepTime;
    public Cleaner(long sleepTime) {
      _sleepTime = sleepTime;
    }
    public void run() {
      while (true) {
        try {
          Thread.sleep(_sleepTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
        Request req = new Request();
        req.oprType = OperateType.CLEAN;
        for (int i = 0; i < __queues.size(); i++) {
          __queues.get(i).offer(req);
        }
      }
    }
  }

  public AutoCleanHashMap(int cleanIntervalSecs, AutoCleanStrategy<K, V> cleanStrategy, int numBuckets,
                            OperateCallback<K, V> callback, OperateStrategy<K, V> oprStrategy) {
    if (cleanIntervalSecs < 0) {
      throw new IllegalArgumentException("cleanIntervalSecs mush be > 0");
    } else if (cleanStrategy == null) {
      throw new IllegalArgumentException("cleanStrategy cannot be null");
    }

    numBuckets = numBuckets <= 0 ? DEFAULT_NUM_BUCKETS : numBuckets;
    final long cleanIntervalMillis = cleanIntervalSecs * 1000L;

    __buckets = new ArrayList<HashMap<K, V>>();
    __queues = new ArrayList<LinkedBlockingQueue<Request>>();
    __threads = new ArrayList<Thread>();
    
    __snapshotMaps = new ArrayList<HashMap<K, V>>();

    for (int i = 0; i < numBuckets; i++) {
      __buckets.add(new HashMap<K, V>());
      __queues.add(new LinkedBlockingQueue<Request>());
      __snapshotMaps.add(new HashMap<K, V>());
      __threads.add(new Thread(new Operator(i, callback, cleanStrategy, oprStrategy)));
      __threads.get(i).setDaemon(true);
      __threads.get(i).start();
    }

    __cleaner = new Thread(new Cleaner(cleanIntervalMillis));
    __cleaner.setDaemon(true);
    __cleaner.start();
  }

  public AutoCleanHashMap(int cleanIntervalSecs, AutoCleanStrategy<K, V> cleanStrategy, int numBuckets) {
    this(cleanIntervalSecs, cleanStrategy, numBuckets, null, null);
  }

  public AutoCleanHashMap(int cleanIntervalSecs, AutoCleanStrategy<K, V> cleanStrategy) {
    this(cleanIntervalSecs, cleanStrategy, DEFAULT_NUM_BUCKETS, null, null);
  }

  public boolean get(K key, OperateCallback<K, V> callback) {
    if (key == null) {
      return false;
    }
    Request req = new Request();
    req.oprType = OperateType.GET;
    req.key = key;
    req.oprCallback = callback;
    int idx = Math.abs(key.hashCode() % __queues.size());
    return __queues.get(idx).offer(req);
  }
  public boolean get(K key) {
    return get(key, null);
  }

  public boolean put(K key, V value, OperateCallback<K, V> callback) {
    if (key == null) {
      return false;
    }
    Request req = new Request();
    req.oprType = OperateType.PUT;
    req.key = key;
    req.value = value;
    req.oprCallback = callback;
    int idx = Math.abs(key.hashCode() % __queues.size());
    return __queues.get(idx).offer(req);
  }
  public boolean put(K key, V value) {
    return put(key, value, null);
  }

  public boolean update(K key, OperateStrategy<K, V> oprStrategy, OperateCallback<K, V> callback) {
    if (key == null) {
      return false;
    }
    Request req = new Request();
    req.oprType = OperateType.UPDATE;
    req.key = key;
    req.oprStrategy = oprStrategy;
    req.oprCallback = callback;
    int idx = Math.abs(key.hashCode() % __queues.size());
    return __queues.get(idx).offer(req);
  }
  public boolean update(K key, OperateStrategy<K, V> oprStrategy) {
    return update(key, oprStrategy, null);
  }
  public boolean update(K key) {
    return update(key, null, null);
  }

  public void traversalRead(OperateCallback<K, V> callback) {
    Request req = new Request();
    req.oprType = OperateType.TRAVERSAL_READ;
    req.oprCallback = callback;
    for (int i = 0; i < __queues.size(); i++) {
      __queues.get(i).offer(req);
    }
  }
  public void traversalRead() {
    traversalRead(null);
  }

  public void traversalUpdate(OperateStrategy<K, V> oprStrategy, OperateCallback<K, V> callback) {
    Request req = new Request();
    req.oprType = OperateType.TRAVERSAL_UPDATE;
    req.oprStrategy = oprStrategy;
    req.oprCallback = callback;
    for (int i = 0; i < __queues.size(); i++) {
      __queues.get(i).offer(req);
    }
  }
  public void traversalUpdate(OperateStrategy<K, V> oprStrategy) {
    traversalUpdate(oprStrategy, null);
  }
  public void traversalUpdate() {
    traversalUpdate(null, null);
  }

  public synchronized HashMap<K, V> snapshotSync() {
    HashMap<K, V> retMap = new HashMap<K, V>();
    int size = __queues.size();
    CountDownLatch latch = new CountDownLatch(size);

    for (int i = 0; i < size; i++) {
      Request req = new Request();
      req.oprType = OperateType.SNAPSHOT_SYNC;
      req.latch = latch;

      __queues.get(i).offer(req);
    }

    try {
      latch.await(); //TODO: timeout
    } catch (InterruptedException e) {
      return null;
    }
    for (int i = 0; i < size; i++) {
      retMap.putAll(__snapshotMaps.get(i));
    }

    return retMap;
  }

  @Override
  protected void finalize() throws Throwable {
    try {
      __cleaner.interrupt();
      for (int i = 0; i < __threads.size(); i++) {
        __threads.get(i).interrupt();
      }
    } finally {
      super.finalize();
    }
  }

}
