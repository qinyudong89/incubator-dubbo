/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcResult;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * When fails, record failure requests and schedule for retry on a regular interval.
 * 当失败时记录错误日志然后在一个时间间隔中规律性的重试
 *
 * Especially useful for services of notification.
 * 尤其用在一些十分有用的方法或者通知上
 *
 * <a href="http://en.wikipedia.org/wiki/Failback">Failback</a>
 *
 */
public class FailbackClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(FailbackClusterInvoker.class);

    /**
     * 重试频率
     */
    private static final long RETRY_FAILED_PERIOD = 5 * 1000;

    /**
     * Use {@link NamedInternalThreadFactory} to produce {@link org.apache.dubbo.common.threadlocal.InternalThread}
     * which with the use of {@link org.apache.dubbo.common.threadlocal.InternalThreadLocal} in {@link RpcContext}.
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2,
            new NamedInternalThreadFactory("failback-cluster-timer", true));

    /**
     * 失败任务集合
     */
    private final ConcurrentMap<Invocation, AbstractClusterInvoker<?>> failed = new ConcurrentHashMap<>();

    /**
     * 重试任务 Future
     */
    private volatile ScheduledFuture<?> retryFuture;

    public FailbackClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    /**
     * 当失败的时候会将invocation 添加到失败集合中
     * @param invocation 失败的invocation
     * @param router 对象自身
     */
    private void addFailed(Invocation invocation, AbstractClusterInvoker<?> router) {
        //如果定时任务未初始化，进行创建
        if (retryFuture == null) {
            synchronized (this) {
                if (retryFuture == null) {
                    retryFuture = scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {

                        @Override
                        public void run() {
                            // collect retry statistics
                            //创建的定时任务，会调用 #retryFailed() 方法，重试任务，发起 RCP 调用。
                            try {
                                retryFailed();
                            } catch (Throwable t) { // Defensive fault tolerance
                                logger.error("Unexpected error occur at collect statistic", t);
                            }
                        }
                    }, RETRY_FAILED_PERIOD, RETRY_FAILED_PERIOD, TimeUnit.MILLISECONDS);
                }
            }
        }
        // 添加到失败任务
        failed.put(invocation, router);
    }

    /**
     * 重试任务，发起 RCP 调用
     */
    void retryFailed() {
        if (failed.size() == 0) {
            return;
        }
        // 循环重试任务，逐个调用
        for (Map.Entry<Invocation, AbstractClusterInvoker<?>> entry : new HashMap<>(failed).entrySet()) {
            Invocation invocation = entry.getKey();
            Invoker<?> invoker = entry.getValue();
            try {
                // RPC 调用得到 Result
                invoker.invoke(invocation);
                // 移除失败任务
                failed.remove(invocation);
            } catch (Throwable e) {
                logger.error("Failed retry to invoke method " + invocation.getMethodName() + ", waiting again.", e);
            }
        }
    }

    @Override
    protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        try {
            checkInvokers(invokers, invocation);
            // 根据负载均衡机制从 invokers 中选择一个Invoker
            Invoker<T> invoker = select(loadbalance, invocation, invokers, null);
            // RPC 调用得到 Result
            return invoker.invoke(invocation);
        } catch (Throwable e) {
            logger.error("Failback to invoke method " + invocation.getMethodName() + ", wait for retry in background. Ignored exception: "
                    + e.getMessage() + ", ", e);
            // 添加到失败任务
            addFailed(invocation, this);
            return new RpcResult(); // ignore
        }
    }

}
