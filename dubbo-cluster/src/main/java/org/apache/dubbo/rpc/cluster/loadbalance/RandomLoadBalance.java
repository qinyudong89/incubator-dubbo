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
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * random load balance.
 *
 * 随机负载均衡
 *
 */
public class RandomLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "random";


    /**
     *  10.0.0.1:20884, weight=2
     *   10.0.0.1:20886, weight=3
     *   10.0.0.1:20888, weight=4
     *
     *  随机算法的实现：
     *   totalWeight=9;
     *   假设offset=1（即random.nextInt(9)=1） 1-2=-1<0？是，所以选中 10.0.0.1:20884, weight=2
     *  假设offset=4（即random.nextInt(9)=4） 4-2=2<0？否，这时候offset=2， 2-3<0？是，所以选中 10.0.0.1:20886, weight=3
     *  假设offset=7（即random.nextInt(9)=7） 7-2=5<0？否，这时候offset=5， 5-3=2<0？否，这时候offset=2， 2-4<0？是，所以选中 10.0.0.1:20888, weight=4
     *
     *
      * @param invokers
     * @param url
     * @param invocation
     * @param <T>
     * @return
     */
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        //先获得invoker 集合大小
        int length = invokers.size(); // Number of invokers
        //总权重
        int totalWeight = 0; // The sum of weights
        //每个invoker是否有相同的权重
        boolean sameWeight = true; // Every invoker has the same weight?
        // 计算总权重
        for (int i = 0; i < length; i++) {
            //获得单个invoker 的权重
            int weight = getWeight(invokers.get(i), invocation);
            //累加
            totalWeight += weight; // Sum
            if (sameWeight && i > 0 && weight != getWeight(invokers.get(i - 1), invocation)) {
                sameWeight = false;
            }
        }
        // 权重不相等，随机后，判断在哪个 Invoker 的权重区间中
        if (totalWeight > 0 && !sameWeight) {
            // 随机
            // If (not every invoker has the same weight & at least one invoker's weight>0), select randomly based on totalWeight.
            int offset = ThreadLocalRandom.current().nextInt(totalWeight);
            // 区间判断
            // Return a invoker based on the random value.
            for (int i = 0; i < length; i++) {
                offset -= getWeight(invokers.get(i), invocation);
                if (offset < 0) {
                    return invokers.get(i);
                }
            }
        }
        // 权重相等，平均随机
        // If all invokers have the same weight value or totalWeight=0, return evenly.
        return invokers.get(ThreadLocalRandom.current().nextInt(length));
    }

}
