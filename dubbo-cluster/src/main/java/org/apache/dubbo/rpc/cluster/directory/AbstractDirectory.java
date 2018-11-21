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
package org.apache.dubbo.rpc.cluster.directory;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.Router;
import org.apache.dubbo.rpc.cluster.RouterFactory;
import org.apache.dubbo.rpc.cluster.router.MockInvokersSelector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Abstract implementation of Directory:
 * 实现Directory接口的抽象类
 * Invoker list returned from this Directory's list method have been filtered by Routers
 * 实现了公用的路由规则( Router )的逻辑。
 *
 */
public abstract class AbstractDirectory<T> implements Directory<T> {

    // logger
    private static final Logger logger = LoggerFactory.getLogger(AbstractDirectory.class);

    /**
     * 注册中心 URL
     */
    private final URL url;

    /**
     * 是否已经销毁
     */
    private volatile boolean destroyed = false;

    /**
     * 消费者 URL
     *
     * 若未显示调用 {@link #AbstractDirectory(URL, URL, List)} 构造方法，consumerUrl 等于 {@link #url}
     */
    private volatile URL consumerUrl;

    /**
     * Router 数组
     */
    private volatile List<Router> routers;

    public AbstractDirectory(URL url) {
        this(url, null);
    }

    public AbstractDirectory(URL url, List<Router> routers) {
        this(url, url, routers);
    }

    public AbstractDirectory(URL url, URL consumerUrl, List<Router> routers) {
        if (url == null)
            throw new IllegalArgumentException("url == null");
        this.url = url;
        this.consumerUrl = consumerUrl;
        // 设置 Router 数组初始化并设置 Router 数组
        setRouters(routers);
    }

    /**
     * 获得所有服务 Invoker 集合
     * @param invocation
     * @return Invoker 集合
     * @throws RpcException
     */
    @Override
    public List<Invoker<T>> list(Invocation invocation) throws RpcException {
        //当销毁时抛出异常
        if (destroyed) {
            throw new RpcException("Directory already destroyed .url: " + getUrl());
        }
        // 获得所有 Invoker 集合
        List<Invoker<T>> invokers = doList(invocation);
        //根据路由规则，筛选Invoker集合
        List<Router> localRouters = this.routers; // local reference 本地引用，避免并发问题
        if (localRouters != null && !localRouters.isEmpty()) {
            for (Router router : localRouters) {
                try {
                    if (router.getUrl() == null || router.getUrl().getParameter(Constants.RUNTIME_KEY, false)) {
                        invokers = router.route(invokers, getConsumerUrl(), invocation);
                    }
                } catch (Throwable t) {
                    logger.error("Failed to execute router: " + getUrl() + ", cause: " + t.getMessage(), t);
                }
            }
        }
        return invokers;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    public List<Router> getRouters() {
        return routers;
    }

    /**
     * 初始化并设置 Router 数组
     * @param routers Router 数组
     */
    protected void setRouters(List<Router> routers) {
        // copy list
        routers = routers == null ? new ArrayList<Router>() : new ArrayList<Router>(routers);
        // append url router
        String routerKey = url.getParameter(Constants.ROUTER_KEY);
        if (routerKey != null && routerKey.length() > 0) {
            RouterFactory routerFactory = ExtensionLoader.getExtensionLoader(RouterFactory.class).getExtension(routerKey);
            routers.add(routerFactory.getRouter(url));
        }
        // append mock invoker selector
        //都会执行 MockInvokersSelector 的路由逻辑。
        routers.add(new MockInvokersSelector());
        Collections.sort(routers);
        this.routers = routers;
    }

    public URL getConsumerUrl() {
        return consumerUrl;
    }

    public void setConsumerUrl(URL consumerUrl) {
        this.consumerUrl = consumerUrl;
    }

    public boolean isDestroyed() {
        return destroyed;
    }

    @Override
    public void destroy() {
        destroyed = true;
    }

    protected abstract List<Invoker<T>> doList(Invocation invocation) throws RpcException;

}
