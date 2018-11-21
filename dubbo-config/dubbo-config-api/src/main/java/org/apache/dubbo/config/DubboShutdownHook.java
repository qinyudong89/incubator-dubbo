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
package org.apache.dubbo.config;

import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.registry.support.AbstractRegistryFactory;
import org.apache.dubbo.rpc.Protocol;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The shutdown hook thread to do the clean up stuff.
 * 关闭钩子线程目的去做清空操作
 *
 * This is a singleton in order to ensure there is only one shutdown hook registered.
 * 这是一个单例类，目的是确保只有有个线程
 *
 * Because {@link ApplicationShutdownHooks} use {@link java.util.IdentityHashMap}
 * to store the shutdown hooks.
 *
 * 因为使用了 XXXX存储  shutdown hooks.
 *
 */
public class DubboShutdownHook extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(DubboShutdownHook.class);

    private static final DubboShutdownHook dubboShutdownHook = new DubboShutdownHook("DubboShutdownHook");

    public static DubboShutdownHook getDubboShutdownHook() {
        return dubboShutdownHook;
    }

    /**
     * Has it already been destroyed or not?
     * 是否已经被销毁过了
     */
    private final AtomicBoolean destroyed;

    private DubboShutdownHook(String name) {
        super(name);
        this.destroyed = new AtomicBoolean(false);
    }

    @Override
    public void run() {
        if (logger.isInfoEnabled()) {
            logger.info("Run shutdown hook now.");
        }
        destroyAll();
    }

    /**
     * Destroy all the resources, including registries and protocols.
     * 销毁所有的资源，包括 registries 和 protocols
     */
    public void destroyAll() {
        //若是以销毁，忽略
        if (!destroyed.compareAndSet(false, true)) {
            return;
        }
        // destroy all the registries
        // 销毁Registry 相关
        AbstractRegistryFactory.destroyAll();
        // destroy all the protocols
        // 销毁 Protocol 相关
        destroyProtocols();
    }

    /**
     * Destroy all the protocols.
     * 销毁所有的 protocols
     */
    private void destroyProtocols() {
        ExtensionLoader<Protocol> loader = ExtensionLoader.getExtensionLoader(Protocol.class);
        for (String protocolName : loader.getLoadedExtensions()) {
            try {
                Protocol protocol = loader.getLoadedExtension(protocolName);
                if (protocol != null) {
                    protocol.destroy();
                }
            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }
        }
    }

}
