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
package org.apache.dubbo.rpc.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.protocol.AbstractProtocol;

/**
 * MockProtocol is used for generating a mock invoker by URL and type on consumer side
 *
 * 实现 AbstractProtocol 抽象类，用于在服务消费者，通过类型为 "mock" 的 URL ，引用创建 MockInvoker 对象
 *
 */
final public class MockProtocol extends AbstractProtocol {

    @Override
    public int getDefaultPort() {
        return 0;
    }

    @Override
    public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
        throw new UnsupportedOperationException();
    }

    /**
     * 引用创建 MockInvoker 对象。一般情况下，我们可以通过 dubbo-admin 运维平台或者直接向 Zookeeper 写入静态 URL
     * @param type 服务的类型
     * @param url  远程服务的URL地址
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        return new MockInvoker<T>(url);
    }
}
