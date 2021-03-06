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
package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.AbstractPostProcessFilter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;

import java.util.HashMap;
import java.util.Map;

/**
 * ContextInvokerFilter
 *
 * 服务提供者的 ContextFilter 实现类
 *
 */
@Activate(group = Constants.PROVIDER, order = -10000)
public class ContextFilter extends AbstractPostProcessFilter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // 创建新的 `attachments` 集合，清理公用的隐式参数
        Map<String, String> attachments = invocation.getAttachments();
        if (attachments != null) {
            attachments = new HashMap<String, String>(attachments);
            attachments.remove(Constants.PATH_KEY);
            attachments.remove(Constants.GROUP_KEY);
            attachments.remove(Constants.VERSION_KEY);
            attachments.remove(Constants.DUBBO_VERSION_KEY);
            attachments.remove(Constants.TOKEN_KEY);
            attachments.remove(Constants.TIMEOUT_KEY);
            attachments.remove(Constants.ASYNC_KEY);// Remove async property to avoid being passed to the following invoke chain.
            // 清空消费端的异步参数
        }
        // 设置 RpcContext 对象
        RpcContext.getContext()
                .setInvoker(invoker)
                .setInvocation(invocation)
//                .setAttachments(attachments)  // merged from dubbox
                .setLocalAddress(invoker.getUrl().getHost(),
                        invoker.getUrl().getPort());

        // merged from dubbox
        // we may already added some attachments into RpcContext before this filter (e.g. in rest protocol)
        // 在此过滤器(例如rest协议)之前，我们可能已经在RpcContext中添加了一些附件。
        // TODO
        if (attachments != null) {
            if (RpcContext.getContext().getAttachments() != null) {
                RpcContext.getContext().getAttachments().putAll(attachments);
            } else {
                RpcContext.getContext().setAttachments(attachments);
            }
        }
        // 设置 RpcInvocation 对象的 `invoker` 属性
        if (invocation instanceof RpcInvocation) {
            ((RpcInvocation) invocation).setInvoker(invoker);
        }
        // 服务调用
        try {
            return postProcessResult(invoker.invoke(invocation), invoker, invocation);
        } finally {
            // 移除上下文
            // IMPORTANT! For async scenario, we must remove context from current thread, so we always create a new RpcContext for the next invoke for the same thread.
            RpcContext.removeContext();
            RpcContext.removeServerContext();
        }
    }

    @Override
    protected Result doPostProcess(Result result, Invoker<?> invoker, Invocation invocation) {
        // pass attachments to result
        result.addAttachments(RpcContext.getServerContext().getAttachments());
        return result;
    }
}
