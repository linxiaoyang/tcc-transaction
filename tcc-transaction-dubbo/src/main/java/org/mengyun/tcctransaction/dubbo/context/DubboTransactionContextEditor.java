package org.mengyun.tcctransaction.dubbo.context;

import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionContextEditor;
import org.mengyun.tcctransaction.dubbo.constants.TransactionContextConstants;

import java.lang.reflect.Method;

/**
 * Created by changming.xie on 1/19/17.
 */
public class DubboTransactionContextEditor implements TransactionContextEditor {

    static final Logger logger = Logger.getLogger(DubboTransactionContextEditor.class.getSimpleName());


    @Override
    public TransactionContext get(Object target, Method method, Object[] args) {

        logger.debug("DubboTransactionContextEditor  get start");

        String context = RpcContext.getContext().getAttachment(TransactionContextConstants.TRANSACTION_CONTEXT);

        if (StringUtils.isNotEmpty(context)) {
            return JSON.parseObject(context, TransactionContext.class);
        }

        return null;
    }

    @Override
    public void set(TransactionContext transactionContext, Object target, Method method, Object[] args) {

        logger.debug("DubboTransactionContextEditor set start");

        RpcContext.getContext().setAttachment(TransactionContextConstants.TRANSACTION_CONTEXT, JSON.toJSONString(transactionContext));
    }
}
