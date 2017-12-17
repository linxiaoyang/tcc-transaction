package org.mengyun.tcctransaction.interceptor;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.mengyun.tcctransaction.NoExistedTransactionException;
import org.mengyun.tcctransaction.SystemException;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.TransactionManager;
import org.mengyun.tcctransaction.api.Compensable;
import org.mengyun.tcctransaction.api.Propagation;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.common.MethodType;
import org.mengyun.tcctransaction.support.FactoryBuilder;
import org.mengyun.tcctransaction.utils.CompensableMethodUtils;
import org.mengyun.tcctransaction.utils.ReflectionUtils;
import org.mengyun.tcctransaction.utils.TransactionUtils;

import java.lang.reflect.Method;
import java.util.Set;

/**
 * Created by changmingxie on 10/30/15.
 */
@Slf4j
public class CompensableTransactionInterceptor {

    private TransactionManager transactionManager;

    private Set<Class<? extends Exception>> delayCancelExceptions;

    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public void setDelayCancelExceptions(Set<Class<? extends Exception>> delayCancelExceptions) {
        this.delayCancelExceptions = delayCancelExceptions;
    }

    /**
     * 拦截补偿方法.
     *
     * @param pjp
     * @throws Throwable
     */
    public Object interceptCompensableMethod(ProceedingJoinPoint pjp) throws Throwable {

        log.debug("CompensableTransactionInterceptor interceptCompensableMethod");
        //找到被Compensable注解注释的方法
        Method method = CompensableMethodUtils.getCompensableMethod(pjp);
        log.debug("被拦截的方法名 method:{}", method.getName());

        //从方法中获取到Compensable注解
        Compensable compensable = method.getAnnotation(Compensable.class);
        //获取传播事务的传播行为
        Propagation propagation = compensable.propagation();
        log.debug("传播行为 propagation:{}", propagation);
        //从spring容器中获取指定transactionContextEditor类型的实例，调用实例的get方法，传入参数，获取事务上下文
        TransactionContext transactionContext = FactoryBuilder.factoryOf(compensable.transactionContextEditor()).getInstance().get(pjp.getTarget(), method, pjp.getArgs());
        //是否是异步确认
        boolean asyncConfirm = compensable.asyncConfirm();
        //是否是异步取消
        boolean asyncCancel = compensable.asyncCancel();
        //是否事务激活
        boolean isTransactionActive = transactionManager.isTransactionActive();
        log.debug("asyncConfirm:{},asyncCancel:{},isTransactionActive:{}", new Object[]{asyncConfirm,asyncCancel,isTransactionActive});
        //判断是否是合法的事务，不合法，抛出异常
        if (!TransactionUtils.isLegalTransactionContext(isTransactionActive, propagation, transactionContext)) {
            throw new SystemException("no active compensable transaction while propagation is mandatory for method " + method.getName());
        }
        //计算方法类型
        MethodType methodType = CompensableMethodUtils.calculateMethodType(propagation, isTransactionActive, transactionContext);
        log.debug("计算出的方法类型 methodType:{}", methodType);
        switch (methodType) {
            case ROOT:
                return rootMethodProceed(pjp, asyncConfirm, asyncCancel);
            case PROVIDER:
                return providerMethodProceed(pjp, transactionContext, asyncConfirm, asyncCancel);
            default:
                return pjp.proceed();
        }
    }

    /**
     * 主事务方法的处理.
     *
     * @param pjp
     * @throws Throwable
     */
    private Object rootMethodProceed(ProceedingJoinPoint pjp, boolean asyncConfirm, boolean asyncCancel) throws Throwable {

        log.debug("==>rootMethodProceed");
        Object returnValue = null;

        Transaction transaction = null;

        try {
            // 事务开始（创建事务日志记录，并在当前线程缓存该事务日志记录）
            transaction = transactionManager.begin();


            try {
                log.debug("==>rootMethodProceed try begin");
                // Try (开始执行被拦截的方法，或进入下一个拦截器处理逻辑)
                returnValue = pjp.proceed();
                log.debug("==>rootMethodProceed try end");

            } catch (Throwable tryingException) {

                //判断异常的类型是否是延迟取消的异常，如果是就同步事务的状态（update）
                if (isDelayCancelException(tryingException)) {
                    transactionManager.syncTransaction();
                } else {
                    log.warn(String.format("compensable transaction trying failed. transaction content:%s", JSON.toJSONString(transaction)), tryingException);
                    //不是指定的异常类型，就回滚
                    transactionManager.rollback(asyncCancel);
                }

                throw tryingException;
            }

            //如果没有异常那么久提交
            transactionManager.commit(asyncConfirm);

        } finally {
            transactionManager.cleanAfterCompletion(transaction);
        }

        return returnValue;
    }

    /**
     * 服务提供者事务方法处理.
     *
     * @param pjp
     * @param transactionContext
     * @throws Throwable
     */
    private Object providerMethodProceed(ProceedingJoinPoint pjp, TransactionContext transactionContext, boolean asyncConfirm, boolean asyncCancel) throws Throwable {

        log.debug("==>providerMethodProceed transactionStatus:" + TransactionStatus.valueOf(transactionContext.getStatus()).toString());

        Transaction transaction = null;
        try {

            switch (TransactionStatus.valueOf(transactionContext.getStatus())) {
                case TRYING:
                    log.debug("==>providerMethodProceed try begin");
                    // 基于全局事务ID扩展创建新的分支事务，并存于当前线程的事务局部变量中.
                    transaction = transactionManager.propagationNewBegin(transactionContext);
                    log.debug("==>providerMethodProceed try end");
                    // 开始执行被拦截的方法，或进入下一个拦截器处理逻辑
                    return pjp.proceed();
                case CONFIRMING:
                    try {
                        log.debug("==>providerMethodProceed confirm begin");
                        // 找出存在的事务并处理.
                        transaction = transactionManager.propagationExistBegin(transactionContext);
                        // 提交
                        transactionManager.commit(asyncConfirm);
                        log.debug("==>providerMethodProceed confirm end");
                    } catch (NoExistedTransactionException excepton) {
                        //the transaction has been commit,ignore it.
                    }
                    break;
                case CANCELLING:
                    try {
                        log.debug("==>providerMethodProceed cancel begin");
                        transaction = transactionManager.propagationExistBegin(transactionContext);
                        // 回滚
                        transactionManager.rollback(asyncCancel);
                        log.debug("==>providerMethodProceed cancel end");

                    } catch (NoExistedTransactionException exception) {
                        //the transaction has been rollback,ignore it.
                    }
                    break;
            }

        } finally {
            transactionManager.cleanAfterCompletion(transaction);
        }

        Method method = ((MethodSignature) (pjp.getSignature())).getMethod();

        return ReflectionUtils.getNullValue(method.getReturnType());
    }

    private boolean isDelayCancelException(Throwable throwable) {

        if (delayCancelExceptions != null) {
            for (Class delayCancelException : delayCancelExceptions) {

                Throwable rootCause = ExceptionUtils.getRootCause(throwable);

                if (delayCancelException.isAssignableFrom(throwable.getClass())
                        || (rootCause != null && delayCancelException.isAssignableFrom(rootCause.getClass()))) {
                    return true;
                }
            }
        }

        return false;
    }

}
