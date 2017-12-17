package org.mengyun.tcctransaction.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.mengyun.tcctransaction.InvocationContext;
import org.mengyun.tcctransaction.Participant;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.TransactionManager;
import org.mengyun.tcctransaction.api.Compensable;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.api.TransactionXid;
import org.mengyun.tcctransaction.support.FactoryBuilder;
import org.mengyun.tcctransaction.utils.CompensableMethodUtils;
import org.mengyun.tcctransaction.utils.ReflectionUtils;

import java.lang.reflect.Method;

/**
 * Created by changmingxie on 11/8/15.
 *
 * 资源协调拦截器
 */
@Slf4j
public class ResourceCoordinatorInterceptor {

    private TransactionManager transactionManager;


    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public Object interceptTransactionContextMethod(ProceedingJoinPoint pjp) throws Throwable {
        log.debug("==>interceptTransactionContextMethod(ProceedingJoinPoint pjp)");
        // 获取当前事务
        Transaction transaction = transactionManager.getCurrentTransaction();

        if (transaction != null) {

            switch (transaction.getStatus()) {
                case TRYING:
                    //增加合作者
                    enlistParticipant(pjp);
                    break;
                case CONFIRMING:
                    break;
                case CANCELLING:
                    break;
            }
        }

        return pjp.proceed(pjp.getArgs());
    }

    /**
     * 增加合作者
     * @param pjp
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    private void enlistParticipant(ProceedingJoinPoint pjp) throws IllegalAccessException, InstantiationException {
        log.debug("==> enlistParticipant(ProceedingJoinPoint pjp)");

        //获取有Compensable注解的方法
        Method method = CompensableMethodUtils.getCompensableMethod(pjp);
        log.debug("被拦截的方法名 method:{}", method.getName());
        if (method == null) {
            throw new RuntimeException(String.format("join point not found method, point is : %s", pjp.getSignature().getName()));
        }
        //获取方法上的Compensable注解
        Compensable compensable = method.getAnnotation(Compensable.class);

        //从注解上分析得到confirmMethod与cancelMethod方法
        String confirmMethodName = compensable.confirmMethod();
        String cancelMethodName = compensable.cancelMethod();

        //获取当前的事务
        Transaction transaction = transactionManager.getCurrentTransaction();
        //创建不同的TransactionXid使用相同的全局事务ID
        TransactionXid xid = new TransactionXid(transaction.getXid().getGlobalTransactionId());

        //获取注解中设置的transactionContextEditor的类型的实例，然后调用get方法，如果为空，就往里面设置值
        if (FactoryBuilder.factoryOf(compensable.transactionContextEditor()).getInstance().get(pjp.getTarget(), method, pjp.getArgs()) == null) {
            FactoryBuilder.factoryOf(compensable.transactionContextEditor()).getInstance().set(new TransactionContext(xid, TransactionStatus.TRYING.getId()), pjp.getTarget(), ((MethodSignature) pjp.getSignature()).getMethod(), pjp.getArgs());
        }

        //找到目标类，就是被AOP拦截的类
        Class targetClass = ReflectionUtils.getDeclaringType(pjp.getTarget().getClass(), method.getName(), method.getParameterTypes());

        //创建InvocationContext，目标类，确认的方法，方法的参数类型，方法的参数
        InvocationContext confirmInvocation = new InvocationContext(targetClass,
                confirmMethodName,
                method.getParameterTypes(), pjp.getArgs());

        //创建InvocationContext，目标类，确认的方法，方法的参数类型，方法的参数
        InvocationContext cancelInvocation = new InvocationContext(targetClass,
                cancelMethodName,
                method.getParameterTypes(), pjp.getArgs());

        //创建一个参数者
        Participant participant =
                new Participant(
                        xid,
                        confirmInvocation,
                        cancelInvocation,
                        compensable.transactionContextEditor());

        // 加入参与者
        transactionManager.enlistParticipant(participant);

    }


}
