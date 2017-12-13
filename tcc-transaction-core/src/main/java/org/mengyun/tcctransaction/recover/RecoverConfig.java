package org.mengyun.tcctransaction.recover;

import java.util.Set;

/**
 * 事务恢复配置接口.
 * Created by changming.xie on 6/1/16.
 */
public interface RecoverConfig {

    /**
     * 获取最大重试次数
     *
     * @return
     */
    int getMaxRetryCount();

    /**
     * 获取需要执行事务恢复的持续时间.
     *
     * @return
     */
    int getRecoverDuration();

    /**
     * 获取定时任务规则表达式.
     *
     * @return
     */
    String getCronExpression();

    Set<Class<? extends Exception>> getDelayCancelExceptions();

    void setDelayCancelExceptions(Set<Class<? extends Exception>> delayRecoverExceptions);

    int getAsyncTerminateThreadPoolSize();
}
