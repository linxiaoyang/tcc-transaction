package org.mengyun.tcctransaction;


import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.api.TransactionXid;
import org.mengyun.tcctransaction.common.TransactionType;

import javax.transaction.xa.Xid;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by changmingxie on 10/26/15.
 */
public class Transaction implements Serializable {

    private static final long serialVersionUID = 7291423944314337931L;

    /**
     * 事务XID.
     */
    private TransactionXid xid;

    /**
     * 事务状态.
     */
    private TransactionStatus status;

    /**
     * 事务类型.
     */
    private TransactionType transactionType;

    /**
     * 事务恢复重试次数
     */
    private volatile int retriedCount = 0;

    /**
     * 创建时间
     */
    private Date createTime = new Date();

    /**
     * 最后更新时间
     */
    private Date lastUpdateTime = new Date();

    /**
     * 版本（默认值为1）
     */
    private long version = 1;

    /**
     * 参与者列表.
     */
    private List<Participant> participants = new ArrayList<Participant>();

    /**
     * 附加属性
     */
    private Map<String, Object> attachments = new ConcurrentHashMap<String, Object>();

    public Transaction() {

    }

    /**
     * 事务构造方法（基于全局事务id创建新的分支事务）
     * 通过transactionContext传入Xid，默认状态为TRYING:1，默认事务类型为BRANCH:2
     * @param transactionContext
     */
    public Transaction(TransactionContext transactionContext) {
        this.xid = transactionContext.getXid();
        this.status = TransactionStatus.TRYING;
        this.transactionType = TransactionType.BRANCH;
    }

    /**
     * 事务构造方法，传入transactionType，默认状态为TRYING:1，Xid自动生成
     * @param transactionType
     */
    public Transaction(TransactionType transactionType) {
        this.xid = new TransactionXid();
        this.status = TransactionStatus.TRYING;
        this.transactionType = transactionType;
    }

    /**
     * 招募参与者（加入参与者）
     * @param participant
     */
    public void enlistParticipant(Participant participant) {
        participants.add(participant);
    }

    /**
     * 获取事务ID.
     * @return
     */
    public Xid getXid() {
        return xid.clone();
    }

    public TransactionStatus getStatus() {
        return status;
    }


    public List<Participant> getParticipants() {
        return participants;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void changeStatus(TransactionStatus status) {
        this.status = status;
    }


    /**
     * 提交
     */
    public void commit() {
        /**
         * 遍历所有的参与者，调用参与者的提交方法
         */
        for (Participant participant : participants) {
            participant.commit();
        }
    }

    /**
     * 回滚
     */
    public void rollback() {
        /**
         * 遍历所有的参与者，调用参与者的回滚方法
         */
        for (Participant participant : participants) {
            participant.rollback();
        }
    }

    public int getRetriedCount() {
        return retriedCount;
    }

    /**
     * 重试次数+1
     */
    public void addRetriedCount() {
        this.retriedCount++;
    }

    public void resetRetriedCount(int retriedCount) {
        this.retriedCount = retriedCount;
    }

    public Map<String, Object> getAttachments() {
        return attachments;
    }

    public long getVersion() {
        return version;
    }

    public void updateVersion() {
        this.version++;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Date date) {
        this.lastUpdateTime = date;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void updateTime() {
        this.lastUpdateTime = new Date();
    }


}
