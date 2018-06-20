package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.*;

/*
*类名和方法不能修改
 */
public class Schedule {

    private List<Integer> nodes = new ArrayList<Integer>();
    private List<Integer> tasks = new ArrayList<Integer>();

    private Map<Integer,List<TaskInfo>> taskStatus = new HashMap<Integer, List<TaskInfo>>();
    private Map<Integer,Integer> taskMap = new HashMap<Integer,Integer>();

    private Map<Integer,List<Integer>> sameTasks = new HashMap<Integer, List<Integer>>();

    private int threshold = 0;



    Comparator<TaskInfo> comparator = new Comparator<TaskInfo>(){
        public int compare(TaskInfo taskInfo1, TaskInfo taskInfo2) {
            return (taskInfo1.getTaskId()-taskInfo2.getTaskId());
        }
    };

    Comparator<TaskInfo> comparatorByNodeId = new Comparator<TaskInfo>(){
        public int compare(TaskInfo taskInfo1, TaskInfo taskInfo2) {
            return (taskInfo1.getNodeId()-taskInfo2.getNodeId());
        }
    };

    Comparator<Integer> comparatorByTime = new Comparator<Integer>(){
        public int compare(Integer integer1, Integer integer2) {
    taskInfo1
            return (taskMap.get(integer2)-taskMap.get(integer1));
        }
    };

    public int init() {

        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
        //节点id 小于0返回
        if(nodeId <= 0)
        {
            return ReturnCodeKeys.E004;
        }

        //节点已包含返回e005
        if (nodes.contains(nodeId))
        {
            return ReturnCodeKeys.E005;
        }

        nodes.add(nodeId);
        Collections.sort(nodes);
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {

        //服务节点小于等于0 非法
        if(nodeId <= 0)
        {
            return ReturnCodeKeys.E004;
        }

        //服务节点号未注册
        if(!nodes.contains(nodeId))
        {
            return ReturnCodeKeys.E007;
        }
        nodes.remove(new Integer(nodeId));
        return ReturnCodeKeys.E006;
    }


    public int addTask(int taskId, int consumption) {

        //服务节点小于等于0 非法
        if(taskId<=0)
        {
            return ReturnCodeKeys.E009;
        }
        // 任务已添加
        if(tasks.contains(taskId))
        {
            return ReturnCodeKeys.E010;
        }
        tasks.add(taskId);
        taskMap.put(taskId,consumption);
        Collections.sort(tasks,comparatorByTime);
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {

        //服务节点小于等于0 非法
        if(taskId<=0)
        {
            return ReturnCodeKeys.E009;
        }

        //任务节点不存在
        if(!tasks.contains(taskId))
        {
            return ReturnCodeKeys.E012;
        }

        tasks.remove(new Integer(taskId));
        taskMap.remove(new Integer(taskId));
        return ReturnCodeKeys.E011;
    }


    private int countTaskInfos(List<TaskInfo> taskInfos){
        int result = 0;
        for(TaskInfo taskInfo:taskInfos){
            result+=taskMap.get(taskInfo.getTaskId());
        }
        return result;
    }

    private int findNodes() {
        int tmpId = -1;
        int min = Integer.MAX_VALUE;
        for(Integer nodeId:nodes){
            List<TaskInfo> taskInfos = taskStatus.get(nodeId);

            if(taskInfos==null){
                return nodeId;
            }else{
                int count = countTaskInfos(taskInfos);
                if(count<min){
                    min = count;
                    tmpId = nodeId;
                }
            }
        }
        return tmpId;
    }

    private boolean calcBalance(int nodeId){
        int source = countTaskInfos(taskStatus.get(nodeId));
        for(Integer id:nodes){
            if(!id.equals(nodeId)){
                int count = 0;
                if(taskStatus.get(id)==null){
                    count=0;
                }else{
                    count = countTaskInfos(taskStatus.get(id));
                }

                if(Math.abs(count-source)>this.threshold) return false;
            }
        }
        return true;
    }

    private void insertSameTask(int taskId){
        int time = taskMap.get(taskId);
        List<Integer> list = sameTasks.get(time);
        if(list==null){
            list = new ArrayList<Integer>();
            sameTasks.put(time,list);
        }
        list.add(taskId);
    }

    public int scheduleTask(int threshold) {
        if(tasks.isEmpty())
        {
            return ReturnCodeKeys.E014;
        }
        this.threshold = threshold;
        boolean balanced = false;


        List<Integer> tmpTasks = new ArrayList<Integer>();
        for(Integer taskId:tasks){
            tmpTasks.add(taskId);
        }
        for(Integer nodeId:nodes){
            List<TaskInfo> taskInfos = new ArrayList<TaskInfo>();
            taskStatus.put(nodeId,taskInfos);
        }

        while(!balanced || tmpTasks.size()>0){
            for(Integer taskId:tmpTasks){
                int nodeId = findNodes();
                List<TaskInfo> taskInfos = taskStatus.get(nodeId);
                TaskInfo taskInfo = new TaskInfo();
                taskInfo.setTaskId(taskId);
                taskInfo.setNodeId(nodeId);
                taskInfos.add(taskInfo);
                tmpTasks.remove(new Integer(taskId));
                insertSameTask(taskId);
                balanced = calcBalance(nodeId);
                break;
            }
            if(tmpTasks.size()==0 && !balanced)
                return ReturnCodeKeys.E014;
        }

        for (Integer time:sameTasks.keySet()){
            List<Integer> list = sameTasks.get(time);
            if(list.size()>1){

                List<TaskInfo> tasks = new ArrayList<TaskInfo>();
                for(Integer nodeId:taskStatus.keySet()){
                    List<TaskInfo> taskInfos = taskStatus.get(nodeId);
                    for(TaskInfo ti:taskInfos){
                        if(list.contains(ti.getTaskId())){
                            tasks.add(ti);
                        }
                    }
                }
                Collections.sort(tasks,comparatorByNodeId);
                Collections.sort(list);
                for(int i=0;i<tasks.size();i++){
                    TaskInfo ti = tasks.get(i);
                    ti.setTaskId(list.get(i));
                }
            }
        }


        return ReturnCodeKeys.E013;
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {

        //tasks 为空
        if(null == tasks || tasks.size() <= 0 )
        {
            return ReturnCodeKeys.E016;
        }

        //查询tasks
        for(Integer nodeId:taskStatus.keySet()){
            tasks.addAll(taskStatus.get(nodeId));
        }
        Collections.sort(tasks,comparator);
        System.out.println(tasks);
        return ReturnCodeKeys.E015;
    }

}
