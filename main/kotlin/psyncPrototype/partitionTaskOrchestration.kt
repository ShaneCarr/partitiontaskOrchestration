/*
 This is a missing component from orchestration -------------->

 the purpose of this class is to demonstrate associating n-Tasks with partitions,

 this prevents duplicate tasks running due to race conditions generating tasks.

 note it is still possible that a task with a set of data will run multiple times.

  This complexity is pushed to the task author this is why files are copied to break their tasks in an item potent way.
  this code merely protects from generating duplicate work where that is not safe or desireable

  Each tasks has an input and an output

 this change make it so that only one out1 is selected. it doesn't prevent side effects being generated from t1 if run multiple times.

  out1 = t1(in0)

  note this is ok:
  out2=t2(out1)
  out2=t2(out1)

  without this change this can happen
  out1 = t1(in0)
  out2 = t1(in0)

  out2=t2(out1)
  out2=t2(out2)

  this is prevented because we use a transactionid to "pick out1"
  so the end result of each tasks is on transaction not multiple, which is unstable
 */
package psyncPrototype

import java.util.*



class ServiceLocator(){
    val partitionTable = ArrayList<Partition>();
}

data class Payload( val partitionId:Long, val startId:Int, val endId:Int, val transactionId: UUID)
data class currentItem (val fileId:Int, val partition: Partition)

// database
class Partition(val id: Long){
    // you can raise 2^n to select the nth bit to generate ids for tasks.
    // 32 bit int can support 32 tasks
    // 11111111 11111111 11111111 11111111
    var claimedTasks:Int = 0
    var tid: UUID? = null
}



// service
// technically this is a tasks. but simplifying redundant demonstration
class PartitionCreator {
    fun execute(svc: ServiceLocator)
    {
        val currentItem = advanceStartPtrToNextSetOfFiles(svc)

        // this isn't exact because i have created a parameter object for partition
        // and orchestration args that take a tid, and an instant.now.

        // create/enqueue task.
        val payload = Payload(currentItem.partition.id, currentItem.fileId, currentItem.fileId, UUID.randomUUID())
        val createJobTask1 = CreateJobTask(svc)

        // not able to advance the current partition to the next set of files, so we come back in with same current item
        // transaction id is UUID.RandomUUID which is never stored is generated each time and provides a means to detect this.
        val payload2 = Payload(currentItem.partition.id, currentItem.fileId, currentItem.fileId, UUID.randomUUID())

        // will work
        createJobTask1.execute(payload)

        // won't work
        createJobTask1.execute(payload2)
    }

    /*
    Load the partition id from the current Item if it is set, or creates a partition item.
    move next can be teh point we create teh partition record
    advanceStartPtrToNextSetOfFiles can create partition / store it.
     */
    private fun advanceStartPtrToNextSetOfFiles(svc: ServiceLocator): currentItem {
        // current item will store a partition id before we attempt to enqueue the item current item, then we create that record onetime
        // associated with the  job
        val partition = Partition(1)
        svc.partitionTable.add(partition)
        return currentItem(1, partition)
    }
}

/*
 claim uses
 -a bitmask to signal information about the tasks in the  partition
 - a transaction id to dedupe an instance of the  task.

 this code should really not be in the tasks.
 it should land in the "frameowrk" orchestration code, and ideally not even the base class.
 */
// tasks are have partitions
open class Task(val taskId: Int, val svc: ServiceLocator) {
    fun claim(tid: UUID, partitionId: Long): Boolean {

        // this is a transaction

        // Update Partition set tid = {tid}, state=state&{taskId} WHERE state&{taskId} = 0
        // ONE SQL STATEMENT

        // ==========>
        // where clause:
        val partition: Partition? = svc.partitionTable.find { pRow -> pRow.id == partitionId && pRow.claimedTasks.and(taskId) != taskId }
        // update statement:
        if (partition != null) {
            partition.tid = tid
            partition.claimedTasks = partition.claimedTasks.or(taskId)
        }

        // this is ok to do outside of the transaction ( a seperate call in the claim function)
        val claimedOrNot: Partition? = svc.partitionTable.find { pRow -> pRow.id == partitionId && pRow.tid == tid }

        if(claimedOrNot != null) {
            return true
        }

        return false
    }
}



// creates the sharepoint job
class CreateJobTask( svc: ServiceLocator) : Task(1, svc){

    fun execute(payload: Payload){

        // this can go in base class, or in the caller
        if (!claim(payload.transactionId, payload.partitionId)){
            println("NOT RUNNING this instance ${payload} ")
            return // this is the state to stop the task /give up
        }

        println("RUNNING this instance ${payload} ")
        val payload3 = Payload(payload.partitionId, payload.startId, payload.endId, UUID.randomUUID())
        val payload4 = Payload(payload.partitionId, payload.startId, payload.endId, UUID.randomUUID())
        val startJobTask  = StartJobTask(svc)

        // this is just another example to prove a point that it is completely fine to generate a new task, and it all will be still blocked
        val startJobTask2  = StartJobTask(svc)

        // this should work
        startJobTask.execute(payload3)

        // this will work.
        startJobTask.execute(payload3)

        // this won't work'
        startJobTask2.execute(payload4)
    }
}

// starts the sharepoint job
class StartJobTask( svc: ServiceLocator) : Task(2, svc){
    fun execute(payload: Payload){

        // this can go in base class, or in the caller
        if (!claim(payload.transactionId, payload.partitionId)){
            println("NOT RUNNING this instance ${payload} ")
            return // this is the state to stop the task /give up
        }

        println("RUNNING this instance ${payload} ")
    }
}

// completes the sharepoint job
class CompleteJobTask(svc: ServiceLocator) : Task(8, svc){
    // same...
}


fun main(args: Array<String>) {
    val service = ServiceLocator()
    val partitionCreator = PartitionCreator()
    partitionCreator.execute(service)
    println("Running migration")
}

