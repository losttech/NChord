/*
 * ChordInstance.Maintenance.cs:
 * 
 *  Implementation of the core maintenance facilities that are used to keep the Chord node sane
 *  and in sync with the rest of the Chord ring.  Maintenance tasks run as background worker tasks
 *  and are started on Join and stopped on Depart.
 * 
 *  There are many ways in which maintenance could be implemented.  A possible improvement to this
 *  facility would be to allow for seamless plugability.
 * 
 */

using System;
using System.Threading;
using System.ComponentModel;

namespace NChordLib
{
    public partial class ChordInstance : MarshalByRefObject
    {
        private BackgroundWorker m_StabilizeSuccessors = new BackgroundWorker();
        private BackgroundWorker m_StabilizePredecessors = new BackgroundWorker();
        private BackgroundWorker m_UpdateFingerTable = new BackgroundWorker();
        private BackgroundWorker m_Rejoin = new BackgroundWorker();
        private BackgroundWorker m_ReplicateStorage = new BackgroundWorker();

        /// <summary>
        /// Start the maintenance tasks to run as background worker threads.
        /// </summary>
        private void StartMaintenance()
        {
            m_StabilizeSuccessors.DoWork += new DoWorkEventHandler(this.StabilizeSuccessors);
            m_StabilizeSuccessors.WorkerSupportsCancellation = true;
            m_StabilizeSuccessors.RunWorkerAsync();

            m_StabilizePredecessors.DoWork += new DoWorkEventHandler(this.StabilizePredecessors);
            m_StabilizePredecessors.WorkerSupportsCancellation = true;
            m_StabilizePredecessors.RunWorkerAsync();

            m_UpdateFingerTable.DoWork += new DoWorkEventHandler(this.UpdateFingerTable);
            m_UpdateFingerTable.WorkerSupportsCancellation = true;
            m_UpdateFingerTable.RunWorkerAsync();

            m_Rejoin.DoWork += new DoWorkEventHandler(this.ReJoin);
            m_Rejoin.WorkerSupportsCancellation = true;
            m_Rejoin.RunWorkerAsync();

            m_ReplicateStorage.DoWork += new DoWorkEventHandler(this.ReplicateStorage);
            m_ReplicateStorage.WorkerSupportsCancellation = true;
            m_ReplicateStorage.RunWorkerAsync();
        }

        /// <summary>
        /// Stop the maintenance tasks (asynchronously) that are currently running.
        /// </summary>
        private void StopMaintenance()
        {
            m_StabilizeSuccessors.CancelAsync();
            m_StabilizePredecessors.CancelAsync();
            m_UpdateFingerTable.CancelAsync();
            m_Rejoin.CancelAsync();
            m_ReplicateStorage.CancelAsync();
        }
    }
}
