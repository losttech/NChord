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
 * ****************************************************************************
 *  Copyright (c) 2008 Andrew Cencini
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * ****************************************************************************
 */

namespace NChord
{
    using System;
    using System.ComponentModel;

    public partial class ChordInstance : MarshalByRefObject
    {
        private BackgroundWorker m_StabilizeSuccessors = new BackgroundWorker();
        private BackgroundWorker m_StabilizePredecessors = new BackgroundWorker();
        private BackgroundWorker m_UpdateFingerTable = new BackgroundWorker();
        private BackgroundWorker m_Rejoin = new BackgroundWorker();

        /// <summary>
        /// Start the maintenance tasks to run as background worker threads.
        /// </summary>
        private void StartMaintenance()
        {
            this.m_StabilizeSuccessors.DoWork += new DoWorkEventHandler(this.StabilizeSuccessors);
            this.m_StabilizeSuccessors.WorkerSupportsCancellation = true;
            this.m_StabilizeSuccessors.RunWorkerAsync();

            this.m_StabilizePredecessors.DoWork += new DoWorkEventHandler(this.StabilizePredecessors);
            this.m_StabilizePredecessors.WorkerSupportsCancellation = true;
            this.m_StabilizePredecessors.RunWorkerAsync();

            this.m_UpdateFingerTable.DoWork += new DoWorkEventHandler(this.UpdateFingerTable);
            this.m_UpdateFingerTable.WorkerSupportsCancellation = true;
            this.m_UpdateFingerTable.RunWorkerAsync();

            this.m_Rejoin.DoWork += new DoWorkEventHandler(this.ReJoin);
            this.m_Rejoin.WorkerSupportsCancellation = true;
            this.m_Rejoin.RunWorkerAsync();
        }

        /// <summary>
        /// Stop the maintenance tasks (asynchronously) that are currently running.
        /// </summary>
        private void StopMaintenance()
        {
            this.m_StabilizeSuccessors.CancelAsync();
            this.m_StabilizePredecessors.CancelAsync();
            this.m_UpdateFingerTable.CancelAsync();
            this.m_Rejoin.CancelAsync();
        }
    }
}
