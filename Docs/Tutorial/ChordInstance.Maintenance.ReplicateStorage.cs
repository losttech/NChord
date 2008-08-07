/*
 * ChordInstance.Maintenance.ReplicateStorage.cs:
 * 
 * Perform extremely simple replication of data store to
 * successor as a maintenance task.
 * 
 */

using System;
using System.ComponentModel;
using System.Threading;

namespace NChordLib
{
    public partial class ChordInstance : MarshalByRefObject
    {
        /// <summary>
        /// Replicate the local data store on a background thread.
        /// </summary>
        /// <param name="sender">The background worker thread this task is running on.</param>
        /// <param name="ea">Args (ignored).</param>
        private void ReplicateStorage(object sender, DoWorkEventArgs ea)
        {
            BackgroundWorker me = (BackgroundWorker)sender;

            while (!me.CancellationPending)
            {
                try
                {
                    // replicate each key to the successor safely
                    foreach (ulong key in this.m_DataStore.Keys)
                    {
                        // if the key is local (don't copy replicas)
                        if (ChordServer.IsIDInRange(key, this.ID, this.Successor.ID))
                        {
                            ChordServer.CallReplicateKey(this.Successor, key, this.m_DataStore[key]);
                        }
                    }
                }
                catch (Exception e)
                {
                    // (overly safe here)
                    ChordServer.Log(LogLevel.Error, "Maintenance", "Error occured during ReplicateStorage ({0})", e.Message);
                }

                // TODO: make this configurable via config file or passed in as an argument
                Thread.Sleep(30000);
            }
        }
    }
}
