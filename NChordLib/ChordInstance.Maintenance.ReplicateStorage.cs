/*
 * ChordInstance.Maintenance.ReplicateStorage.cs:
 * 
 * Perform extremely simple replication of data store to
 * successor as a maintenance task.
 * 
 */

using System;
using System.Collections.Generic;
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
                    foreach (ChordNode successor in this.m_SuccessorCache)
                    {
                        ulong remoteVersion = ChordServer.CallGetStoreVersion(successor, ChordServer.LocalNode.ID);
                        ulong localVersion = ChordServer.CallGetStoreVersion(ChordServer.LocalNode.ID);

                        if (remoteVersion != localVersion)
                        {
                            // the data to ship
                            SortedList<ulong, object> data = new SortedList<ulong, object>();

                            if (remoteVersion == 0)
                            {
                                // ship everything
                                foreach (ulong key in this.m_StorageManager[ChordServer.LocalNode.ID].Keys)
                                {
                                    if (!data.ContainsKey(key))
                                    {
                                        data.Add(key, this.m_StorageManager[ChordServer.LocalNode.ID][key]);
                                    }
                                }
                            }
                            else if (localVersion < remoteVersion)
                            {
                                // delete remote store and 
                                ChordServer.CallDeleteStore(successor, ChordServer.LocalNode.ID);
                                // ship everything
                                foreach (ulong key in this.m_StorageManager[ChordServer.LocalNode.ID].Keys)
                                {
                                    if (!data.ContainsKey(key))
                                    {
                                        data.Add(key, this.m_StorageManager[ChordServer.LocalNode.ID][key]);
                                    }
                                }
                            }
                            else
                            {
                                // ship whatever has changed since the remote version
                                foreach (ulong key in this.m_StorageManager[ChordServer.LocalNode.ID].VersionHistory.Keys)
                                {
                                    foreach (ulong id in this.m_StorageManager[ChordServer.LocalNode.ID].VersionHistory[key])
                                    {
                                        if (!data.ContainsKey(id))
                                        {
                                            data.Add(id, this.m_StorageManager[ChordServer.LocalNode.ID][id]);
                                        }
                                    }
                                }
                            }

                            ChordServer.CallReplicateIn(successor, ChordServer.LocalNode.ID, localVersion, data);
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
