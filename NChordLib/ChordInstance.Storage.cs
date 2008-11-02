using System;
using System.Collections.Generic;
using System.Text;
using NChordLib.Storage;

namespace NChordLib
{
    public partial class ChordInstance
    {
        // private storagemanager instance used by the local node
        // todo: figure out better initialization
        ChordStorageManager m_StorageManager = new ChordStorageManager();

        /// <summary>
        /// Adds an object value for a given key to the storage manager for
        /// the local nodeId.
        /// </summary>
        /// <param name="key">The hash key of the value being stored.</param>
        /// <param name="value">The value being stored.</param>
        public void AddKey(ulong key, object value)
        {
            this.AddKey(this.ID, key, value);
        }

        /// <summary>
        /// Adds an object value for a given key to the storage manager for
        /// the supplied nodeId.
        /// </summary>
        /// <param name="nodeId">The id of the node (or physical storage manager) whose store should have the key added to.</param>
        /// <param name="key">The hash key of the value being stored.</param>
        /// <param name="value">The value being stored.</param>
        public void AddKey(ulong nodeId, ulong key, object value)
        {
            if (!this.m_StorageManager.ContainsKey(nodeId))
            {
                // ABC TODO: handle adding new storage managers (e.g. replicas)
                // properly as opposed to just willy-nilly creation of memory stores.
                this.m_StorageManager.Add(nodeId, new MemoryPhysicalStore());
            }

            if (this.m_StorageManager[nodeId].ContainsKey(key))
            {
                // ABC TODO: handle this as an error - we don't want to double-add
            }

            this.m_StorageManager[nodeId].Add(key, value);
            this.m_StorageManager[nodeId].VersionNumber++;
            List<ulong> addedKeys = new List<ulong>();
            addedKeys.Add(key);
            this.m_StorageManager[nodeId].VersionHistory.Add(this.m_StorageManager[nodeId].VersionNumber, addedKeys);

            // replicate...
            if (nodeId == ChordServer.LocalNode.ID)
            {
                foreach (ChordNode successor in this.m_SuccessorCache)
                {
                    ChordServer.CallAddKey(successor, nodeId, key, value);
                }
            }
        }

        /// <summary>
        /// Retrieves a key value from the storage manager for the local node id.
        /// </summary>
        /// <param name="key">The key whose value should be retrieved.</param>
        /// <returns>The key value, or null if the value cannot be retrieved or does not exist.</returns>
        public object FindKey(ulong key)
        {
            return this.FindKey(this.ID, key);
        }
        
        /// <summary>
        /// Retrieves a key value from the storage manager for the given node id.
        /// </summary>
        /// <param name="nodeId">The node id of the storage manager from which the key value should be retrieved.</param>
        /// <param name="key">The key whose value should be retrieved.</param>
        /// <returns>The key value, or null if the value cannot be retrieved or does not exist.</returns>
        public object FindKey(ulong nodeId, ulong key)
        {
            // ABC TODO: if the nodeId does not exist, handle differently
            // from when key does not exist.
            // ABC TODO: locking also needs to be implemented here in order
            // prevent access conflicts.
            if (this.m_StorageManager.ContainsKey(nodeId))
            {
                if (this.m_StorageManager[nodeId].ContainsKey(key))
                {
                    return this.m_StorageManager[nodeId][key];
                }
            }

            // if we can't find anything, we simply return null
            return null;
        }

        /// <summary>
        /// Get the store version number for a given node ID (used mostly by replication).
        /// </summary>
        /// <param name="nodeId">The ID of the store to get the version number for.</param>
        /// <returns>The version number of the store, or 0 if the store does not exist.</returns>
        public ulong GetStoreVersion(ulong nodeId)
        {
            if (this.m_StorageManager.ContainsKey(nodeId))
            {
                return this.m_StorageManager[nodeId].VersionNumber;
            }
            else
            {
                return 0;
            }
        }

        /// <summary>
        /// Update the specified store to reflect the new replica version and add the specified data
        /// to the store and also to the version history.
        /// </summary>
        /// <param name="nodeId">The ID of the store to replicate into.</param>
        /// <param name="replicaVersion">The version to set the store to when complete.</param>
        /// <param name="data">The data to replicate in.</param>
        public void ReplicateIn(ulong nodeId, ulong replicaVersion, SortedList<ulong, object> data)
        {
            if (!this.m_StorageManager.ContainsKey(nodeId))
            {
                this.m_StorageManager.Add(nodeId, new FilePhysicalStore(nodeId));
            }

            this.m_StorageManager[nodeId].ReplicateIn(data);
            this.m_StorageManager[nodeId].VersionNumber = replicaVersion;
            this.m_StorageManager[nodeId].VersionHistory = new SortedList<ulong, List<ulong>>();
            List<ulong> addedKeys = new List<ulong>();
            foreach (ulong key in data.Keys)
            {
                addedKeys.Add(key);
            }
            this.m_StorageManager[nodeId].VersionHistory.Add(replicaVersion, addedKeys);
        }

        /// <summary>
        /// Delete the given store (in cases where the store is no longer needed, or is hopelessly
        /// out of date or incorrect).  This resets the store's version number to zero - if it exists,
        /// and clears out the history and removes the store.
        /// </summary>
        /// <param name="nodeId"></param>
        public void DeleteStore(ulong nodeId)
        {
            if (this.m_StorageManager.ContainsKey(nodeId))
            {
                // remove all entries
                this.m_StorageManager[nodeId].Clear();
                
                // clear and rest version history - note this is likely redundant
                this.m_StorageManager[nodeId].VersionHistory.Clear();
                this.m_StorageManager[nodeId].VersionNumber = 0;
                
                // get rid of the store
                this.m_StorageManager.Remove(nodeId);
            }
        }
    }
}
