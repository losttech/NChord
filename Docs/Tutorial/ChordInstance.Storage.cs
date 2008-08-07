/*
 * ChordInstance.Storage.cs:
 * 
 *  Contains private data structure and public methods for
 *  key-value data storage.
 * 
 */

using System;
using System.Collections.Generic;

namespace NChordLib
{
    public partial class ChordInstance : MarshalByRefObject
    {
        /// <summary>
        /// The data structure used to store string data given a 
        /// 64-bit (ulong) key value.
        /// </summary>
        private SortedList<ulong, string> m_DataStore = new SortedList<ulong, string>();

        /// <summary>
        /// Add a key to the store.  Gets a hash of the key, determines
        /// the correct owning node, and stores the string value
        /// on that node.
        /// </summary>
        /// <param name="value">The value to add.</param>
        public void AddKey(string value)
        {
            // the key is the hash of the value to
            // add to the store, and determines the
            // owning NChord node
            ulong key = ChordServer.GetHash(value);

            // using the local node, determine the correct owning
            // node for the data to be stored given the key value
            ChordNode owningNode = ChordServer.CallFindSuccessor(key);

            if (owningNode != ChordServer.LocalNode)
            {
                // if this is not the owning node, then call AddKey
                // on the actual owning node
                ChordServer.CallAddKey(owningNode, value);
            }
            else
            {
                // if this is the owning node, then add the
                // key to the local data store
                this.m_DataStore.Add(key, value);
            }
        }

        /// <summary>
        /// Retrieve the string value for a given ulong
        /// key.
        /// </summary>
        /// <param name="key">The key whose value should be returned.</param>
        /// <returns>The string value for the given key, or an empty string if not found.</returns>
        public string FindKey(ulong key)
        {
            // determine the owning node for the key
            ChordNode owningNode = ChordServer.CallFindSuccessor(key);

            if (owningNode != ChordServer.LocalNode)
            {
                // if this is not the owning node, call
                // FindKey on the remote owning node
                return ChordServer.CallFindKey(owningNode, key);
            }
            else
            {
                // if this is the owning node, check
                // to see if the key exists in the data store
                if (this.m_DataStore.ContainsKey(key))
                {
                    // if the key exists, return the value
                    return this.m_DataStore[key];
                }
                else
                {
                    // if the key does not exist, return empty string
                    return string.Empty;
                }
            }
        }

        /// <summary>
        /// Add the given key/value pair as replicas to the local store.
        /// </summary>
        /// <param name="key">The key to replicate.</param>
        /// <param name="value">The value to replicate.</param>
        public void ReplicateKey(ulong key, string value)
        {
            // add the key/value pair to the local
            // data store regardless of ownership
            if (!this.m_DataStore.ContainsKey(key))
            {
                this.m_DataStore.Add(key, value);
            }
        }
    }
}