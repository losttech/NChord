using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace NChordLib.Storage
{
    /// <summary>
    /// The FilePhysicalStore creates a simple file store on disk given a nodeId and (optional) path.  If
    /// not specified, the file store will be created as a subdirectory of the working directory of the executable
    /// using the FilePhysicalStore.
    /// The design is very simple.  Each key being stored corresponds to a file on disk in the given store path
    /// 
    ///  ( the actual store path is:  <m_StorePath> \ <m_StoreId> \ { ... files, one per <key>, named <key> } )
    /// 
    /// An internal list (m_KeyList) is used to provide enumerable access over all the items.  On startup, the store
    /// directory is checked for any existing files, and adds them to the key list if there are any.  As files are added,
    /// a key is added to m_KeyList in addition to adding the file to disk.
    /// 
    /// When a key is accessed (via the indexer), the correct file is retrieved and its contents are deserialized and returned.
    /// 
    /// NOTE: All objects being stored must be Serializable; for more complex cases, an assembly cache may be needed (when
    /// object types being stored come from an assembly other than the ones that are distributed with all nodes or part of
    /// the standard .NET framework, etc.).
    /// 
    /// </summary>
    class FilePhysicalStore : ChordPhysicalStore
    {
        /// <summary>
        /// the physical path to the store (not including node id) 
        /// </summary>
        private string m_StorePath = ".\\";

        /// <summary>
        /// the id of the store
        /// </summary>
        private ulong m_StoreId = 0;

        /// <summary>
        /// key list (based on file names)
        /// </summary>
        private List<ulong> m_KeyList = new List<ulong>();

        /// <summary>
        /// Creates a new instance of the file physical store for a given node id using the
        /// default path.
        /// </summary>
        /// <param name="nodeId">The node/store id to use in creating the store.</param>
        public FilePhysicalStore(ulong nodeId)
        {
            this.m_StoreId = nodeId;
            Initialize();
        }

        /// <summary>
        /// Creates a new instance of the file physical store for a given node id using
        /// the specified path.
        /// </summary>
        /// <param name="storePath">The physical parent path (including trailing backslash) of the store.</param>
        /// <param name="nodeId">The node/store id to use in creating the store.</param>
        public FilePhysicalStore(string storePath, ulong nodeId)
        {
            this.m_StorePath = storePath;
            this.m_StoreId = nodeId;
            Initialize();
        }

        /// <summary>
        /// Initialize the data store by emptying out the cached key list
        /// and rebuilding it given the path information stored in the local members.
        /// </summary>
        private void Initialize()
        {
            this.m_KeyList.Clear();

            // check if the path exists; create if it doesn't
            try
            {
                if (!Directory.Exists(this.m_StorePath + this.m_StoreId))
                {
                    Directory.CreateDirectory(this.m_StorePath + this.m_StoreId);
                }

                // iterate through all files and add the valid ones
                foreach (string file in Directory.GetFiles(this.m_StorePath + this.m_StoreId))
                {
                    try
                    {
                        this.m_KeyList.Add((ulong)Convert.ToInt64(file));
                        this.VersionNumber = 0; // we are starting out from scratch but need to add all the keys that have been added
                                                // to the replication history
                        this.VersionHistory[0].Add((ulong)Convert.ToInt64(file));
                    }
                    catch
                    {
                        // ignore files that are not keys
                    }
                }
            }
            catch
            {
                // log the exception but do not halt things
                // note: this is not the ideal semantics, most likely
            }
        }

        /// <summary>
        /// Retrieve the object that is stored for the given key.
        /// </summary>
        /// <param name="key">The key value to retrieve the object for.</param>
        /// <returns>The deserialized object; or null if unable to retrieve or deserialize.</returns>
        public new object this[ulong key]
        {
            get 
            {
                if (File.Exists(this.m_StorePath + this.m_StoreId + "\\" + key))
                {
                    BinaryFormatter formatter = new BinaryFormatter();
                    FileStream file = File.OpenRead(this.m_StorePath + this.m_StoreId + "\\" + key);
                    object retObj = formatter.Deserialize(file);
                    file.Close();
                    return retObj;
                }
                                
                return null; 
            }
        }

        /// <summary>
        /// Indicates whether or not the specified key is considered stored by this store.
        /// </summary>
        /// <param name="key">The key to check.</param>
        /// <returns>True if this key is stored in this store; false otherwise.</returns>
        public new bool ContainsKey(ulong key)
        {
            return this.m_KeyList.Contains(key);
        }

        /// <summary>
        /// Adds a key to the store by creating a file for it on disk and serializing the
        /// object to that file, and recording the key in the key cache.
        /// </summary>
        /// <param name="key">The key whose value is to be added.</param>
        /// <param name="value">The value to store on disk.</param>
        public new void Add(ulong key, object value)
        {
            try
            {
                BinaryFormatter formatter = new BinaryFormatter();
                FileStream file = File.Create(this.m_StorePath + this.m_StoreId + "\\" + key);
                formatter.Serialize(file, value);
                file.Close();
            }
            catch
            {
                // if this doesn't work, log the error (TODO)
            }

            return;
        }

        /// <summary>
        /// Clear out all cached and disk-ed keys/values.
        /// </summary>
        public new void Clear()
        {
            foreach (ulong key in this.m_KeyList)
            {
                try
                {
                    File.Delete(this.m_StorePath + this.m_StoreId + "\\" + key);
                }
                catch
                {
                    // if we can't clear a file, log and keep moving for now
                }
            }

            this.m_KeyList.Clear();
        }

        //ABC TODO: hook this up to ienumerable

        /// <summary>
        /// For enumeration over all keys in the store (e.g. by replication, etc.).
        /// </summary>
        public new IList<ulong> Keys
        {
            get { return this.m_KeyList; }
        }

        /// <summary>
        /// Replicate data into the store (each key calls the internal Add method).
        /// </summary>
        /// <param name="data">The data to replicate in.</param>
        public override void ReplicateIn(SortedList<ulong, object> data)
        {
            foreach (ulong key in data.Keys)
            {
                this.Add(key, data[key]);
            }
        }
    }
}
