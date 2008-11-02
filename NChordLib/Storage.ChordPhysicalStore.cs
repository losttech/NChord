using System;
using System.Collections.Generic;
using System.Text;

namespace NChordLib.Storage
{
    /// <summary>
    /// ChordPhysicalStore is the generic base class for all subclass instances
    /// of a physical store (e.g. file storage, memory storage, etc.).  The store
    /// is kept internal to the ChordInstance (which exposes just wrapped Add/Find methods
    /// to remoting).  The physical store exposes whatever storage being used as a simplified
    /// SortedList, keyed on a 64-bit hash key, with a single value that is of type object.
    /// Physical storage can represent the value however it wants (e.g. byte[], etc.), but
    /// must expose the stored value as type object.  
    /// 
    /// If an assembly cache is added to the store (in order to work with user defined data types from a third-party assembly not
    /// shipped as part of the ChordServer distribution), it could be implemented as just
    /// another instance of the PhysicalStore.
    ///
    /// Instances of the ChordPhysicalStore should do all initialization through the constructor
    /// as opposed to exposing a separate Init method.
    /// </summary>
    public abstract class ChordPhysicalStore : SortedList<ulong, object>
    {
        private ulong m_VersionNumber = 0;
        /// <summary>
        /// The VersionNumber exposes the current version of the store as used by replication.
        /// We start at zero and move upwards on each transaction or replication update.
        /// </summary>
        public ulong VersionNumber
        {
            get { return this.m_VersionNumber; }
            set { this.m_VersionNumber = value; }
        }

        private SortedList<ulong, List<ulong>> m_VersionHistory = new SortedList<ulong, List<ulong>>();
        /// <summary>
        /// The VersionHistory maintains a tracked list of changes by version.  This can then be
        /// replayed to the remote node on replication.  The schema is: changeId (ulong), changedIds (List<ulong>).
        /// </summary>
        public SortedList<ulong, List<ulong>> VersionHistory
        {
            get { return this.m_VersionHistory; }
            set { this.m_VersionHistory = value; }
        }

        /// <summary>
        /// Used by replication, replicates into the store a set of keys/values to bring the replica up to date.
        /// </summary>
        /// <param name="data">They keys/values to replicate into the store.</param>
        public abstract void ReplicateIn(SortedList<ulong, object> data);
    }
}
