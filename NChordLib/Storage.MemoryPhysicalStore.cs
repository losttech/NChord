using System;
using System.Collections.Generic;
using System.Text;

namespace NChordLib.Storage
{
    /// <summary>
    /// The MemoryPhysicalStore creates a simple in-memory store.  The internal representation is,
    /// in fact, identical to the SortedList<ulong, object> of the abstract base class, so very
    /// little needs imlementation here.
    /// </summary>
    class MemoryPhysicalStore : ChordPhysicalStore
    {
        /// <summary>
        /// Replicate in a set of keys / values by merging them into the in-memory list.
        /// </summary>
        /// <param name="data">The keys / values to merge in.</param>
        public override void ReplicateIn(SortedList<ulong, object> data)
        {
            foreach (ulong key in data.Keys)
            {
                this.Add(key, data[key]);
            }
        }
    }
}
