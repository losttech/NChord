using System;
using System.Collections.Generic;
using System.Text;

namespace NChordLib.Storage
{
    /// <summary>
    /// The ChordStorageManager maintains a sortedlist of ChordPhysicalStore instances,
    /// keyed off of a 64-bit hash id (equal to the node id of the "owning" node of the
    /// data store - e.g. the local node id or the node id of some replica being stored
    /// here).  
    /// 
    /// For those extending NChord, ChordStorageManager should ideally be set up
    /// to accept or read some sort of initial configuration so that it can bootstrap "default"
    /// or known data stores on startup, as well as in order to define certain standard
    /// behaviors/parameters that could be needed by the ChordStorageManager.
    /// 
    /// ABC Note (11/1): TODO: In the future I can flesh out configuration properties.
    /// </summary>
    public class ChordStorageManager : SortedList<ulong, ChordPhysicalStore>
    {
    }
}
