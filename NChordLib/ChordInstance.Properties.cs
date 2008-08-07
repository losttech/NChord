/*
 * ChordInstance.Properties.cs:
 * 
 *  Contains core public properties and members for a given ChordInstance.
 *  These properties and members are used primarily for navigation and maintenance, but are also
 *  exposed off of ChordInstance for use by other applications as well.
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

using System;

namespace NChordLib
{
    public partial class ChordInstance : MarshalByRefObject
    {
        /// <summary>
        /// The SeedNode is cached for re-join purposes in case ring partition is detected.  Alternately,
        /// an array (similar to SuccessorCache) could be used to cache multiple seed nodes in case the
        /// original seed node has disappeared.
        /// </summary>
        private ChordNode m_SeedNode = null;

        /// <summary>
        /// The host name that identifies this ChordInstance.
        /// </summary>
        public string Host
        {
            get 
            { 
                return ChordServer.LocalNode.Host;
            }
        }

        /// <summary>
        /// The port on which this ChordInstance is listening.
        /// </summary>
        public int Port
        {
            get 
            { 
                return ChordServer.LocalNode.PortNumber;
            }
        }

        /// <summary>
        /// The ID is the hash value that corresponds to the current ChordInstance, and is used, amongst
        /// other things, to determine the correct position in a Chord ring for the ChordInstance and its
        /// neighbors.
        /// </summary>
        public UInt64 ID
        {
            get 
            { 
                return ChordServer.LocalNode.ID;
            }
        }

        /// <summary>
        /// The Successor is the ChordNode that follows the current ChordInstance in the Chord ring.
        /// Since the Successor is also the first item in the SuccessorCache, we simply get/set out of the
        /// first item stored in the SuccessorCache.
        /// </summary>
        public ChordNode Successor
        {
            get 
            { 
                return this.m_SuccessorCache[0]; 
            }
            set
            {
                if (value == null && value != this.m_SuccessorCache[0])
                {
                    ChordServer.Log(LogLevel.Info, "Navigation", "Setting successor to null.");
                }
                else if (value != null && 
                    (this.m_SuccessorCache[0] == null || this.m_SuccessorCache[0].ID != value.ID))
                {
                    ChordServer.Log(LogLevel.Info, "Navigation", "New Successor {0}.", value);
                }
                this.m_SuccessorCache[0] = value;
            }
        }

        private ChordNode m_Predecessor = null;
        /// <summary>
        /// The Predecessor is the ChordNode that precedes the current ChordInstance in the Chord ring.
        /// </summary>
        public ChordNode Predecessor
        {
            get 
            { 
                return this.m_Predecessor; 
            }
            set
            {
                if (value == null && value != this.m_Predecessor)
                {
                    ChordServer.Log(LogLevel.Info, "Navigation", "Setting predecessor to null.");
                }
                else if (value != null && 
                    (this.m_Predecessor == null || this.m_Predecessor.ID != value.ID))   // (otherwise, no change...)
                {
                    ChordServer.Log(LogLevel.Info, "Navigation", "New Predecessor {0}.", value);
                }
                this.m_Predecessor = value;
            }
        }

        private ChordNode[] m_SuccessorCache;
        /// <summary>
        /// The SuccessorCache is used to keep the N (N == SuccessorCache.Length) closest successors
        /// to this ChordInstance handy.  Different values for the size of the SuccessorCache length
        /// can impact performance under churn and in varying (often, smaller) sized Chord rings.
        /// </summary>
        public ChordNode[] SuccessorCache
        {
            get 
            { 
                return this.m_SuccessorCache; 
            }
            set
            {
                this.m_SuccessorCache = value;
            }
        }

        private ChordFingerTable m_FingerTable;
        /// <summary>
        /// The FingerTable contains reasonably up-to-date successor ChordNode owners for exponentially
        /// distant ID values.  The FingerTable is maintained in the background by the maintenance
        /// processes and is used in navigation as a shortcut when possible.
        /// </summary>
        public ChordFingerTable FingerTable
        {
            get 
            { 
                return this.m_FingerTable; 
            }
            set 
            { 
                this.m_FingerTable = value; 
            }
        }
    }
}
