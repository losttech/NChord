/*
 * ChordInstance.Navigation.cs:
 * 
 *  The two core navigation methods, FindSuccessor and FindClosestPrecedingFinger are implemented here.
 * 
 *  FindSuccessor and FindClosestPrecedingFinger are implemented based on the specifications of the Chord
 *  paper, where the node that is the proper owner (successor) for a given ID value is located.  If the node
 *  on which FindSuccessor has been invoked is not the proper predecessor for ID, the finger table is consulted in
 *  order to determine a closer node, on whom FindSuccessor is (safely) remotely invoked.
 * 
 *  FindClosestPrecedingFinger works through the finger table until the finger with the closest preceding
 *  startValue to the specified ID is found, and the cached Successor ChordNode is returned to the caller.
 * 
 *  In addition to the standard machinery for FindSuccessor, a simple hopCount has been added for tracking
 *  and verifying efficiency of the DHT.
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
        /// Find the node that is the rightful owner of a given id.
        /// </summary>
        /// <param name="id">The id value whose successor should be found.</param>
        /// <returns>The ChordNode that is the Successor of the given ID value.</returns>
        public ChordNode FindSuccessor(UInt64 id)
        {
            int hopCountOut = 0;    // ignore hopCount
            return FindSuccessor(id, 0, out hopCountOut);
        }

        /// <summary>
        /// Find the node that is the rightful owner of a given id.
        /// </summary>
        /// <param name="id">The id whose successor should be found.</param>
        /// <param name="hopCount">The number of network hops taken in finding the successor.</param>
        /// <returns>The ChordNode that is the Successor of a given ID value.</returns>
        public ChordNode FindSuccessor(UInt64 id, int hopCountIn, out int hopCountOut)
        {
            // is the local node's successor the rightful owner?
            if (ChordServer.IsIDInRange(id, this.ID, this.Successor.ID))
            {
                hopCountOut = hopCountIn;
                return this.Successor;
            }
            else
            {
                // otherwise, find the nearest preceding finger, and ask that node.
                ChordNode predNode = FindClosestPrecedingFinger(id);
                return ChordServer.CallFindSuccessor(predNode, id, 0, ++hopCountIn, out hopCountOut);
            }
        }

        /// <summary>
        /// Returns the closest successor preceding id.
        /// </summary>
        /// <param name="id">The id for which the closest finger should be found</param>
        /// <returns>The successor node of the closest finger to id in the current node's finger table</returns>
        private ChordNode FindClosestPrecedingFinger(UInt64 id)
        {
            // iterate downward through the finger table looking for the right finger in the right range. if the finger is 
            // in the range but not valid, keep moving. if the entire finger table is checked without success, check the successor 
            // cache - if that fails, return the local node as the closest preceding finger.
            for (int i = this.FingerTable.Length - 1; i >= 0; i--)
            {
                // if the finger is more closely between the local node and id and that finger corresponds to a valid node, return the finger
                if (this.FingerTable.Successors[i] != null && this.FingerTable.Successors[i] != ChordServer.LocalNode)
                {
                    if (ChordServer.FingerInRange(this.FingerTable.Successors[i].ID, this.ID, id))
                    {
                        ChordInstance instance = ChordServer.GetInstance(this.FingerTable.Successors[i]);
                        if (ChordServer.IsInstanceValid(instance))
                        {
                            return this.FingerTable.Successors[i];
                        }
                    }
                }
            }

            // at this point, not even the successor is any good so go through the successor cache and run the same test
            for (int i = 0; i < this.SuccessorCache.Length; i++)
            {
                if (this.SuccessorCache[i] != null && this.SuccessorCache[i] != ChordServer.LocalNode)
                {                    
                    if (ChordServer.FingerInRange(this.SuccessorCache[i].ID, this.ID, id))
                    {
                        ChordInstance instance = ChordServer.GetInstance(this.SuccessorCache[i]);
                        if (ChordServer.IsInstanceValid(instance))
                        {
                            return this.SuccessorCache[i];
                        }
                    }
                }
            }

            // otherwise, if there is nothing closer, the local node is the closest preceding finger
            return ChordServer.LocalNode;
        }
    }
}
