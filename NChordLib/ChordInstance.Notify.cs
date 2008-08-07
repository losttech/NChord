/*
 * ChordInstance.Notify.cs:
 * 
 *  Implementation of the Notify method used by Chord to notify a node's successor
 *  as to who its predecessor is.  The Notify method may be called in an even safer
 *  fashion via the static CallNotify method in ChordServer for remote calls.
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
        /// Called by the predecessor to a remote node, this acts as a dual heartbeat mechanism and more importantly
        /// notification mechanism between predecessor and successor.
        /// </summary>
        /// <param name="node">A ChordNode instance indicating who the calling node (predecessor) is.</param>
        public void Notify(ChordNode node)
        {
            // if the node has absolutely no predecessor, take
            // the first one it finds
            if (this.Predecessor == null)
            {
                this.Predecessor = node;
                return;
            }

            // otherwise, ensure that the predecessor that is calling in
            // is indeed valid...
            if (ChordServer.IsIDInRange(node.ID, this.Predecessor.ID, this.ID))
            {
                this.Predecessor = node;
                return;
            }
        }
    }
}
