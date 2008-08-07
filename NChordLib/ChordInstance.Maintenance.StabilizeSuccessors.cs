/*
 * ChordInstance.Maintenance.StabilizeSuccessors.cs:
 * 
 *  Maintenance task to stabilize successors per the Chord paper.  The algorithm here doesn't deviate too
 *  too much from that specified in the Chord paper, though there is a little extra handling for error
 *  cases, etc.
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
using System.ComponentModel;
using System.Threading;

namespace NChordLib
{
    public partial class ChordInstance : MarshalByRefObject
    {
        /// <summary>
        /// Maintenance task to ensure that the local node has valid successor node.  Roughly equivalent
        /// to what is called out in the Chord paper.
        /// </summary>
        /// <param name="sender">The worker thread the task is running on.</param>
        /// <param name="ea">Args (ignored here).</param>
        private void StabilizeSuccessors(object sender, DoWorkEventArgs ea)
        {
            BackgroundWorker me = (BackgroundWorker)sender;

            while (!me.CancellationPending)
            {
                try
                {
                    // check in successor and if it's bad, replace it with
                    // the next live entry in the successor cache
                    ChordNode succPredNode = ChordServer.GetPredecessor(this.Successor);
                    if (succPredNode != null)
                    {
                        if (ChordServer.IsIDInRange(succPredNode.ID, this.ID, this.Successor.ID))
                        {
                            this.Successor = succPredNode;
                        }

                        // ignoring return because bad node will be detected on next invocation
                        ChordServer.CallNotify(this.Successor, ChordServer.LocalNode);
                        GetSuccessorCache(this.Successor);
                    }
                    else
                    {
                        bool successorCacheHelped = false;
                        foreach (ChordNode entry in this.m_SuccessorCache)
                        {
                            ChordInstance instance = ChordServer.GetInstance(entry);
                            if (ChordServer.IsInstanceValid(instance))
                            {
                                this.Successor = entry;
                                ChordServer.CallNotify(this.Successor, ChordServer.LocalNode);
                                GetSuccessorCache(this.Successor);
                                successorCacheHelped = true;
                                break;
                            }
                        }

                        // if we get here, then we got no help and have no other recourse than to re-join using the initial seed...
                        if (!successorCacheHelped)
                        {
                            ChordServer.Log(LogLevel.Error, "StabilizeSuccessors", "Ring consistency error, Re-Joining Chord ring.");
                            Join(this.m_SeedNode, this.Host, this.Port);
                            return;
                        }
                    }
                }
                catch (Exception e)
                {
                    ChordServer.Log(LogLevel.Error, "Maintenance", "Error occured during StabilizeSuccessors ({0})", e.Message);
                }

                // TODO: this could be tweaked and/or made configurable elsewhere or passed in as arguments
                Thread.Sleep(5000);
            }
        }

        /// <summary>
        /// Get the successor cache from a remote node and assign an altered version the local successorCache.
        /// Gets the remote successor cache, prepends remoteNode and lops off the last entry from the remote
        /// successorcache.
        /// </summary>
        /// <param name="remoteNode">The remote node to get the succesorCache from.</param>
        private void GetSuccessorCache(ChordNode remoteNode)
        {
            ChordNode[] remoteSuccessorCache = ChordServer.GetSuccessorCache(remoteNode);
            if (remoteSuccessorCache != null)
            {
                this.SuccessorCache[0] = remoteNode;
                for (int i = 1; i < this.SuccessorCache.Length; i++)
                {
                    this.SuccessorCache[i] = remoteSuccessorCache[i - 1];
                }
            }
        }
    }
}
