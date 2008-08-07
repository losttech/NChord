/*
 * ChordInstance.Maintenance.ReJoin.cs:
 * 
 *  Ring reformation maintenance task.  This task does a simple ring consistency check by verifying
 *  that the seed node (used to join the ring in the first place) is indeed its own successor.  In
 *  cases when a ring is partitioned or otherwise very damaged, the seed node's successor will not
 *  be itself.  Re-joining the ring via that seed node (assuming the node still exists) allows the
 *  ring to repair itself automatically in a reasonably orderly fashion.
 * 
 *  This task, however, is somewhat incomplete.  First, if the seedNode has in fact disappeared, but 
 *  not on account of ring damage (the seed node may simply have dropped off the network) then the
 *  ReJoin task will not do much good.  Second, the use of a single seed node (rather than a collection
 *  of seed nodes) could possibly lead to some degree of bottlenecking in a large ring that has somehow
 *  come undone, in the case that all (or a large number) of the nodes used the same seed node.  Modifying
 *  this task to use a collection of seed nodes (or simply to cache the seed node's SuccessorCache or
 *  FingerTable) for both verification and rejoin/retry purposes could be a better approach (and there
 *  are plenty of other potential approaches out there).
 * 
 *  The ReJoin task is configured to run every 30 seconds.  This interval is somewhat arbitrary and
 *  can be tuned as needed depending on system size and churn characteristics.
 * 
 *  Purists would likely disable this maintenance task; however, experimental experience has shown the
 *  ReJoin logic to be helpful under extreme churn despite its limitations.  More work in this area is
 *  always welcome.
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
        // toggled on join/first-run of ReJoin to provide some buffer between join and consistency check
        private bool m_HasReJoinRun = false;    

        /// <summary>
        /// Maintenance task to perform ring consistency checking and re-joining to keep a Chord
        /// ring stable under extreme churn and in cases of ring damage.
        /// </summary>
        /// <param name="sender">The calling backgroundworker.</param>
        /// <param name="ea">Args (ignored for this task).</param>
        private void ReJoin(object sender, DoWorkEventArgs ea)
        {
            BackgroundWorker me = (BackgroundWorker)sender;

            while (!me.CancellationPending)
            {
                try
                {
                    // if this is the first iteration, then the core logic
                    // is skipped, as the first iteration generally occurs
                    // right after node Join - allowing a short buffer for
                    // routing structures to stabilize improves the utility
                    // of the ReJoin facility.
                    if (this.m_HasReJoinRun)
                    {
                        // first find the successor for the seed node
                        if (this.m_SeedNode != null)
                        {
                            ChordNode seedSuccessor = FindSuccessor(this.m_SeedNode.ID);

                            // if the successor is not equal to the seed node, something is fishy
                            if (seedSuccessor.ID != this.m_SeedNode.ID)
                            {
                                // if the seed node is still active, re-join the ring to the seed node
                                ChordInstance instance = ChordServer.GetInstance(this.m_SeedNode);
                                if (ChordServer.IsInstanceValid(instance))
                                {
                                    ChordServer.Log(LogLevel.Error, "ReJoin", "Unable to contact initial seed node {0}.  Re-Joining...", this.m_SeedNode);
                                    Join(this.m_SeedNode, this.Host, this.Port);
                                }

                                // otherwise, in the future, there will be a cache of seed nodes to check/join from...
                                // as it may be the case that the seed node simply has disconnected from the network.
                            }
                        }
                    }
                    else
                    {
                        // subsequent iterations will go through the core logic
                        this.m_HasReJoinRun = true;
                    }
                }
                catch (Exception e)
                {
                    ChordServer.Log(LogLevel.Error, "Maintenance", "Error occured during ReJoin ({0})", e.Message);
                }

                // TODO: the delay between iterations, here, is configurable, and
                // ideally should be retrieved from configuration or otherwise passed in...
                Thread.Sleep(30000);
            }
        }
    }
}
