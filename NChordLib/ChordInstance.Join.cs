/*
 * ChordInstance.Join.cs:
 * 
 *  Implements the Join() logic for a given ChordInstance.  Aside from safely establishing core routing
 *  objects, the node's successor is established, while all other routing information is populated
 *  lazily by the instance's background maintenance threads.
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
using System.Threading;

namespace NChordLib
{
    public partial class ChordInstance : MarshalByRefObject
    {
        /// <summary>
        /// Join the current ChordInstance to a chord ring given a seed node.
        /// </summary>
        /// <param name="seed">Remote node to connect to that is a member of a Chord ring.  To start a new Chord ring, a null seed may be provided.</param>
        /// <param name="host">The local host name.</param>
        /// <param name="port">The tcp port on which this node will listen for calls from other Chord nodes.</param>
        /// <returns>true if the Join succeeds; false, otherwise.</returns>
        public bool Join(ChordNode seed, string host, int port)
        {
            // LocalNode is established first to ensure proper logging.
            ChordServer.LocalNode = new ChordNode(host, port);

            this.m_HasReJoinRun = false;
            this.m_SeedNode = seed; // cache the seed node so the ring may re-form itself in case of partition or damage

            // safely establish finger table (startvalues and successors, using LocalNode as the default successor)
            this.FingerTable = new ChordFingerTable(ChordServer.LocalNode);

            // establish successor cache initially with all entries pointing loally
            this.SuccessorCache = new ChordNode[3]; // TODO: make this configurable
            for (int i = 0; i < this.SuccessorCache.Length; i++)
            {
                this.SuccessorCache[i] = ChordServer.LocalNode;
            }

            if (seed != null)   // join an existing chord ring
            {
                ChordServer.Log(LogLevel.Info, "Navigation", "Joining Ring @ {0}:{1}.", seed.Host, seed.PortNumber);

                // first, establish successor via seed node
                ChordInstance instance = ChordServer.GetInstance(seed);
                if (ChordServer.IsInstanceValid(instance))
                {
                    try
                    {
                        this.Successor = instance.FindSuccessor(this.ID);   // NOTE: this conceivably could be replaced with ChordServer.FindSuccessor()
                        
                        // disabled: a clever trick that requires only one remote network call is to
                        //  append the successor's successor cache (minus its last entry) to the local
                        //  successor cache, starting at the second entry in the local successor cache.
                        //  during churn, this can break down, so instead the successor cache is populated
                        //  and maintained lazily by maintenance.
                        //  as the successor cache was initialized with the LocalNode as the default
                        //  instance values, "misses" on successor cache entries are gracefully handled by
                        //  simply being forwarded (via the local node) on to the successor node where
                        //  better information may be found and utilized.
                        //  
                        //this.SuccessorCache = ChordServer.GetSuccessorCache(this.Successor);
                    }
                    catch (Exception e)
                    {
                        ChordServer.Log(LogLevel.Error, "Navigation", "Error setting Successor Node ({0}).", e.Message);
                        return false;
                    }
                }
                else
                {
                    ChordServer.Log(LogLevel.Error, "Navigation", "Invalid Node Seed.");
                    return false;
                }
            }
            else // start a new ring
            {
                // not much needs to happen - successor is already established as 
                // ChordServer.LocalNode,everything else takes place lazily as part of maintenance
                ChordServer.Log(LogLevel.Info, "Navigation", "Starting Ring @ {0}:{1}.", this.Host, this.Port);
            }

            // everything that needs to be populated or kept up-to-date
            // lazily is handled via background maintenance threads running periodically
            StartMaintenance();

            return true;
        }
    }
}
