/*
 * ChordInstance.Depart.cs:
 * 
 *  Implements the Depart() logic for a given ChordInstance.  While not always run on departure of a
 *  node from a Chord ring (often, nodes will just become unreachable without notifying anyone),
 *  depart is a courtesy to the departing node's Successor and Predecessor so they do not have to 
 *  figure out who each other are.
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
        /// Depart from the Chord ring and stop maintenance.
        /// </summary>
        public void Depart()
        {
            // first, stop maintenance so nothing gets broken
            StopMaintenance();

            try
            {
                // courteously introduce departing node's successor and predecessor
                // ...successor, meet predecessor; predecessor, meet successor
                ChordInstance instance = ChordServer.GetInstance(this.Successor);
                instance.Predecessor = this.Predecessor;

                instance = ChordServer.GetInstance(this.Predecessor);
                instance.Successor = this.Successor;
            }
            catch (Exception e)
            {
                ChordServer.Log(LogLevel.Error, "Navigation", "Error on Depart ({0}).", e.Message);
            }
            finally
            {
                // set local state in such a way as to force it out of the Chord ring
                this.Successor = ChordServer.LocalNode;
                this.Predecessor = ChordServer.LocalNode;
                this.FingerTable = new ChordFingerTable(ChordServer.LocalNode);
                for (int i = 0; i < this.SuccessorCache.Length; i++)
                {
                    this.SuccessorCache[i] = ChordServer.LocalNode;
                }
            }
        }
    }
}
