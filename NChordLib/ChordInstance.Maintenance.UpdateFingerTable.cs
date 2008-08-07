/*
 * ChordInstance.Maintenance.UpdateFingerTable.cs:
 * 
 *  Maintenance task to keep the local node's finger table up to date.  There are myriad ways in which this particular
 *  task can be implemented (e.g. run frequency vs. number of fingers updated per execution); however, in practice this
 *  approach seemed to work fairly well where a single finger is updated every second - meaning roughly once per minute
 *  the whole finger table has been traversed.  This job has also been configured to update the entire finger table in
 *  a single go, and also to be extremely un-aggressive in keeping the finger table up-to-date - in all cases, lookup
 *  still worked and efficiency under churn didn't suffer too badly either.
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
        private int m_NextFingerToUpdate = 0;

        /// <summary>
        /// Update the local node's finger table entries on a background thread.
        /// </summary>
        /// <param name="sender">The background worker thread this task is running on.</param>
        /// <param name="ea">Args (ignored).</param>
        private void UpdateFingerTable(object sender, DoWorkEventArgs ea)
        {
            BackgroundWorker me = (BackgroundWorker)sender;

            while (!me.CancellationPending)
            {
                try
                {
                    // update the fingers moving outwards - once the last finger
                    // has been reached, start again closest to LocalNode (0).
                    if (this.m_NextFingerToUpdate >= this.m_FingerTable.Length)
                    {
                        this.m_NextFingerToUpdate = 0;
                    }

                    try
                    {
                        // Node validity is checked by findSuccessor
                        this.FingerTable.Successors[this.m_NextFingerToUpdate] = FindSuccessor(this.FingerTable.StartValues[this.m_NextFingerToUpdate]);
                    }
                    catch (Exception e)
                    {
                        ChordServer.Log(LogLevel.Error, "Navigation", "Unable to update Successor for start value {0} ({1}).", this.FingerTable.StartValues[this.m_NextFingerToUpdate], e.Message);
                    }

                    this.m_NextFingerToUpdate = this.m_NextFingerToUpdate + 1;
                }
                catch (Exception e)
                {
                    // (overly safe here)
                    ChordServer.Log(LogLevel.Error, "Maintenance", "Error occured during UpdateFingerTable ({0})", e.Message);
                }

                // TODO: make this configurable via config file or passed in as an argument
                Thread.Sleep(1000);
            }
        }
    }
}
