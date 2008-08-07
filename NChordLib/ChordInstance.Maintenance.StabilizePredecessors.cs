/*
 * ChordInstance.Maintenance.StabilizePredecessors.cs:
 * 
 *  Maintenance task to stabilize the local node's predecessor as specified by the Chord paper.
 * 
 *  This task runs every 5 seconds though that value can be tweaked as needed.
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
        /// Maintenance task to stabilize the local node's predecessor as per the Chord paper.
        /// </summary>
        /// <param name="sender">The backgroundworker thread that this task is running on.</param>
        /// <param name="ea">Args (ignored)</param>
        private void StabilizePredecessors(object sender, DoWorkEventArgs ea)
        {
            BackgroundWorker me = (BackgroundWorker)sender;

            while (!me.CancellationPending)
            {
                if (this.Predecessor != null)
                {
                    try
                    {
                        // validate predecessor (in case of error, predecessor becomes null
                        // and is fixed by stabilizesuccessors and notify.
                        ChordInstance instance = ChordServer.GetInstance(this.Predecessor);
                        if (!ChordServer.IsInstanceValid(instance))
                        {
                            this.Predecessor = null;
                        }
                    }
                    catch (Exception e)
                    {
                        ChordServer.Log(LogLevel.Error, "StabilizePredecessors", "StabilizePredecessors error: {0}", e.Message);
                        this.Predecessor = null;
                    }

                }

                // TODO: make this configurable either via config file or passed in via arguments.
                Thread.Sleep(5000);
            }
        }
    }
}
