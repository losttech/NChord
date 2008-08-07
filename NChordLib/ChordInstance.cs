/*
 * ChordInstance.cs:
 * 
 *  Constructor and lifetime service management for the ChordInstance class.
 * 
 *  In the most common case, ChordInstance is a singleton remoting instance that is both a client
 *  and server.  To prevent an idle (but quite useful) ChordInstance from being garbage collected,
 *  the InitializeLifetimeService method must be overridden to return null.
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
        /// create a new ChordInstance
        /// </summary>
        public ChordInstance()
        {
            // constructor does not need to do anything
        }

        /// <summary>
        /// Prevents singleton instance from being garbage collected.
        /// </summary>
        /// <returns>Always null to prevent the singleton from being garbage collected.</returns>
        public override object InitializeLifetimeService()
        {
            return null;
        }
    }
}
