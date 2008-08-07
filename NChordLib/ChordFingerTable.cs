/*
 * ChordFingerTable.cs:
 * 
 *  The ChordFingerTable is used as a shortcut in navigation.  It is maintained asynchronously by
 *  the UpdateFingerTable maintenance task, and is safely initialized to the seed value (generally
 *  LocalNode).
 * 
 *  The finger table as it stands currently assumes a 64-bit ID space.  If a different bit-width ID
 *  space is desired, the size of the ChordFinger table should be changed as well.
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
    /// <summary>
    /// The ChordFingerTable is used as a navigation shortcut.  One instance of ChordFingerTable is
    /// maintained per Chord server instance, and is maintained asynchronously by background maintenance.
    /// </summary>
    [Serializable]
    public class ChordFingerTable
    {
        /// <summary>
        /// Creates a new instance of the finger table
        /// </summary>
        /// <param name="nodeIn">The node to populate the finger table with</param>
        public ChordFingerTable(ChordNode seed)
        {
            // populate the start array and successors
            for (int i = 0; i < this.Length; i++)
            {
                this.StartValues[i] = (seed.ID + (UInt64)Math.Pow(2, i)) % UInt64.MaxValue;
                this.Successors[i] = seed;
            }
        }

        private UInt64[] m_StartValues = new UInt64[64];  // the array of start values (n+2^i % m)
        /// <summary>
        /// The StartValues for each finger in the finger table.  Each StartValue is a sequential
        /// power of two (wrapped around the Chord ring) further from its previous finger.
        /// </summary>
        public UInt64[] StartValues
        {
            get 
            { 
                return m_StartValues; 
            }
            set 
            { 
                m_StartValues = value; 
            }
        }

        private ChordNode[] m_Successors = new ChordNode[64]; // the array of successors that correspond to each start value
        /// <summary>
        /// The parallel Successor array, where each Successor represents a cached version of FindSuccessor() on
        /// the corresponding StartValue from the StartValues array.
        /// </summary>
        public ChordNode[] Successors
        {
            get 
            { 
                return m_Successors; 
            }
            set 
            { 
                m_Successors = value; 
            }
        }

        /// <summary>
        /// the length of the fingerTable (equal to M or the number of bits in the hash key)
        /// </summary>
        public int Length
        {
            get
            {
                return m_StartValues.Length;
            }
        }
    }
}
