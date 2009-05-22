/*
 * ChordNode.cs:
 * 
 *  Contains the ChordNode class, which is used to describe and specify local and remote Chord nodes.
 *  A ChordNode is comprised of identifying information and an ID (hash) value.  In this implementation,
 *  the identifying information is a simple hostname/port combination, but adapted implementations might
 *  modify the identifier to use a MAC address, IP address, or fully-qualified domain name plus port number
 *  for identification.
 * 
 *  The best way to compare ChordNodes is on their ID values, and, for various reasons, ChordNode should always
 *  implement IComparable in order to easily sort and manipulate ChordNode objects.
 * 
 *  Hashing may (should) be swapped out via the centralized ChordServer.GetHash method should different hashing
 *  semantics be desired.
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
    /// ChordNode is an object used to identify a potential node in a Chord ring given a hostname and a port.
    /// The main use of ChordNode is for communication between client and server, as well as between servers in a Chord ring.
    /// </summary>
    [Serializable]
    public class ChordNode : IComparable
    {
        private string m_hostName = string.Empty;
        /// <summary>
        /// The host name that identifies the specified Chord node.
        /// </summary>
        public string Host
        {
            get
            {
                return this.m_hostName;
            }
        }

        private int m_portNumber = 0;
        /// <summary>
        /// The port number that the specified Chord node listens on.
        /// </summary>
        public int PortNumber
        {
            get
            {
                return this.m_portNumber;
            }
        }

        /// <summary>
        /// Create a new instance of ChordNode for a given host and port.
        /// </summary>
        /// <param name="host">The host name that identifies this Chord node.</param>
        /// <param name="port">The port number the Chord node is listening on.</param>
        public ChordNode(string host, int port)
        {
            this.m_hostName = host;
            this.m_portNumber = port;
        }

        /// <summary>
        /// Returns the node hash value (based on a case-insensitive hash of the node's hostname and port).
        ///     NOTE:  while not entirely critical to performance, repeated calls in to the hashing
        ///            function could be reduced by caching the hashed ID in a private member.
        /// </summary>
        public UInt64 ID
        {
            get
            {
                return ChordServer.GetHash(this.Host.ToUpper() + this.PortNumber.ToString());
            }
        }

        /// <summary>
        /// Compares ChordNodes on their ID hash value.
        /// </summary>
        /// <param name="obj">The object to compare this ChordNode to.</param>
        /// <returns>A negative value if the object is smaller; zero if they are equal; positive if the object is larger.</returns>
        public int CompareTo(object obj)
        {
            if (obj is ChordNode)
            {
                ChordNode node = (ChordNode)obj;
                return this.ID.CompareTo(node.ID);
            }

            throw new ArgumentException("Object is not a ChordNode.");
        }

        /// <summary>
        /// Perform equality comparison on ChordNodes on their ID hash value.
        /// </summary>
        /// <param name="obj">The object to compare this ChordNode to.</param>
        /// <returns>True if equal; false, otherwise.</returns>
        public override bool Equals(object obj)
        {
            try
            {
                ChordNode node = (ChordNode)obj;
                return this.ID == node.ID;
            }
            catch
            {
                // Equality operation should not throw...
                return false;
            }
        }

        /// <summary>
        /// Output friendly format of ChordNode.
        /// </summary>
        /// <returns>A string describing the ChordNode instance.</returns>
        public override string ToString()
        {
            return string.Format("{0}:{1} ({2})", this.Host, this.PortNumber.ToString(), this.ID.ToString("x10").ToUpper());
        }

        /// <summary>
        /// Get hash code.
        /// </summary>
        /// <returns>Hash code.</returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}
